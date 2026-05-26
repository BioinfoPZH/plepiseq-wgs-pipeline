"""Assign hierCC levels for a cgMLST sequence type from local ``plepiseq-cluster`` outputs.

This is the local-database counterpart of ``phiercc_enterobase.py`` and
``phiercc_pubmlst.py``. For each supported scheme we look up the matching
cgMLST ST in two pre-computed hierarchical clustering files produced by
`plepiseq-cluster <https://github.com/BioinfoPZH/plepiseq-cluster>`_:

    - ``profile_single_linkage.HierCC.gz`` (plus ``.index``)
    - ``profile_complete_linkage.HierCC.gz`` (plus ``.index``)

Inputs (CLI):
    --cgmlst-parsed          path to ``cgMLST_parsed_output.txt`` produced by
                             the cgMLST parsing step. We read ``(ST_sample,
                             ST_matching, distance)`` from line 2.
    --species / --genus / --qc-status / --qc-contam-status :
                             sample metadata forwarded from the Nextflow module.
    --db-dir                 root of the local hierCC database directory
                             (typically ``/db/phiercc_local`` inside the
                             container).
    --output-single          path of the single-linkage TSV to write.
    --output-complete        path of the complete-linkage TSV to write.
    --output-json            path of the aggregated JSON to write.

Index protocol (as emitted by ``plepiseq-cluster``)::

    #ST_id            0             # comment header, skipped by the reader
    1                 47            # numeric section: sparse checkpoints every 10k rows
    10001             4213052
    ...
    500001            211284770     # last numeric checkpoint
    __LOCAL_START__   211520933     # always-present sentinel, offset of first local_* row
    local_1           211520933     # local section: dense, one entry per local_* ST
    local_2           211521020
    ...
    local_9871        212408262

Pure-numeric profiles still emit the sentinel, at EOF, so the reader never
has to branch on "does this profile have locals?".

Output contract:
    The only hard consumer is ``run_cgMLST_final_json`` in
    ``cgmlst_json_aggregator.nf``, which reads ``--output-json``. The
    aggregator skips payloads whose top-level key is ``"dummy"``, so we
    emit ``{"dummy": "dummy"}`` for bad QC / unsupported species and a
    proper ``{"hiercc_clustering_internal_data": [...]}`` payload otherwise
    (with ``-1`` sentinels on any level we could not resolve).

    The two TSVs land only in the publish directory for human inspection;
    they are guaranteed to be well-formed (scheme header + one data row)
    in all happy-path and mid-processing fallback cases, and the compact
    ``ST\\tComment`` two-line shape in short-circuit cases (bad QC /
    unsupported species).

Exit codes:
    0  outputs were produced (happy path or any fallback).
    1  outputs could not be produced at all (disk full, validation failed);
       Nextflow halts the pipeline deliberately.
    2  click-level CLI misuse.
"""

import gzip
import json
import logging
import os
import re
import sys
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple

import click


LOCAL_PREFIX = "local_"
LOCAL_SENTINEL = "__LOCAL_START__"
INDEX_COMMENT_PREFIX = "#"

SINGLE_LINKAGE_GZ = "profile_single_linkage.HierCC.gz"
SINGLE_LINKAGE_INDEX = "profile_single_linkage.HierCC.index"
COMPLETE_LINKAGE_GZ = "profile_complete_linkage.HierCC.gz"
COMPLETE_LINKAGE_INDEX = "profile_complete_linkage.HierCC.index"


@dataclass(frozen=True)
class SchemeConfig:
    """Per-scheme configuration for a local hierCC lookup."""

    header: tuple
    level_keys: tuple
    hiercc_columns: tuple
    subpath: str


SCHEMES = {
    "Salmonella": SchemeConfig(
        header=(
            "ST", "HC0", "HC2", "HC5", "HC10", "HC20", "HC50", "HC100",
            "HC200", "HC400", "HC900(ceBG)", "HC2000", "HC2600", "HC2850(subsp.)",
        ),
        level_keys=(
            "d0", "d2", "d5", "d10", "d20", "d50", "d100", "d200",
            "d400", "d900", "d2000", "d2600", "d2850",
        ),
        hiercc_columns=(1, 3, 6, 11, 21, 51, 101, 201, 401, 901, 2001, 2601, 2851),
        subpath="Salmonella",
    ),
    "Escherichia": SchemeConfig(
        header=(
            "ST", "HC0", "HC2", "HC5", "HC10", "HC20", "HC50", "HC100",
            "HC200", "HC400", "HC1100(cgST Cplx)", "HC1500", "HC2000", "HC2350(subsp.)",
        ),
        level_keys=(
            "d0", "d2", "d5", "d10", "d20", "d50", "d100", "d200",
            "d400", "d1100", "d1500", "d2000", "d2350",
        ),
        hiercc_columns=(1, 3, 6, 11, 21, 51, 101, 201, 401, 1101, 1501, 2001, 2351),
        subpath="Escherichia",
    ),
    "jejuni": SchemeConfig(
        header=("ST", "HC5", "HC10", "HC25", "HC50", "HC100", "HC200"),
        level_keys=("d5", "d10", "d25", "d50", "d100", "d200"),
        hiercc_columns=(6, 11, 26, 51, 101, 201),
        subpath="Campylobacter/jejuni",
    ),
}


class SampleSTParseError(Exception):
    """Raised when ``cgMLST_parsed_output.txt`` cannot be parsed."""


class PhierccDataError(Exception):
    """Raised when a local hierCC data file (index or gz) cannot be loaded/used."""


class MissingLevelError(Exception):
    """Raised when a hierCC row is too short to contain every expected column."""


class OutputValidationError(Exception):
    """Raised when a freshly-written output file fails structural validation."""


def select_scheme(genus, species):
    """Pick the scheme for a sample: Salmonella/Escherichia by genus, jejuni by species."""
    if genus in SCHEMES:
        return SCHEMES[genus]
    if species in SCHEMES:
        return SCHEMES[species]
    return None


def read_sample_st(path):
    """Read ``(ST_sample, ST_matching, distance)`` from the parsed cgMLST file."""
    try:
        with open(path) as f:
            for line in f:
                fields = line.split()
                if not fields or fields[0] == "ST_sample":
                    continue
                if len(fields) < 3:
                    raise SampleSTParseError(
                        f"Data row in {path!r} has {len(fields)} fields, expected >=3"
                    )
                return fields[0], fields[1], int(fields[2])
    except OSError as exc:
        raise SampleSTParseError(f"Could not read {path!r}: {exc}") from exc
    raise SampleSTParseError(f"No data row found in {path!r}")


def load_hiercc_index(index_path):
    """Parse a ``plepiseq-cluster`` ``.index`` file into its two logical sections.

    Returns ``(numeric_checkpoints, local_dense, local_section_offset)``:

        - ``numeric_checkpoints``: ``list[(int_st, offset)]`` in file order
          (expected to be ascending by ST).
        - ``local_dense``: ``dict[str, int]`` mapping ``local_*`` labels to
          exact gz offsets.
        - ``local_section_offset``: the offset carried by the
          ``__LOCAL_START__`` sentinel; also the upper bound for bounded
          numeric scans of the gz.

    Raises ``PhierccDataError`` on malformed input or a missing sentinel.
    """
    numeric_checkpoints: List[Tuple[int, int]] = []
    local_dense: Dict[str, int] = {}
    local_section_offset: Optional[int] = None
    seen_sentinel = False

    try:
        fh = open(index_path)
    except OSError as exc:
        raise PhierccDataError(f"Cannot open index {index_path!r}: {exc}") from exc

    with fh:
        for lineno, raw in enumerate(fh, start=1):
            line = raw.rstrip("\n")
            if not line or line.startswith(INDEX_COMMENT_PREFIX):
                continue
            parts = line.split()
            if len(parts) < 2:
                raise PhierccDataError(
                    f"Malformed index row at {index_path!r}:{lineno}: {line!r}"
                )
            st, raw_offset = parts[0], parts[1]
            try:
                offset = int(raw_offset)
            except ValueError as exc:
                raise PhierccDataError(
                    f"Non-integer offset at {index_path!r}:{lineno}: {raw_offset!r}"
                ) from exc

            if st == LOCAL_SENTINEL:
                local_section_offset = offset
                seen_sentinel = True
                continue

            if seen_sentinel:
                local_dense[st] = offset
            else:
                try:
                    numeric_checkpoints.append((int(st), offset))
                except ValueError as exc:
                    raise PhierccDataError(
                        f"Non-integer ST in numeric section at "
                        f"{index_path!r}:{lineno}: {st!r}"
                    ) from exc

    if local_section_offset is None:
        raise PhierccDataError(
            f"Index {index_path!r} is missing the required "
            f"{LOCAL_SENTINEL!r} sentinel row"
        )
    return numeric_checkpoints, local_dense, local_section_offset


def find_offset(matching_st, numeric_checkpoints, local_dense, local_section_offset):
    """Map ``matching_st`` to the gz offset the scanner should seek to.

    - ``local_*`` label -> ``local_dense.get(...)`` (O(1)).
    - Numeric label -> walk the sparse numeric checkpoints and return the
      largest offset whose ST is ``<= matching_st``.

    Returns ``None`` if the target cannot be located (DB drift).
    """
    if matching_st.startswith(LOCAL_PREFIX):
        return local_dense.get(matching_st)

    try:
        target = int(matching_st)
    except ValueError:
        return None

    pointer: Optional[int] = None
    for st, offset in numeric_checkpoints:
        if target < st:
            break
        pointer = offset
    return pointer


def scan_hiercc_row(gz_path, start_offset, matching_st, stop_offset=None):
    """Seek into the gz, scan rows forward until ``row[0] == matching_st``.

    ``stop_offset`` bounds the scan so numeric queries never cross into the
    local block. For ``local_*`` queries the caller passes ``None`` - the
    dense index means the first row read IS the hit.

    Returns the tokenised row (``list[str]``) on a hit, ``None`` on miss.
    """
    try:
        fh = gzip.open(gz_path, "rb")
    except OSError as exc:
        raise PhierccDataError(f"Cannot open gz {gz_path!r}: {exc}") from exc

    with fh:
        try:
            fh.seek(start_offset)
        except OSError as exc:
            raise PhierccDataError(
                f"Cannot seek to offset {start_offset} in {gz_path!r}: {exc}"
            ) from exc

        while True:
            if stop_offset is not None and fh.tell() >= stop_offset:
                return None
            raw = fh.readline()
            if not raw:
                return None
            row = raw.decode("utf-8", errors="replace").split()
            if row and row[0] == matching_st:
                return row
    return None


def extract_levels(row, columns):
    """Pick the configured hierCC columns out of a row, with a length guard."""
    if len(row) <= max(columns):
        raise MissingLevelError(
            f"Row has {len(row)} tokens, need >= {max(columns) + 1}"
        )
    return [row[i] for i in columns]


def collapse_local_sts(levels, level_keys, sample_st, distance):
    """Overwrite the lowest HC levels with the local sample ST.

    When the sample's allelic distance to its closest reference ST is > 0,
    any HC level whose radius is strictly less than ``distance`` cannot
    contain the sample - substitute the sample's local ST for those
    low-radius levels. Matches the original heredoc's behaviour.
    """
    levels = list(levels)
    radii = [int(re.findall(r"\d+", k)[0]) for k in level_keys]
    last_index = -1
    for i, r in enumerate(radii):
        if r < distance:
            last_index = i
    if last_index >= 0:
        for i in range(last_index + 1):
            levels[i] = sample_st
    return levels


def lookup_linkage(gz_path, index_path, scheme, matching_st, sample_st, distance):
    """Orchestrate a single (linkage, gz, index) lookup.

    Returns ``list[str]`` of resolved HC levels on success, or ``None`` on
    any recoverable failure (including the DB-drift case where the ST is
    simply not present in the local index).
    """
    try:
        numeric_cps, local_dense, local_section_offset = load_hiercc_index(index_path)
    except PhierccDataError:
        logging.exception("Failed to load hierCC index %s", index_path)
        return None

    start_offset = find_offset(
        matching_st, numeric_cps, local_dense, local_section_offset,
    )
    if start_offset is None:
        logging.warning(
            "ST %r not present in local hierCC index %s - likely DB drift",
            matching_st, index_path,
        )
        return None

    stop_offset = None if matching_st.startswith(LOCAL_PREFIX) else local_section_offset

    try:
        row = scan_hiercc_row(gz_path, start_offset, matching_st, stop_offset)
    except PhierccDataError:
        logging.exception("Failed to scan hierCC gz %s", gz_path)
        return None

    if row is None:
        logging.warning(
            "ST %r located in index %s but row missing from gz %s - "
            "index/gz out of sync",
            matching_st, index_path, gz_path,
        )
        return None

    try:
        raw_levels = extract_levels(row, scheme.hiercc_columns)
    except MissingLevelError:
        logging.exception("hierCC row for ST %r is too short", matching_st)
        return None

    return collapse_local_sts(raw_levels, scheme.level_keys, sample_st, distance)


def _write_tsv(path, header, row):
    with open(path, "w") as f:
        f.write("\t".join(header) + "\n")
        f.write("\t".join(str(x) for x in row) + "\n")


def write_short_circuit_fallback(path, species):
    """Two-line ``ST\\tComment`` file used for bad QC / unsupported species."""
    _write_tsv(path, ("ST", "Comment"), ("unk", f"Unknown species: {species}"))
    validate_output(path, expected_columns=2)


def write_sentinel_fallback(path, scheme, reason):
    """Full scheme header + ``unk`` and ``-1`` sentinels for mid-processing failures."""
    logging.error("Writing sentinel fallback to %s: %s", path, reason)
    sentinels = ["-1"] * (len(scheme.header) - 1)
    _write_tsv(path, scheme.header, ("unk", *sentinels))
    validate_output(path, expected_columns=len(scheme.header))


def write_result(path, scheme, sample_st, levels):
    """Happy-path writer: scheme header + sample ST + resolved HC levels."""
    if len(levels) != len(scheme.level_keys):
        raise OutputValidationError(
            f"Expected {len(scheme.level_keys)} levels, got {len(levels)}"
        )
    _write_tsv(path, scheme.header, (sample_st, *levels))
    validate_output(path, expected_columns=len(scheme.header))


def validate_output(path, expected_columns):
    """Re-read the file and check its shape. Guards against silent truncation."""
    try:
        with open(path) as f:
            lines = f.read().splitlines()
    except OSError as exc:
        raise OutputValidationError(f"Cannot re-read {path!r}: {exc}") from exc

    if len(lines) < 2:
        raise OutputValidationError(
            f"{path!r} has {len(lines)} line(s), expected 2"
        )
    header_cols = lines[0].split("\t")
    value_cols = lines[1].split("\t")
    if len(header_cols) != expected_columns:
        raise OutputValidationError(
            f"{path!r} header has {len(header_cols)} cols, "
            f"expected {expected_columns}"
        )
    if len(value_cols) != expected_columns:
        raise OutputValidationError(
            f"{path!r} row has {len(value_cols)} cols, "
            f"expected {expected_columns}"
        )


def build_json_payload(scheme, levels):
    """Build the ``hiercc_clustering_internal_data`` dict, using sentinels on miss.

    ``levels`` may be ``None`` (no linkage succeeded) or a ``list[str]``.
    """
    # The header's first column is the ST itself; the HC labels start at index 1.
    hc_labels = scheme.header[1:]
    values = levels if levels is not None else ["-1"] * len(hc_labels)
    list_to_dump = []
    for label, value in zip(hc_labels, values):
        digits = re.findall(r"\d+", label)
        list_to_dump.append({
            "level": digits[0] if digits else label,
            "group_id": str(value).rstrip(),
        })
    return {"hiercc_clustering_internal_data": list_to_dump}


def write_json(path, payload):
    """Write ``payload`` to ``path`` as JSON and validate the file parses back."""
    with open(path, "w") as f:
        f.write(json.dumps(payload))
    try:
        with open(path) as f:
            round_trip = json.load(f)
    except (OSError, json.JSONDecodeError) as exc:
        raise OutputValidationError(
            f"JSON output {path!r} failed round-trip parse: {exc}"
        ) from exc
    if round_trip != payload:
        raise OutputValidationError(
            f"JSON output {path!r} content mismatch after round-trip"
        )


def _unrecoverable_exit(reason):
    """Log and exit 1 when even the fallback write failed."""
    logging.critical("Cannot produce a valid output file set: %s", reason)
    sys.exit(1)


def _write_dummy_short_circuit(output_single, output_complete, output_json, species):
    """Emit the bad-QC / unsupported-species trio in one place."""
    write_short_circuit_fallback(output_single, species)
    write_short_circuit_fallback(output_complete, species)
    with open(output_json, "w") as f:
        f.write(json.dumps({"dummy": "dummy"}))


def _write_full_sentinel_trio(output_single, output_complete, output_json,
                              scheme, reason):
    """Emit sentinel TSVs + a real-structured sentinel JSON for mid-proc failures."""
    write_sentinel_fallback(output_single, scheme, reason)
    write_sentinel_fallback(output_complete, scheme, reason)
    write_json(output_json, build_json_payload(scheme, levels=None))


@click.command()
@click.option("--cgmlst-parsed", "cgmlst_parsed",
              type=click.Path(exists=True, dir_okay=False),
              required=True,
              help="Path to cgMLST_parsed_output.txt.")
@click.option("--species", required=True, help="Predicted species label.")
@click.option("--genus", required=True, help="Predicted genus label.")
@click.option("--qc-status", "qc_status",
              type=click.Choice(["tak", "nie", "blad"], case_sensitive=False),
              required=True, help="Primary QC status.")
@click.option("--qc-contam-status", "qc_contam_status",
              type=click.Choice(["tak", "nie", "blad"], case_sensitive=False),
              required=True, help="Contamination QC status.")
@click.option("--db-dir", "db_dir",
              type=click.Path(file_okay=False),
              required=True,
              help="Root of the local hierCC database (contains per-scheme subdirs).")
@click.option("--output-single", "output_single",
              type=click.Path(dir_okay=False), required=True,
              help="Path of parsed_phiercc_minimum_spanning_tree.txt to write.")
@click.option("--output-complete", "output_complete",
              type=click.Path(dir_okay=False), required=True,
              help="Path of parsed_phiercc_maximum_spanning_tree.txt to write.")
@click.option("--output-json", "output_json",
              type=click.Path(dir_okay=False), required=True,
              help="Path of cgMLST_json_phiercc_local.json to write.")
def main(cgmlst_parsed, species, genus, qc_status, qc_contam_status,
         db_dir, output_single, output_complete, output_json):
    """Resolve local hierCC levels for a cgMLST ST; always emit a valid output set."""
    logging.basicConfig(
        stream=sys.stderr,
        level=logging.INFO,
        format="[phiercc_local] %(levelname)s %(message)s",
    )

    if qc_status.lower() == "nie" or qc_contam_status.lower() == "nie":
        try:
            _write_dummy_short_circuit(
                output_single, output_complete, output_json, species,
            )
        except Exception as exc:  # noqa: BLE001
            _unrecoverable_exit(f"bad-QC short-circuit write failed: {exc}")
        return

    scheme = select_scheme(genus, species)
    if scheme is None:
        logging.warning(
            "genus=%r species=%r is not a supported cgMLST scheme, "
            "writing short-circuit fallback", genus, species,
        )
        try:
            _write_dummy_short_circuit(
                output_single, output_complete, output_json, species,
            )
        except Exception as exc:  # noqa: BLE001
            _unrecoverable_exit(f"unsupported-species short-circuit write failed: {exc}")
        return

    scheme_dir = os.path.join(db_dir, scheme.subpath)
    single_gz = os.path.join(scheme_dir, SINGLE_LINKAGE_GZ)
    single_index = os.path.join(scheme_dir, SINGLE_LINKAGE_INDEX)
    complete_gz = os.path.join(scheme_dir, COMPLETE_LINKAGE_GZ)
    complete_index = os.path.join(scheme_dir, COMPLETE_LINKAGE_INDEX)

    try:
        sample_st, matching_st, distance = read_sample_st(cgmlst_parsed)
    except SampleSTParseError as exc:
        logging.exception("Cannot parse %s", cgmlst_parsed)
        try:
            _write_full_sentinel_trio(
                output_single, output_complete, output_json, scheme,
                reason=f"sample ST parse failed: {exc}",
            )
        except Exception as inner:  # noqa: BLE001
            _unrecoverable_exit(f"sentinel fallback failed: {inner}")
        return

    single_levels = lookup_linkage(
        single_gz, single_index, scheme, matching_st, sample_st, distance,
    )
    complete_levels = lookup_linkage(
        complete_gz, complete_index, scheme, matching_st, sample_st, distance,
    )

    try:
        if single_levels is not None:
            write_result(output_single, scheme, sample_st, single_levels)
        else:
            write_sentinel_fallback(
                output_single, scheme,
                reason=f"single-linkage lookup failed for ST {matching_st!r}",
            )

        if complete_levels is not None:
            write_result(output_complete, scheme, sample_st, complete_levels)
        else:
            write_sentinel_fallback(
                output_complete, scheme,
                reason=f"complete-linkage lookup failed for ST {matching_st!r}",
            )

        # JSON payload is derived from single-linkage (as in the original heredoc),
        # but falls back to complete-linkage if single is missing, and to sentinels
        # if both are missing.
        json_levels = single_levels if single_levels is not None else complete_levels
        write_json(output_json, build_json_payload(scheme, json_levels))
    except Exception as exc:  # noqa: BLE001
        logging.exception("Output write failed, attempting full sentinel trio")
        try:
            _write_full_sentinel_trio(
                output_single, output_complete, output_json, scheme,
                reason=f"output write failed: {exc}",
            )
        except Exception as inner:  # noqa: BLE001
            _unrecoverable_exit(f"sentinel fallback failed: {inner}")


if __name__ == "__main__":
    main()
