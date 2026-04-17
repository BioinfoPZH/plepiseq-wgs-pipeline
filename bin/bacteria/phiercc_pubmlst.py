"""Assign PubMLST hierCC levels for a cgMLST sequence type.

Unlike Enterobase, PubMLST does not expose hierCC levels through an API,
so the assignment is done against a locally downloaded ``sts_table.npy``
file. The output shape matches ``phiercc_enterobase.py`` so that the
downstream Nextflow module (``extract_historical_data_pubmlst``) can
consume either interchangeably.

Inputs (CLI):
    --cgmlst-parsed  : path to ``cgMLST_parsed_output.txt`` produced by the
                       cgMLST parsing step. Two tab-separated columns are read
                       from line 2: sample ST label, matching reference ST,
                       and allelic distance between them.
    --species / --genus / --qc-status / --qc-contam-status :
                       sample metadata forwarded from the Nextflow module.
    --db-dir          : root of the PubMLST database directory (typically
                       ``/db/pubmlst`` inside the container).
    --output          : path of the file to write.

Output contract: identical to ``phiercc_enterobase.py`` - see that script's
module docstring for the full explanation. The downstream module looks up
``HC25`` for ``jejuni``.

Exit codes:
    0  output was produced (happy path or any fallback).
    1  output could not be produced at all; Nextflow halts deliberately.
    2  click-level CLI misuse.
"""

import logging
import re
import sys
from dataclasses import dataclass

# Shim to load .npy files that were serialised with numpy>=2 while the
# container still pins numpy==1.23.4 (Dockerfile-bacterial line 19).
# Remove once the container is upgraded to numpy>=2.
import numpy
sys.modules["numpy._core"] = numpy.core
sys.modules["numpy._core.multiarray"] = numpy.core.multiarray
sys.modules["numpy._core._multiarray_umath"] = numpy.core._multiarray_umath

import numpy as np
import click


@dataclass(frozen=True)
class SchemeConfig:
    """Per-species configuration for a PubMLST cgMLST scheme."""

    header: tuple
    level_keys: tuple
    subpath: str


SCHEMES = {
    "jejuni": SchemeConfig(
        header=("ST", "HC5", "HC10", "HC25", "HC50", "HC100", "HC200"),
        level_keys=("d5", "d10", "d25", "d50", "d100", "d200"),
        subpath="Campylobacter/jejuni",
    ),
}


class SampleSTParseError(Exception):
    """Raised when ``cgMLST_parsed_output.txt`` cannot be parsed."""


class PubmlstDataError(Exception):
    """Raised when the local PubMLST ``sts_table.npy`` cannot be loaded/used."""


class MissingLevelError(Exception):
    """Raised when the ST entry in the local table lacks required HC keys."""


class OutputValidationError(Exception):
    """Raised when the final output file fails structural validation."""


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


def load_sts_table(db_dir, subpath):
    """Load ``<db_dir>/<subpath>/sts_table.npy`` as a dict.

    The file is written as a single numpy-pickled ``dict``; we call
    ``.item()`` to unwrap the 0-d array.
    """
    table_path = f"{db_dir.rstrip('/')}/{subpath}/sts_table.npy"
    try:
        loaded = np.load(table_path, allow_pickle=True)
    except (OSError, ValueError) as exc:
        raise PubmlstDataError(f"Cannot load {table_path!r}: {exc}") from exc
    try:
        table = loaded.item()
    except (AttributeError, ValueError) as exc:
        raise PubmlstDataError(
            f"{table_path!r} is not a pickled numpy scalar dict: {exc}"
        ) from exc
    if not isinstance(table, dict):
        raise PubmlstDataError(
            f"{table_path!r} unpickled to {type(table).__name__}, expected dict"
        )
    return table


def extract_levels(sts_table, st_matching, level_keys):
    """Pull the HC level values for ``st_matching`` from the local table."""
    try:
        entry = sts_table[st_matching]
    except KeyError as exc:
        raise MissingLevelError(
            f"Matching ST {st_matching!r} not present in PubMLST table"
        ) from exc

    missing = [k for k in level_keys if k not in entry]
    if missing:
        raise MissingLevelError(
            f"Entry for ST {st_matching!r} is missing HC keys: {missing}"
        )
    return [str(entry[k]) for k in level_keys]


def collapse_local_sts(levels, level_keys, sample_st, distance):
    """Overwrite the lowest HC levels with the local sample ST.

    See ``phiercc_enterobase.collapse_local_sts`` for the rationale - this is
    the same rule applied to the PubMLST scheme.
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


def _write_tsv(path, header, row):
    with open(path, "w") as f:
        f.write("\t".join(header) + "\n")
        f.write("\t".join(str(x) for x in row) + "\n")


def write_short_circuit_fallback(path, species):
    """Two-line ``ST\\tComment`` file used for bad QC / unsupported species."""
    _write_tsv(path, ("ST", "Comment"), ("unk", f"Unknown species: {species}"))
    validate_output(path, expected_columns=2)


def write_sentinel_fallback(path, scheme, reason):
    """Full scheme header + ``unk`` and ``-1`` sentinels.

    Used when mid-processing fails for a supported species with good QC.
    """
    logging.error("Writing sentinel fallback: %s", reason)
    sentinels = ["-1"] * (len(scheme.header) - 1)
    _write_tsv(path, scheme.header, ("unk", *sentinels))
    validate_output(path, expected_columns=len(scheme.header))


def write_result(path, scheme, sample_st, levels):
    """Happy-path writer: scheme header + sample ST + HC levels."""
    if len(levels) != len(scheme.level_keys):
        raise OutputValidationError(
            f"Expected {len(scheme.level_keys)} levels, got {len(levels)}"
        )
    _write_tsv(path, scheme.header, (sample_st, *levels))
    validate_output(path, expected_columns=len(scheme.header))


def validate_output(path, expected_columns):
    """Re-read the freshly-written file and check its shape."""
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
            f"{path!r} header has {len(header_cols)} cols, expected {expected_columns}"
        )
    if len(value_cols) != expected_columns:
        raise OutputValidationError(
            f"{path!r} row has {len(value_cols)} cols, expected {expected_columns}"
        )


def _unrecoverable_exit(path, reason):
    logging.critical("Cannot produce a valid output file at %r: %s", path, reason)
    sys.exit(1)


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
@click.option("--db-dir", "db_dir", required=True,
              type=click.Path(file_okay=False),
              help="Root of the PubMLST database directory (e.g. /db/pubmlst).")
@click.option("--output", required=True, type=click.Path(dir_okay=False),
              help="Path to write the parsed_phiercc_pubmlst.txt file.")
def main(cgmlst_parsed, species, genus, qc_status, qc_contam_status,
         db_dir, output):
    """Assign PubMLST hierCC levels for a cgMLST ST; always emit a valid output."""
    logging.basicConfig(
        stream=sys.stderr,
        level=logging.INFO,
        format="[phiercc_pubmlst] %(levelname)s %(message)s",
    )

    if qc_status.lower() == "nie" or qc_contam_status.lower() == "nie":
        try:
            write_short_circuit_fallback(output, species)
        except Exception as exc:
            _unrecoverable_exit(output, f"short-circuit fallback failed: {exc}")
        return

    if species not in SCHEMES:
        logging.warning("Species %r is not supported by PubMLST scheme, "
                        "writing short-circuit fallback", species)
        try:
            write_short_circuit_fallback(output, species)
        except Exception as exc:
            _unrecoverable_exit(output, f"short-circuit fallback failed: {exc}")
        return

    scheme = SCHEMES[species]
    try:
        sample_st, matching_st, distance = read_sample_st(cgmlst_parsed)
        sts_table = load_sts_table(db_dir, scheme.subpath)
        raw_levels = extract_levels(sts_table, matching_st, scheme.level_keys)
        levels = collapse_local_sts(
            raw_levels, scheme.level_keys, sample_st, distance,
        )
        write_result(output, scheme, sample_st, levels)
    except Exception as exc:  # noqa: BLE001 - deliberately broad for fallback
        logging.exception("pHierCC assignment failed, falling back to sentinels")
        try:
            write_sentinel_fallback(output, scheme, reason=str(exc))
        except Exception as inner:
            _unrecoverable_exit(output, f"sentinel fallback failed: {inner}")


if __name__ == "__main__":
    main()
