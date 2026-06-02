"""Assign Enterobase hierCC levels for a cgMLST sequence type.

Queries the Enterobase API (https://enterobase.readthedocs.io/en/latest/api/)
for a given sample's matching cgMLST ST and writes a tab-separated table
with the HC cluster assignments for every level of the scheme.

Inputs (CLI):
    --cgmlst-parsed  : path to ``cgMLST_parsed_output.txt`` produced by the
                       cgMLST parsing step. Two tab-separated columns are read
                       from line 2: sample ST label, matching reference ST,
                       and allelic distance between them.
    --species / --genus / --qc-status / --qc-contam-status :
                       sample metadata forwarded from the Nextflow module.
    --api-token       : Enterobase API token (passed via Nextflow params).
    --output          : path of the file to write.

Output contract:
    The downstream ``extract_historical_data_enterobase`` module reads the
    output file with ``get_hiercc_level`` - first line = tab-separated
    header keys, second line = tab-separated values - and looks up
    ``HC5`` (Salmonella) or ``HC20`` (Escherichia). This script therefore
    guarantees one of three output shapes:

    1. Happy path: scheme-specific header + values fetched from Enterobase.
    2. Short-circuit fallback (bad QC or unsupported genus): two-line file
       ``ST\\tComment`` / ``unk\\tUnknown species: <x>``. Downstream's own
       guards handle this case without reading HC levels.
    3. Mid-processing fallback (API error / malformed response for a
       supported genus): scheme-specific header + ``unk`` and ``-1`` sentinels
       on every HC level. Downstream reads the expected keys, finds no
       matching strains in its reference DB, and emits an empty historical
       table plus a valid JSON - pipeline continues.

Exit codes:
    0  output was produced (happy path or any fallback).
    1  output could not be produced at all (disk full, validation failed);
       Nextflow halts the pipeline deliberately.
    2  click-level CLI misuse.
"""

import base64
import json
import logging
import re
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Sequence

import click


ENTEROBASE_BASE_URL = "https://enterobase.warwick.ac.uk/api/v2.0"
HTTP_TIMEOUT_SECONDS = 30
HTTP_RETRIES = 2


@dataclass(frozen=True)
class SchemeConfig:
    """Per-genus configuration for an Enterobase cgMLST scheme."""

    database: str
    scheme_name: str
    header: tuple
    level_keys: tuple


SCHEMES = {
    "Salmonella": SchemeConfig(
        database="senterica",
        scheme_name="cgMLST_v2",
        header=(
            "ST", "HC0", "HC2", "HC5", "HC10", "HC20", "HC50", "HC100",
            "HC200", "HC400", "HC900(ceBG)", "HC2000", "HC2600", "HC2850(subsp.)",
        ),
        level_keys=(
            "d0", "d2", "d5", "d10", "d20", "d50", "d100", "d200",
            "d400", "d900", "d2000", "d2600", "d2850",
        ),
    ),
    "Escherichia": SchemeConfig(
        database="ecoli",
        scheme_name="cgMLST",
        header=(
            "ST", "HC0", "HC2", "HC5", "HC10", "HC20", "HC50", "HC100",
            "HC200", "HC400", "HC1100(cgST Cplx)", "HC1500", "HC2000", "HC2350(subsp.)",
        ),
        level_keys=(
            "d0", "d2", "d5", "d10", "d20", "d50", "d100", "d200",
            "d400", "d1100", "d1500", "d2000", "d2350",
        ),
    ),
}


class SampleSTParseError(Exception):
    """Raised when ``cgMLST_parsed_output.txt`` cannot be parsed."""


class EnterobaseAPIError(Exception):
    """Raised on unrecoverable API failure (after retries)."""


class MissingLevelError(Exception):
    """Raised when the API response is missing one or more expected HC levels."""


class OutputValidationError(Exception):
    """Raised when the final output file fails structural validation."""


def read_sample_st(path):
    """Read ``(ST_sample, ST_matching, distance)`` from the parsed cgMLST file.

    The file has a header row starting with ``ST_sample`` followed by a data
    row. We return the 3 fields from the data row.
    """
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


def build_authenticated_request(url, token):
    """Build an Enterobase ``urllib`` request carrying the Basic auth header.

    The credential is encoded as ``base64(token + ":")`` per RFC 7617
    (token-as-username + empty password). Enterobase also accepts a trailing
    whitespace after the colon, but the canonical form is preferred.
    """
    encoded = base64.b64encode(f"{token}:".encode("utf-8")).decode()
    headers = {"Authorization": f"Basic {encoded}"}
    return urllib.request.Request(url, None, headers)


def fetch_phiercc_entry(database, scheme_name, st_id, token,
                        *, timeout=HTTP_TIMEOUT_SECONDS, retries=HTTP_RETRIES):
    """Query Enterobase for a single ST entry and return the parsed JSON.

    Retries transient 5xx errors up to ``retries`` times. Raises
    ``EnterobaseAPIError`` on permanent failure (4xx, exhausted retries,
    network error, or empty ``STs`` list).
    """
    url = (f"{ENTEROBASE_BASE_URL}/{database}/{scheme_name}/sts"
           f"?st_id={st_id}&scheme={scheme_name}&limit=5")
    request = build_authenticated_request(url, token)

    last_error = None
    for attempt in range(retries + 1):
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                data = json.load(response)
            sts = data.get("STs") or []
            if not sts:
                raise EnterobaseAPIError(
                    f"Enterobase returned no STs for st_id={st_id} in "
                    f"{database}/{scheme_name}"
                )
            return sts[0]
        except urllib.error.HTTPError as exc:
            last_error = exc
            if 500 <= exc.code < 600 and attempt < retries:
                logging.warning("Enterobase API %s on attempt %d/%d, retrying",
                                exc.code, attempt + 1, retries + 1)
                continue
            raise EnterobaseAPIError(
                f"Enterobase API HTTP {exc.code} {exc.reason} for {url}"
            ) from exc
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            last_error = exc
            if attempt < retries:
                logging.warning("Enterobase network error on attempt %d/%d: %s",
                                attempt + 1, retries + 1, exc)
                continue
            raise EnterobaseAPIError(f"Enterobase network error: {exc}") from exc

    raise EnterobaseAPIError(f"Unreachable: retries exhausted ({last_error})")


def extract_levels(api_entry, level_keys):
    """Extract the HC level values (as strings) from an Enterobase ST entry."""
    try:
        hiercc = api_entry["info"]["hierCC"]
    except (KeyError, TypeError) as exc:
        raise MissingLevelError(f"API entry is missing 'info.hierCC': {exc}") from exc

    missing = [k for k in level_keys if k not in hiercc]
    if missing:
        raise MissingLevelError(
            f"API response is missing HC keys: {missing}"
        )
    return [str(hiercc[k]) for k in level_keys]


def collapse_local_sts(levels, level_keys, sample_st, distance):
    """Overwrite the lowest HC levels with the local sample ST.

    When the sample's allelic distance to its closest reference ST is > 0,
    any HC level whose radius is strictly less than ``distance`` cannot
    possibly contain the sample - we therefore substitute the sample's
    local ST label for those low-radius levels, matching the original
    heredoc's behaviour.
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
    """Two-line ``ST\\tComment`` file used for bad QC / unsupported genus."""
    _write_tsv(path, ("ST", "Comment"), ("unk", f"Unknown species: {species}"))
    validate_output(path, expected_columns=2)


def write_sentinel_fallback(path, scheme, reason):
    """Full scheme header + ``unk`` and ``-1`` sentinels.

    Used when mid-processing fails for a supported genus with good QC.
    Downstream will look up ``HC5`` / ``HC20`` in the resulting dict, get
    ``-1``, find no matching strains in its reference database, and produce
    an empty historical-data table plus a valid JSON.
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
    """Re-read the freshly-written file and check its shape.

    Guards against silent truncation (disk full, killed process, etc).
    """
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
    """Log and exit 1 when even the fallback write failed."""
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
@click.option("--api-token", "api_token", required=True,
              help="Enterobase API token.")
@click.option("--output", required=True, type=click.Path(dir_okay=False),
              help="Path to write the parsed_phiercc_enterobase.txt file.")
def main(cgmlst_parsed, species, genus, qc_status, qc_contam_status,
         api_token, output):
    """Fetch Enterobase hierCC levels for a cgMLST ST; always emit a valid output."""
    logging.basicConfig(
        stream=sys.stderr,
        level=logging.INFO,
        format="[phiercc_enterobase] %(levelname)s %(message)s",
    )

    if qc_status.lower() == "nie" or qc_contam_status.lower() == "nie":
        try:
            write_short_circuit_fallback(output, species)
        except Exception as exc:
            _unrecoverable_exit(output, f"short-circuit fallback failed: {exc}")
        return

    if genus not in SCHEMES:
        logging.warning("Genus %r is not supported by Enterobase, "
                        "writing short-circuit fallback", genus)
        try:
            write_short_circuit_fallback(output, species)
        except Exception as exc:
            _unrecoverable_exit(output, f"short-circuit fallback failed: {exc}")
        return

    scheme = SCHEMES[genus]
    try:
        sample_st, matching_st, distance = read_sample_st(cgmlst_parsed)
        api_entry = fetch_phiercc_entry(
            scheme.database, scheme.scheme_name, matching_st, api_token,
        )
        raw_levels = extract_levels(api_entry, scheme.level_keys)
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
