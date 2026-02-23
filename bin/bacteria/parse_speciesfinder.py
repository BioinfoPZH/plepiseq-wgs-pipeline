#!/usr/bin/env python3
"""Parse SpeciesFinder/KmerFinder results.spa and return genus or species."""
import math
import numpy as np
import click


def _zscore(values):
    if not values:
        return []
    arr = np.array(values, dtype=float)
    mu = float(np.mean(arr))
    sigma = float(np.std(arr))
    if sigma == 0:
        return [0.0 for _ in values]
    return ((arr - mu) / sigma).tolist()


def _parse_template_name(template_value):
    """
    Parse first column from results.spa:
    '<accession> <Genus> <species> ...'
    """
    tokens = template_value.strip().split()
    if len(tokens) < 3:
        return "", ""
    genus = tokens[1]
    species = f"{tokens[1]} {tokens[2]}"
    return genus, species


def _load_spa_entries(path):
    entries = []
    with open(path, "r") as handle:
        header = []
        for raw_line in handle:
            line = raw_line.strip()
            if not line:
                continue
            if line.startswith("#Template"):
                header = [x.lstrip("#").strip() for x in line.split("\t")]
                continue
            if not header:
                continue
            parts = [x.strip() for x in line.split("\t")]
            if len(parts) != len(header):
                continue
            row = dict(zip(header, parts))
            try:
                template_length = float(row.get("Template_length", "0"))
                template_identity = float(row.get("Template_Identity", "0"))
                depth = float(row.get("Depth", "0"))
            except ValueError:
                continue
            entries.append(
                {
                    "template": row.get("Template", ""),
                    "Template_length": template_length,
                    "Template_Identity": template_identity,
                    "Depth": depth,
                }
            )
    return entries


def _pick_best_from_spa(path):
    rows = _load_spa_entries(path)
    rows = [r for r in rows if r["Template_length"] >= 1_000_000]
    if not rows:
        return None

    # Depth has no hard upper bound; log transform reduces outlier dominance.
    depth_vals = [math.log1p(r["Depth"]) for r in rows]
    identity_vals = [r["Template_Identity"] for r in rows]
    z_depth = _zscore(depth_vals)
    z_ident = _zscore(identity_vals)

    for i, row in enumerate(rows):
        row["score"] = (z_depth[i] + z_ident[i]) / 2.0

    return max(rows, key=lambda x: x["score"])


def _extract_value(input_file, out_kind):
    best = _pick_best_from_spa(input_file)
    if best is None:
        return ""
    genus, species = _parse_template_name(best["template"])
    return species if out_kind == "species" else genus


@click.command()
@click.argument("input_file", type=click.Path(exists=True, dir_okay=False))
@click.argument("what", type=click.Choice(["genus", "species"], case_sensitive=False))
def main(input_file, what):
    print(_extract_value(input_file, what.lower()))


if __name__ == "__main__":
    main()
