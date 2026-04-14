#!/usr/bin/env python3
"""
RSV F protein resistance mutation analysis for the pipeline.

Single-sample mode: accepts one consensus genome FASTA with a known subtype,
runs Nextclade solely to extract the F protein translation, then aligns that
protein against the repository reference F protein with mafft. Resistance
mutations are detected exclusively from the alignment against the known
reference (data/rsv/fusion_protein/{A,B}/F.fasta).

Outputs a JSON file compliant with the rsv_data resistance sub-schema.

Source for resistance data: virusfrenchresistance.org V3 (November 2025)
"""

import argparse
import json
import os
import subprocess
import sys
import tempfile

from Bio import SeqIO

# ── Resistance mutation definitions ──────────────────────────────────────────
# Positions are relative to the reference F protein stored in the repository.
# Categories: resistance (FC>10), possible_resistance (FC 5-10), no_impact (FC<5)
# Combinations are listed as tuples of mutations (all must be present).

RESISTANCE_DB = {
    'A': {
        'Nirsevimab': {
            'resistance': [
                ('N67I', 'N208Y'),
            ],
            'possible_resistance': [
                ('K68E',),
                ('K209E',),
            ],
            'no_impact': [
                ('N63T',), ('N63S',),
                ('I64V',),
                ('K65R',),
                ('E66K',), ('E66G',),
                ('K68N',), ('K68R',),
                ('N197K',), ('N197H',), ('N197D',),
                ('I199M',),
                ('L204I',),
                ('I206T',), ('I206I',),
                ('V207I',),
                ('K209R',),
                ('Q210L',),
                ('S211R',),
            ],
        },
        'Palivizumab': {
            'resistance': [
                ('K272M',), ('K272T',),
                ('S275F',),
            ],
            'possible_resistance': [],
            'no_impact': [
                ('E66G',),
            ],
        },
        'Clesrovimab': {
            'resistance': [
                ('S443P',),
                ('G446E',), ('G446R',), ('G446W',),
            ],
            'possible_resistance': [],
            'no_impact': [
                ('I432T',), ('I432V',),
                ('D440G',),
            ],
        },
    },
    'B': {
        'Nirsevimab': {
            'resistance': [
                ('I64T',),
                ('I64T', 'K68E'),
                ('I64M', 'K65E'),
                ('I64V', 'K65E'),
                ('K68E',), ('K68Q',), ('K68I',),
                ('K68N', 'N201S'),
                ('K68N', 'N208S'),
                ('N201S',), ('N201T',),
                ('L204S',),
                ('P205S',),
                ('N208S',), ('N208D',), ('N208K',), ('N208Y',), ('N208I',),
            ],
            'possible_resistance': [
                ('K65Q', 'S211N'),
                ('K68N',),
                ('K65Q',), ('K65T',),
                ('K65E',),
            ],
            'no_impact': [
                ('N63S',), ('N63D',),
                ('I64V',), ('I64M',),
                ('K65R',),
                ('E66D',),
                ('T67A',), ('T67I',),
                ('K68R',),
                ('N197D',), ('N197S',),
                ('I206M',),
                ('Q209R',), ('Q209K',), ('Q209L',),
                ('Q210H',), ('Q210L',),
                ('S211N',), ('S211I',),
            ],
        },
        'Palivizumab': {
            'resistance': [
                ('K272N',), ('K272Q',),
            ],
            'possible_resistance': [
                ('N63S',),
                ('K272R',),
            ],
            'no_impact': [],
        },
        'Clesrovimab': {
            'resistance': [
                ('S443P',),
                ('G446E',), ('G446W',), ('G446R',),
            ],
            'possible_resistance': [],
            'no_impact': [
                ('I432V',), ('I432T',),
                ('K445R',),
            ],
        },
    },
}

CATEGORY_PRIORITY = {'resistance': 0, 'possible_resistance': 1, 'no_impact': 2}
DRUGS = ['Nirsevimab', 'Palivizumab', 'Clesrovimab']


# ── Nextclade (used only to extract F protein translation) ───────────────────

def run_nextclade(fasta_file, dataset_zip, output_dir):
    """Run Nextclade to produce F protein translation FASTA."""
    os.makedirs(output_dir, exist_ok=True)
    tsv_path = os.path.join(output_dir, 'nextclade.tsv')
    cmd = (f'nextclade run --input-dataset {dataset_zip} '
           f'--output-tsv {tsv_path} --output-all {output_dir} '
           f'{fasta_file}')
    subprocess.run(cmd, shell=True, check=True)

    with open(tsv_path) as fh:
        header = fh.readline().rstrip('\n').split('\t')
        for line in fh:
            cols = line.rstrip('\n').split('\t')
            return dict(zip(header, cols))
    return None


# ── Resistance checking ──────────────────────────────────────────────────────

def check_resistance(f_mutations, subtype):
    """Check detected F protein mutations against resistance database.

    f_mutations must use reference numbering (matching RESISTANCE_DB positions).
    Returns list of (drug, category, pattern_label, matched_mutations).
    """
    if subtype not in RESISTANCE_DB:
        return []

    hits = []
    for drug, categories in RESISTANCE_DB[subtype].items():
        for category in ('resistance', 'possible_resistance', 'no_impact'):
            for pattern in categories.get(category, []):
                if all(mut in f_mutations for mut in pattern):
                    hits.append((drug, category, '+'.join(pattern),
                                 list(pattern)))
    return hits


# ── F protein alignment ──────────────────────────────────────────────────────

def load_ref_protein(ref_protein_dir, subtype):
    """Load reference F protein sequence for a given subtype (A or B)."""
    fasta_path = os.path.join(ref_protein_dir, subtype, 'F.fasta')
    if not os.path.exists(fasta_path):
        return None, None
    record = next(SeqIO.parse(fasta_path, 'fasta'))
    return record.id, str(record.seq).rstrip('*')


def load_nextclade_f_protein(subtype):
    """Load the single F protein translation produced by Nextclade."""
    fasta_path = os.path.join(
        f'nextclade_RSV_{subtype}', 'nextclade.cds_translation.F.fasta')
    if not os.path.exists(fasta_path):
        return None, None
    record = next(SeqIO.parse(fasta_path, 'fasta'))
    return record.id, str(record.seq).rstrip('*')


def align_two_proteins(ref_id, ref_seq, sample_id, sample_seq):
    """Pairwise-align reference and sample F proteins with mafft."""
    with tempfile.NamedTemporaryFile(
            mode='w', suffix='.fasta', delete=False) as tmp_in:
        tmp_in.write(f'>{ref_id}\n{ref_seq}\n')
        tmp_in.write(f'>{sample_id}\n{sample_seq}\n')
        tmp_in_path = tmp_in.name

    tmp_out_path = tmp_in_path + '.aligned'
    try:
        subprocess.run(
            f'mafft --auto --quiet {tmp_in_path} > {tmp_out_path}',
            shell=True, check=True)
        aligned_ref = None
        aligned_sample = None
        for rec in SeqIO.parse(tmp_out_path, 'fasta'):
            if rec.id == ref_id:
                aligned_ref = str(rec.seq)
            else:
                aligned_sample = str(rec.seq)
        return aligned_ref, aligned_sample
    finally:
        for p in (tmp_in_path, tmp_out_path):
            if os.path.exists(p):
                os.unlink(p)


def detect_mutations_from_alignment(aligned_ref, aligned_sample):
    """Compare aligned sample to aligned reference.

    Returns two dicts mapping (ref_aa, alt) -> mutation string:
      - ref_numbered: mutations in reference F.fasta numbering (e.g. K68E)
      - sample_numbered: same mutations in sample numbering (accounting for indels)
    Also returns the ref_numbered mutations as a plain set for resistance lookup.
    """
    ref_numbered = {}
    sample_numbered = {}
    ref_mutations_set = set()
    ref_pos = 0
    sample_pos = 0

    for ref_aa, sample_aa in zip(aligned_ref, aligned_sample):
        is_ref_gap = (ref_aa == '-')
        is_sample_gap = (sample_aa == '-')

        if not is_ref_gap:
            ref_pos += 1
        if not is_sample_gap:
            sample_pos += 1

        if is_ref_gap:
            continue

        if is_sample_gap:
            ref_mut = f'{ref_aa}{ref_pos}-'
            ref_mutations_set.add(ref_mut)
        elif sample_aa != ref_aa and sample_aa != 'X':
            ref_mut = f'{ref_aa}{ref_pos}{sample_aa}'
            sample_mut = f'{ref_aa}{sample_pos}{sample_aa}'
            ref_mutations_set.add(ref_mut)
            ref_numbered[ref_mut] = ref_mut
            sample_numbered[ref_mut] = sample_mut

    return ref_mutations_set, ref_numbered, sample_numbered


# ── JSON output construction ─────────────────────────────────────────────────

def build_drug_entry(drug, hits_for_drug, ref_to_sample_map):
    """Build one element of the resistance_data array for a single drug.

    hits_for_drug: list of (category, pattern_label, matched_mutations)
    ref_to_sample_map: dict mapping ref-numbered mutation -> sample-numbered mutation
    """
    worst_category = 'no_impact'
    mutation_list_ref = []
    mutation_list_sample = []
    seen_patterns = set()

    for category, pattern_label, matched in hits_for_drug:
        if CATEGORY_PRIORITY[category] < CATEGORY_PRIORITY[worst_category]:
            worst_category = category

        if pattern_label in seen_patterns:
            continue
        seen_patterns.add(pattern_label)

        mutation_list_ref.append({
            'mutation_name': pattern_label,
            'mutation_effect': category,
        })

        sample_parts = [ref_to_sample_map.get(mut, mut) for mut in matched]
        mutation_list_sample.append({
            'mutation_name': '+'.join(sample_parts),
            'mutation_effect': category,
        })

    if not mutation_list_ref:
        worst_category = 'no_impact'

    return {
        'drug_name': drug,
        'drug_resistance_status': worst_category,
        'mutation_list_data': mutation_list_sample,
        'mutation_list_data_reference_numbering': mutation_list_ref,
    }


def build_error_json(message):
    return {
        'resistance_status': 'nie',
        'resistance_error_message': message,
    }


def build_success_json(subtype, ref_mutations_set, ref_to_sample_map):
    """Build the full resistance output JSON.

    All mutation data comes exclusively from the alignment against the
    repository reference F protein.
    """
    hits = check_resistance(ref_mutations_set, subtype)

    hits_by_drug = {d: [] for d in DRUGS}
    for drug, category, pattern_label, matched in hits:
        hits_by_drug[drug].append((category, pattern_label, matched))

    resistance_data = []
    for drug in DRUGS:
        entry = build_drug_entry(drug, hits_by_drug[drug], ref_to_sample_map)
        resistance_data.append(entry)

    return {
        'resistance_status': 'tak',
        'resistance_data': resistance_data,
    }


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('fasta', help='Input single-sample FASTA with RSV consensus')
    ap.add_argument('nextclade_db', help='Directory containing RSV_A.zip and RSV_B.zip')
    ap.add_argument('ref_protein_dir',
                    help='Directory with reference F proteins: {dir}/A/F.fasta, {dir}/B/F.fasta')
    ap.add_argument('subtype', choices=['A', 'B'],
                    help='RSV subtype determined by the pipeline')
    ap.add_argument('output', help='Output JSON file path')
    ap.add_argument('--lan', default='en', choices=['pl', 'en'],
                    help='Language for error messages')
    args = ap.parse_args()

    dataset_zip = os.path.join(args.nextclade_db, f'RSV_{args.subtype}.zip')
    if not os.path.exists(dataset_zip):
        if args.lan == 'pl':
            msg = f"Brak bazy Nextclade: {dataset_zip}"
        else:
            msg = f"Nextclade dataset not found: {dataset_zip}"
        json.dump(build_error_json(msg), open(args.output, 'w'),
                  ensure_ascii=False, indent=2)
        return

    # Step 1: Run Nextclade to extract F protein translation
    print(f"Running Nextclade against RSV-{args.subtype} "
          f"(F protein extraction only)...", file=sys.stderr)
    outdir = f'nextclade_RSV_{args.subtype}'
    try:
        nc_row = run_nextclade(args.fasta, dataset_zip, outdir)
    except subprocess.CalledProcessError as e:
        if args.lan == 'pl':
            msg = f"Nextclade zakończył się błędem: {e}"
        else:
            msg = f"Nextclade failed: {e}"
        json.dump(build_error_json(msg), open(args.output, 'w'),
                  ensure_ascii=False, indent=2)
        return

    if nc_row is None:
        if args.lan == 'pl':
            msg = "Nextclade nie zwrócił wyników dla tej próbki."
        else:
            msg = "Nextclade returned no results for this sample."
        json.dump(build_error_json(msg), open(args.output, 'w'),
                  ensure_ascii=False, indent=2)
        return

    failed_cdses = nc_row.get('failedCdses', '')
    if 'F' in failed_cdses.split(','):
        if args.lan == 'pl':
            msg = "Gen F nie został poprawnie przetłumaczony przez Nextclade (failedCdses)."
        else:
            msg = "F gene translation failed in Nextclade (failedCdses)."
        json.dump(build_error_json(msg), open(args.output, 'w'),
                  ensure_ascii=False, indent=2)
        return

    # Step 2: Align sample F protein against repository reference
    print("Aligning F protein against reference...", file=sys.stderr)
    ref_id, ref_seq = load_ref_protein(args.ref_protein_dir, args.subtype)
    sample_id, sample_seq = load_nextclade_f_protein(args.subtype)

    if not (ref_id and sample_id and ref_seq and sample_seq):
        if args.lan == 'pl':
            msg = "Nie udało się załadować białka F (referencji lub próbki) do alignmentu."
        else:
            msg = "Could not load F protein sequences (reference or sample) for alignment."
        json.dump(build_error_json(msg), open(args.output, 'w'),
                  ensure_ascii=False, indent=2)
        return

    aligned_ref, aligned_sample = align_two_proteins(
        ref_id, ref_seq, sample_id, sample_seq)

    if not (aligned_ref and aligned_sample):
        if args.lan == 'pl':
            msg = "Alignment białka F zakończył się niepowodzeniem (mafft)."
        else:
            msg = "F protein alignment failed (mafft)."
        json.dump(build_error_json(msg), open(args.output, 'w'),
                  ensure_ascii=False, indent=2)
        return

    ref_mutations_set, ref_numbered, sample_numbered = \
        detect_mutations_from_alignment(aligned_ref, aligned_sample)

    # Step 3: Build output using alignment-derived mutations only
    result = build_success_json(args.subtype, ref_mutations_set, sample_numbered)
    with open(args.output, 'w') as fh:
        json.dump(result, fh, ensure_ascii=False, indent=2)

    print("Done.", file=sys.stderr)


if __name__ == '__main__':
    main()
