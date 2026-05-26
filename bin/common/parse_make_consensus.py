#!/usr/bin/env python3

"""
This script parse the results of picard_wgsmetrics module in the viral pipeline to
produce a valid json (the viral_genom_data group of fileds)
"""

import sys
import click
import json
from Bio import SeqIO


AMBIGUOUS_NUCLEOTIDES = {'R', 'Y', 'S', 'W', 'K', 'M', 'B', 'D', 'H', 'V'}


def parse_fasta(plik_fasta):
    total_length = 0
    total_N = 0
    total_ambiguous = 0
    segment = None

    record = SeqIO.parse(plik_fasta, "fasta")
    for r in record:
        segment = r.id.replace(">", "")
        for element in str(r.seq).upper():  # Iterate over sequence
            if element == 'N':
                total_N += 1
            elif element in AMBIGUOUS_NUCLEOTIDES:
                total_ambiguous += 1
            total_length += 1

    return segment, total_length, total_N, total_ambiguous


@click.command()
@click.option('-i', '--input_fastas', help='[INPUT] a list of segment-specific fastas produced by this module ',
              type=click.Path(), required=False)
@click.option('-k', '--output_path', help='[INPUT] a path where all the relevant files are put by nextflow',
              type=str)
@click.option('-s', '--status', help='[INPUT] PREDEFINED status that is transferred to an output json. '
                                     'If this status was either nie or blad fastqc will not run',
              type=click.Choice(['tak', 'nie', 'blad'], case_sensitive=False),  required=True)
@click.option('-r', '--error', help='[INPUT] PREDEFINED error message that is put in json. '
                                    'Only used when status was set to nie or blad',
              type=str,  required=False, default="")
@click.option('-o', '--output', help='[Output] Name of a file with json output',
              type=str,  required=True)
def main_program(status, output, input_fastas, output_path, error=""):
    total_length_value = 0
    number_of_Ns_value = 0
    number_of_ambiguous_value = 0
    if status != "tak":
        json_output = {"status": status,
                       "error_message": error,
                       "total_length_value": total_length_value,
                       "number_of_Ns_value": number_of_Ns_value,
                       "number_of_ambiguous_value": number_of_ambiguous_value}
    else:
        file_data = []
        with open(input_fastas) as f:
            for line in f:
                segment, segment_length, segment_N, segment_ambiguous = parse_fasta(line.rstrip())
                total_length_value += segment_length
                number_of_Ns_value += segment_N
                number_of_ambiguous_value += segment_ambiguous
                file_data.append({"segment_name": segment,
                                  "segment_file": f'{output_path}/{line.rstrip()}',
                                  "total_length_value": segment_length,
                                  "number_of_Ns_value": segment_N,
                                  "number_of_ambiguous_value": segment_ambiguous})

        json_output = {
            "status": status,
            "total_length_value": total_length_value,
            "number_of_Ns_value": number_of_Ns_value,
            "number_of_ambiguous_value": number_of_ambiguous_value,
            "file_data": file_data
        }
    with open(output, 'w') as f1:
        f1.write(json.dumps(json_output, ensure_ascii=False, indent=4))

    return True


if __name__ == '__main__':
    # The main program returns 3 variables: "status", number_of_reads in a sample, and median_quality_of_reads
    # These can be used to determine QC status in tha module
    if len(sys.argv) == 1:
        main_program(['--help'])
    else:
        main_program(sys.argv[1:])
