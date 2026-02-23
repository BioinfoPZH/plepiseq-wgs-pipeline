#!/usr/bin/env python3
import json
import sys
import math
import click
import numpy as np


@click.command()
@click.option('-k', '--input_kraken', help="[INPUT] a path to a kraken2 output. Use "
                                           "\"skip\" to not include results of this program in json output",
              type=str,  required=True)
@click.option('-g', '--input_metaphlan_genera', help="[INPUT] a path to a metaphlan output. Use "
                                                     "\"skip\" to not include results of this program in json output",
              type=str,  required=True)
@click.option('-x', '--input_metaphlan_species', help="[INPUT] a path to a metaphlan output. Use "
                                                      "\"skip\" to not include results of this program in json output",
              type=str,  required=True)
@click.option('input_speciesfinder', '-y', '--input_speciesfinder', '--input_kmerfinder',
              help='[INPUT] a path to a SpeciesFinder output (`results.txt`). Use '
                   '"skip" to not include results of this program in json output.',
              type=str, required=True)
@click.option('-s', '--status', help='[INPUT] PREDEFINED status that is transferred to an output json. '
                                     'If this status was either nie or blad fastqc will not run',
              type=click.Choice(['tak', 'nie', 'blad'], case_sensitive=False),  required=True)
@click.option('-m', '--error', help='[INPUT] PREDEFINED error message that is put in json. '
                                    'Only used when status was set to nie or blad',
              type=str,  required=False, default="")
@click.option('-o', '--output', help='[Output] Name of a file with json output',
              type=str,  required=True)
def main_program(input_kraken, input_metaphlan_genera, input_metaphlan_species, input_speciesfinder,
                 status, error, output):

    full_output = []
    # Kraken2 results section
    if input_kraken == "skip":
        # ignore kraken2 whatsoever
        pass
    else:
        if status != "tak":
            kraken2_json = {"program_name": "kraken2",
                            "status": status,
                            "error_message": error}
        else:
            f = open(input_kraken).readlines()
            genus_dict = {}
            species_dict = {}
            for line in f:
                line = line.split()
                if line[3] == "S":
                    try:
                        species_dict[" ".join(list(map(str, line[5:])))] = float(line[0])
                        #species_dict[line[5] + " " + line[6]] = float(line[0])
                    except IndexError:
                        species_dict[line[5]] = float(line[0])
                elif line[3] == "G":
                    genus_dict[line[5]] = float(line[0])
            genus_names_sorted = sorted(genus_dict, key=lambda x: genus_dict[x], reverse=True)
            species_names_sorted = sorted(species_dict, key=lambda x: species_dict[x], reverse=True)
            if len(genus_names_sorted) == 1:
                genus_names_sorted.append('None')
                genus_dict['None'] = 0
            if len(species_names_sorted) == 1:
                species_names_sorted.append('None')
                species_dict['None'] = 0
            kraken2_json = {"program_name": "kraken2",
                            "status": "tak",
                            "main_genus_name": genus_names_sorted[0],
                            "secondary_genus_name": genus_names_sorted[1],
                            "main_species_name": species_names_sorted[0],
                            "secondary_species_name": species_names_sorted[1],
                            "main_genus_value": round(genus_dict[genus_names_sorted[0]], 2),
                            "secondary_genus_value": round(genus_dict[genus_names_sorted[1]], 2),
                            "main_species_value": round(species_dict[species_names_sorted[0]], 2),
                            "secondary_species_value": round(species_dict[species_names_sorted[1]], 2)}

        full_output.append(kraken2_json)

    # Metaphlan results section
    if input_metaphlan_genera == "skip" and input_metaphlan_species == "skip":
        pass
    else:
        if status != "tak":
            metaphlan_json = {"program_name": "metaphlan",
                              "status": status,
                              "error_message": error}
        else:
            f1 = open(input_metaphlan_genera).readlines()
            f2 = open(input_metaphlan_species).readlines()
            genus_dict = {}
            species_dict = {}
            for line in f1:
                line = line.split()
                if "g__" in line[0]:
                    genus_dict[line[0].split('_')[-1]] = float(line[2])
            for line in f2:
                line = line.split()
                if "s__" in line[0]:
                    species_dict[" ".join(line[0].split('_')[-2:])] = float(line[2])

            genus_names_sorted = sorted(genus_dict, key=lambda x: genus_dict[x], reverse=True)
            species_names_sorted = sorted(species_dict, key=lambda x: species_dict[x], reverse=True)
            if len(genus_names_sorted) == 1:
                genus_names_sorted.append('None')
                genus_dict['None'] = 0
            if len(species_names_sorted) == 1:
                species_names_sorted.append('None')
                species_dict['None'] = 0

            metaphlan_json = {"program_name": "metaphlan",
                              "status": "tak",
                              "main_genus_name": genus_names_sorted[0],
                              "secondary_genus_name": genus_names_sorted[1],
                              "main_species_name": species_names_sorted[0],
                              "secondary_species_name": species_names_sorted[1],
                              "main_genus_value": round(genus_dict[genus_names_sorted[0]], 2),
                              "secondary_genus_value": round(genus_dict[genus_names_sorted[1]], 2),
                              "main_species_value": round(species_dict[species_names_sorted[0]], 2),
                              "secondary_species_value": round(species_dict[species_names_sorted[1]], 2)}

        full_output.append(metaphlan_json)
    # SpeciesFinder (results.txt)
    if input_speciesfinder == "skip":
        pass
    else:
        if status != "tak":
            kmerfinder_json = {"program_name": "speciesfinder",
                               "status": status,
                               "error_message": error}
        else:
            def _zscore(values):
                if not values:
                    return []
                arr = np.array(values, dtype=float)
                mu = float(np.mean(arr))
                sigma = float(np.std(arr))
                if sigma == 0:
                    return [0.0 for _ in values]
                return ((arr - mu) / sigma).tolist()

            f = open(input_speciesfinder).readlines()
            rows = []
            for line in f:
                line = line.split("\t")
                if len(line) < 15 or "#" in line[0]:
                    continue
                try:
                    template_length = float(line[3])
                    template_identity = float(line[4])
                    depth = float(line[8])
                except ValueError:
                    continue
                if template_length < 1_000_000:
                    continue

                taxonomy = [x.strip() for x in line[14].split(";") if x.strip()]
                if len(taxonomy) < 2:
                    continue
                species = taxonomy[-1]
                rows.append(
                    {
                        "species": species,
                        "template_identity": template_identity,
                        "depth": depth,
                    }
                )

            if len(rows) == 0:
                main_species_name = "None"
                secondary_species_name = "None"
                main_species_coverage = 0
                secondary_species_coverage = 0
            else:
                z_depth = _zscore([math.log1p(x["depth"]) for x in rows])
                z_identity = _zscore([x["template_identity"] for x in rows])
                for i in range(len(rows)):
                    rows[i]["score"] = (z_depth[i] + z_identity[i]) / 2.0

                ranked = sorted(rows, key=lambda x: x["score"], reverse=True)
                main_hit = ranked[0]
                second_hit = None
                for hit in ranked[1:]:
                    if hit["species"] != main_hit["species"]:
                        second_hit = hit
                        break

                main_species_name = main_hit["species"]
                main_species_coverage = round(main_hit["depth"], 2)
                if second_hit is None:
                    secondary_species_name = "None"
                    secondary_species_coverage = 0
                else:
                    secondary_species_name = second_hit["species"]
                    secondary_species_coverage = round(second_hit["depth"], 2)

            kmerfinder_json = {"program_name": "speciesfinder",
                               "status": "tak",
                               "main_species_name": main_species_name,
                               "secondary_species_name": secondary_species_name,
                               "main_species_coverage": main_species_coverage,
                               "secondary_species_coverage": secondary_species_coverage}


        full_output.append(kmerfinder_json)

    # patch
    with open(output, 'w') as f:
        f.write(json.dumps(full_output, ensure_ascii=False, indent = 4))

    return True


if __name__ == '__main__':
    if len(sys.argv) == 1:
        main_program(['--help'])
    else:
        print(main_program(sys.argv[1:]))
