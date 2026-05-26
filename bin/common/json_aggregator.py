#!/usr/bin/env python3

import argparse
import datetime
import json


def fill_infl_data(output_local, resistance="", reassortment=""):
    output_local["output"]["infl_data"] = {}

    if resistance:
        output_local["output"]["infl_data"] = {**output_local["output"]["infl_data"],
                                               **json.load(open(resistance))}
    if reassortment:
        output_local["output"]["infl_data"] = {**output_local["output"]["infl_data"],
                                               **json.load(open(reassortment))}
    return output_local


def fill_sequencing_summary_data(jsons, output_local):
    """
    @param jsons: List of jsons to be included in sequencing_summary_data
    @type jsons: list
    @param output_local:
    @type output_local: dict
    @return: update dictionary with new fileds
    @rtype: dict
    """

    if "sequencing_summary_data" not in output_local['output'].keys():
        output_local['output']['sequencing_summary_data'] = []

    for file in jsons:
        with open(file) as f:
            fastqc = json.load(f)
            output_local["output"]["sequencing_summary_data"].append(fastqc[0])

    return output_local


def fill_genome_files_data(args, output):
    output["output"]['genome_files_data']['file_data'] = []
    output["output"]["genome_files_data"]['status'] = "tak"  # TODO
    if args.pathogen == 'sars2':
        # For sars file is hardcoded. It never changes.
        output["output"]['genome_files_data']['file_data'].append({
            'segment_name': 'MN908947.3',
            'segment_path': args.publish_dir + '/output_consensus_masked_SV.fa'
        })
    elif args.pathogen == 'influenza':
        # For influenza, we need to read the list of fasta files from a file
        with open(args.list_of_fasta_files) as f:
            for line in f:
                filename = line.strip()
                segment_name = filename.split("_")[1].split(".")[0]
                output["output"]['genome_files_data']['file_data'].append({
                    'segment_name': segment_name,
                    'segment_path': filename
                })


def fill_viral_classification_data(file_path, output):
    if file_path == "non-existent":
        return
    with open(file_path) as f:
        data = json.load(f)
        print(data)
        for i in data:
            output["output"]["viral_classification_data"].append(i)


def fill_viral_mutation(file_path, output_local):
    output_local["output"]["viral_mutation_data"] = []
    with open(file_path) as f:
        for line in f:
            slownik = {}
            line = line.split()
            line[-1] = line[-1].rstrip()
            slownik["segment_name"] = line[0]
            slownik["position_value"] = int(line[1])
            slownik["gene_name"] = line[2]
            slownik["reference_allele_name"] = line[3]
            slownik["sample_allele_name"] = line[5]
            slownik["mutation_effect"] = line[6]
            slownik["mutation_type_name"] = line[7]
            if line[8] == "-":
                line[8] = 0
            if line[9] == "-":
                line[9] = 0
            slownik["mutation_coverage_value"] = int(line[8])
            slownik["mutation_usage_value"] = float(f'{float(line[9]):.2f}')
            output_local["output"]["viral_mutation_data"].append(slownik)
    return output_local


def normalize_pathogen(pathogen: str) -> str:

    if pathogen.lower() in ["sars2", "sars-cov-2"]:
        return "sars2"
    elif pathogen.lower() in ["influenza", "infl", "INFL", "flu"]:
        return "influenza"
    elif pathogen.lower() in ["rsv", "RSV", "rsv-a", "rsv-b"]:
        return "rsv"
    else:
        return pathogen


def json_aggregator(args):
    output = {"output": {}}

    if args.version:
        output["output"]["pipeline_version"] = args.version
    else:
        raise Exception('Pipeline version is an obligatory parameter for json, exiting')

    if args.pathogen:
        output["output"]["pathogen"] = normalize_pathogen(args.pathogen)
    else:
        raise Exception('Name of a pathogen is an obligatory parameter for json, exiting')

    if args.sampleId:
        output["output"]["sampleId"] = args.sampleId
    else:
        raise Exception('Sample id is an obligatory parameter for json, exiting')

    # Timestamp is a time point when json aggregator was executed and all modules in nextflow produced some output
    output["output"]["timestamp"] = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

    if args.executiondir:
        output["output"]["ExecutionDir_dir"] = args.executiondir
    else:
        raise Exception('Execution path of the program is an obligatory parameter for json, exiting')

    if args.fastqc_pre:
        # update json dict if user provided valid information
        output = fill_sequencing_summary_data(args.fastqc_pre, output)
    else:
        raise Exception('Sequencing summary parameter for at least one fastq file must be present in json, exiting')

    if args.fastqc_post:
        # In naopore there are no "post" filtering fastq, hence this is not a mandatory step
        output = fill_sequencing_summary_data(args.fastqc_post, output)

    if args.contamination:
        output["output"]["contamination_data"] = json.load(open(args.contamination))
    else:
        raise Exception('Contamination analysis must always be present in json, exiting')

    if args.dehumanized:
        output["output"]["dehumanized_data"] = json.load(open(args.dehumanized))

    if args.coinfection and args.pathogen == "sars2":
        output["output"]["sars_data"] = json.load(open(args.coinfection))

    if args.freyja:
        output["output"]["freyja_data"] = json.load(open(args.freyja))

    if args.alphafold:
        output["output"]["structural_data"] = json.load(open(args.alphafold))

    if args.pangolin:
        if "viral_classification_data" not in output["output"].keys():
            output["output"]['viral_classification_data'] = []

        output["output"]["viral_classification_data"].extend([element for element in json.load(open(args.pangolin))])

    if args.nextclade:
        # Nextclade data contain extra info if analyzed species is RSV
        if "viral_classification_data" not in output["output"].keys():
            output["output"]['viral_classification_data'] = []

        if args.pathogen != "rsv":
            output["output"]["viral_classification_data"].extend([element for element in json.load(open(args.nextclade))])
        else:
            dane_tmp = json.load(open(args.nextclade))
            output["output"]["rsv_data"] = {"type": dane_tmp[0]['type_name']}
            del dane_tmp[0]['type_name']
            output["output"]["viral_classification_data"].extend(
                [element for element in dane_tmp])

    if args.wgsMetrics:
        output["output"]["viral_genome_data"] = json.load(open(args.wgsMetrics))

    if args.consensus:
        dane = json.load(open(args.consensus))
        # consensus has data from two tabs
        output["output"]["viral_genome_data"]["total_length_value"] = dane.pop("total_length_value")
        output["output"]["viral_genome_data"]["number_of_Ns_value"] = dane.pop("number_of_Ns_value")
        # number_of_ambiguous_value was added later; tolerate older consensus JSONs without it
        if "number_of_ambiguous_value" in dane:
            output["output"]["viral_genome_data"]["number_of_ambiguous_value"] = dane.pop("number_of_ambiguous_value")
        output["output"]["genome_files_data"] = dane

    if args.snpeff:
        output = fill_viral_mutation(file_path=args.snpeff,
                                     output_local=output)
    else:
        output["output"]["viral_mutation_data"] = []

    if (args.reassortment or args.drug_resistance) and args.pathogen == "influenza":
        output = fill_infl_data(output_local=output,
                                resistance=args.drug_resistance,
                                reassortment=args.reassortment)

    if args.drug_resistance and args.pathogen == "rsv":
        resistance = json.load(open(args.drug_resistance))
        if "rsv_data" not in output["output"]:
            output["output"]["rsv_data"] = {}
        output["output"]["rsv_data"] = {**output["output"]["rsv_data"], **resistance}

    if args.mapping:
        output["output"]["viral_mapping_data"] = json.load(open(args.mapping))

    with open("output.json", "w") as f:
        f.write(json.dumps(output, ensure_ascii=False,  indent=4))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', help="Pipeline version")
    parser.add_argument('--pathogen', help="Pathogen")
    parser.add_argument('--sampleId', help="Sample ID")
    parser.add_argument('--fastqc_pre', nargs='+', help='At least one file from fastqc module (pre_filtering)')
    parser.add_argument('--fastqc_post', nargs='+', help='At least a file from fastqc module (post_filtering)')
    parser.add_argument('--contamination', help="JSON from contamination detection (kraken2)")
    parser.add_argument('--freyja', help="Output of various freyja modules")
    parser.add_argument('--coinfection', help="Output for custom coinfection analysis for SARS-CoV-2 organism")
    parser.add_argument('--dehumanized', help="Output of defumanization procedure")
    parser.add_argument('--wgsMetrics', help="WGS metrics file")
    parser.add_argument('--consensus', help="JSON from consensus module")
    parser.add_argument('--pangolin', help="JSON from viral classification module (pangolin)")
    parser.add_argument('--nextclade', help="JSON from viral classification module (nextclade)")
    parser.add_argument('--snpeff', help="Output of snpeff for selected organisms)")
    parser.add_argument('--alphafold', help="Output for alphafold module")
    parser.add_argument('--reassortment', help="Output for reassortment module for influenza")
    parser.add_argument('--drug_resistance', help="Output for drug resistance analysis for influenza")
    parser.add_argument('--mapping', help="Output of bwa/minimap2 module")
    parser.add_argument('--executiondir', help="Execution path of the program")

    args = parser.parse_args()
    args.pathogen = normalize_pathogen(args.pathogen)

    json_aggregator(args)


if __name__ == '__main__':
    main()
