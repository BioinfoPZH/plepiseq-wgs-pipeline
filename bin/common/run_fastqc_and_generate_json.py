#!/usr/bin/env python3
"""
Skrypt do parsowania wynikow programu fastqc i produkowania json-a zgodnego ze specyfikacja w repozytorium plepiseq_json
"""

import json
import re
import subprocess
import sys

import click
import numpy as np


def run_fastqc(plik, memory, cpu):
    polecenie = (f'fastqc --format fastq --threads {cpu} --memory {memory} --extract --outdir . {plik}')
    proces = subprocess.Popen(polecenie, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    proces.wait()
    outs, errs = proces.communicate()
    status_fastqc = re.findall('Analysis complete', str(outs))
    if len(status_fastqc) > 0:
        status = 'tak'
    else:
        status = 'blad'
    return status


@click.command()
@click.option('-i', '--input_file', help='[INPUT] a path to a file in fastq format ',
              type=click.Path(), default="fastqc_data.txt")
@click.option('-m', '--memory', help='[INPUT] Memory available to fastqc program',
              type=int, default=4048)
@click.option('-c', '--cpu', help='[INPUT] CPUs available to fastqc program',
              type=int, default=4)
@click.option('-x', '--min_number', help='[INPUT] QC parameter if sample has less reads, '
                                         'the json will contain blad ',
              type=int)
@click.option('-y', '--min_qual', help='[INPUT] QC parameter if reads have median quality less than'
                                       ' this value the json will contain blad',
              type=int)
@click.option('-s', '--status', help='[INPUT] PREDEFINED status that is transferred to an output json. '
                                     'If this status was either nie or blad fastqc will not run',
              type=click.Choice(['tak', 'nie', 'blad'], case_sensitive=False),  required=True)
@click.option('-e', '--stage', help='[INPUT] Stage on which data is analyzed',
              type=click.Choice(["pre-filtering", "post-filtering"], case_sensitive=False),  required=True)
@click.option('-p', '--publishdir', help='[INPUT] Path with fNEXTFLOW output, required to correctly format json',
              type=str,  required=True)
@click.option('-r', '--error', help='[INPUT] PREDEFINED error message that is put in json. '
                                    'Only used when status was set to nie or blad',
              type=str,  required=False, default="")
@click.option('-o', '--output', help='[Output] Name of a file with json output',
              type=str,  required=False)
@click.option('--lan', type=click.Choice(['en', 'pl'], case_sensitive=False),
              default='en', help='Language for automatic error messages (en/pl)')
def main_program(input_file, memory, cpu, min_number, min_qual, status, stage, publishdir, output, error, lan):
    # Na poczatku obsluzmy sytuacje gdzie predefiniowany status to blad lub nie
    if status == "nie" or status == 'blad':
        # check if predefined QC status is not "tak"
        json_dict = [{"status": status, "file_name": input_file, "step_name": stage, "error_message": error}]
        with open(output, 'w') as f1:
            f1.write(json.dumps(json_dict, ensure_ascii=False, indent=4))
        # We always print the output, otherwise bash cannot "capture" the values reruned by this script
        print(f"{status} 0 ")
        return status, 0
    else:
        # upewnijmy sie ze podany plik istnieje
        try:
            with open(input_file) as dummy:
                pass
        except FileNotFoundError:
            status = 'blad'
            msg = "Provided file does not exist" if lan == "en" else "Podany plik nie istnieje"
            json_dict = [{"status": {status}, "file_name": input_file, "step_name": stage,
                         "error_message": msg}]
            with open(output, 'w') as f1:
                f1.write(json.dumps(json_dict, ensure_ascii=False, indent=4))
            print(f"{status} 0")
            return status, 0
        # to wywoluje polecenie fastqc na pliku, przy okazji obsluzymy wyjatek ze podany plik nie jest
        # fastq wedlug fastqc
        status = run_fastqc(input_file, memory, cpu)
        if status == 'blad':
            msg = "Provided file is not in fastq format" if lan == "en" else "Podany plik nie jest w formacie fastq"
            json_dict = [{"status": "blad", "file_name": input_file, "step_name": stage,
                          "error_message": msg}]
            with open(output, 'w') as f1:
                f1.write(json.dumps(json_dict, ensure_ascii=False, indent=4))
            print(f"{status} 0")
            return status, 0
        # mamy plik z wynikami i fastqc go przeprocesowal
        input_dir = input_file.replace('.fastq.gz', '_fastqc')
        number_of_reads_value = 0  # pole number_of_reads_value
        number_of_bases_value = 0  # pole number_of_bases_value
        reads_median_length_value = 0  # pole reads_median_length_value
        reads_min_length_value = 0  # reads_min_length_value
        reads_max_length_value = 0  # pole reads_max_length_value
        reads_median_quality_value = 0  # pole reads_median_quality_value
        reads_quality_histogram_path = f'{input_dir}_reads_quality_histogram.csv'  # pole reads_quality_histogram_path
        reads_length_histogram_path = f'{input_dir}_reads_length_histogram.csv'  # pole reads_length_histogram_path
        position_quality_plot_path = f'{input_dir}_position_quality_plot.csv'
        gc_content_value = 0  # OPCJONALNE pole gc_content_value

        reads_median_quality_data = []  # pomocnicza lista do obliczenia mediany
        reads_median_length_data = []  # pomocnicza lista do obliczenia mediany
        start = 0
        section_name = ""
        # otwieramy polaczenia do plikow z wymaganymi nazwami
        # tworzymy tez naglowki zgodnie z dokumentacja excelu
        qualty_histogram_file = open(reads_quality_histogram_path, 'w')
        qualty_histogram_file.write(f'#indeks;jakosc_wartosc;liczebnosc\n')

        length_histogram_file = open(reads_length_histogram_path, 'w')
        length_histogram_file.write(f'#indeks;dlugosc_wartosc;liczebnosc\n')

        position_quality_file = open(position_quality_plot_path, "w")
        position_quality_file.write(f'#indeks;pozycja_w_odczycie;mediana_jakosc\n')
        with open(f'{input_dir}/fastqc_data.txt') as f:
            for line in f:
                line = line.split('\t')
                if ">>" in line[0] and start == 0:
                    # Sekcje z rownymi typami wynikow w pliku rozpoczynaja sie od ">>"
                    # a koncza na ">>END_MODULE"
                    start = 1
                    section_name = line[0].replace(">>", "")
                elif ">>" in line[0] and start == 1:
                    start = 0
                    if section_name == "Per sequence quality scores":
                        reads_median_quality_value = np.median(reads_median_quality_data)
                        if round(float(reads_median_quality_value), 2) < float(min_qual):
                            status = 'blad'
                            msg = f"Reads quality is below minimum required level i.e. {reads_median_quality_value}" if lan == "en" else f"Jakość odczytów jest poniżej wymaganego minimum {reads_median_quality_value}"
                            json_dict = [{"status": "blad", "file_name": input_file, "step_name": stage,
                                          "error_message": msg}]
                            with open(output, 'w') as f1:
                                f1.write(json.dumps(json_dict, ensure_ascii=False, indent=4))
                            print(f"{status} 0")
                            return status, 0
                    # koniec sekcji mozna zrzucac wyniki
                    if section_name == "Sequence Length Distribution":
                        reads_median_length_value = np.median(reads_median_length_data)

                elif start == 1:
                    # jestem s sekcji
                    if section_name == "Basic Statistics":
                        if line[0] == "Filename":
                            nazwa_pliku = line[1]
                        if line[0] == "Total Sequences":
                            number_of_reads_value = line[1]
                            if float(number_of_reads_value) < float(min_number):
                                status = 'blad'
                                msg = f"The file has fewer reads than required {min_number}" if lan == "en" else f"Plik zawiera mniej odczytów niż wymagane minimum {min_number} odczytow"
                                json_dict = [{"status": "blad", "file_name": input_file, "step_name": stage,
                                              "error_message": msg}]
                                with open(output, 'w') as f1:
                                    f1.write(json.dumps(json_dict, ensure_ascii=False,  indent=4))
                                print(f"{status} 0")
                                return status, 0
                        if line[0] == "Sequence length":
                            try:
                                reads_min_length_value, reads_max_length_value = line[1].split("-")
                            except ValueError:
                                # in case all the sequences have identical lengts fastqc does report only one value not "min - max"
                                reads_min_length_value = line[1].rstrip()
                                reads_max_length_value = line[1].rstrip()
                        if line[0] == "Total Bases":
                            number_of_bases_value = float(line[1].split(" ")[0])
                            unit = line[1].split(" ")[1].rstrip()  # fastqc dodaje okreslecia jak Kbp, Mbp i pewnie Gbp
                            if unit == "Kbp":
                                number_of_bases_value = number_of_bases_value * 1000
                            elif unit == "Mbp":
                                number_of_bases_value = number_of_bases_value * 1000000
                            elif unit == "Gbp":
                                number_of_bases_value = number_of_bases_value * 1000000000
                        if line[0] == "%GC":
                            gc_content_value = line[1]

                    if section_name == "Per sequence quality scores":
                        if "#" in line[0]:
                            indeks = 0
                        else:
                            qualty_histogram_file.write(f'{indeks};{line[0]};{line[1]}')
                            reads_median_quality_data.extend([int(line[0])] * int(float(line[1].rstrip())))
                            indeks += 1
                    if section_name == "Sequence Length Distribution":
                        if "#" in line[0]:
                            indeks = 0
                        else:
                            # fastqc przekazuje dane w postaci od-do np 30-35,
                            low, high = line[0].split("-")
                            mid = ( int(high) - int(low) ) / 2
                            length_histogram_file.write(f'{indeks};{mid};{line[1]}')
                            reads_median_length_data.extend([int(mid)] * int(float(line[1].rstrip())))
                            indeks += 1
                    if section_name == "Per base sequence quality":
                        if "#" in line[0]:
                            indeks = 0
                        else:
                            if len(line[0].split("-")) == 2:
                                pozycja = line[0].split("-")[1]
                            elif len(line[0].split("-")) == 1:
                                pozycja = line[0].split("-")[0]
                            else:
                                status = 'blad'
                                msg = "Error when parsing fastqc file" if lan == "en" else "Błąd podczas parsowania pliku fastqc"
                                json_dict = [{"status": "blad", "file_name": input_file, "step_name": stage,
                                             "error_message": msg}]
                                with open(output, 'w') as f1:
                                    f1.write(json.dumps(json_dict, ensure_ascii=False, indent=4))
                                print(f"{status} 0")
                                return status, 0

                            position_quality_file.write(f"{indeks};{pozycja};{line[2]}\n")
    json_dict = [{"status": "tak",
                  "file_name": nazwa_pliku.rstrip(),
                  "step_name": stage,
                  "number_of_reads_value": int(number_of_reads_value),
                  "number_of_bases_value": int(number_of_bases_value),
                  "reads_median_length_value": round(float(reads_median_length_value), 2),
                  "reads_min_length_value": int(reads_min_length_value),
                  "reads_max_length_value": int(reads_max_length_value),
                  "reads_median_quality_value": round(float(reads_median_quality_value), 2),
                  "reads_quality_histogram_file": f"{publishdir}/{reads_quality_histogram_path}",
                  "reads_length_histogram_file": f"{publishdir}/{reads_length_histogram_path}",
                  "position_quality_plot_file": f"{publishdir}/{position_quality_plot_path}",
                  "gc_content_value": round(float(gc_content_value), 2)}]
    with open(output, 'w') as f1:
        f1.write(json.dumps(json_dict, ensure_ascii=False, indent=4))
    print(f"{status}  {number_of_bases_value}")
    return status, int(number_of_bases_value)


if __name__ == '__main__':
    # The main program returns 3 variables: "status", number_of_reads in a sample, and median_quality_of_reads
    # These can be used to determine QC status in tha module
    if len(sys.argv) == 1:
        main_program(['--help'])
    else:
        return_status, number_of_bases = main_program(sys.argv[1:])
