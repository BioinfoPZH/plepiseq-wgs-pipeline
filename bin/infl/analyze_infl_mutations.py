#!/usr/bin/env python3

"""
Compared to the version with Nanopore, we have additional variables for
file locations (Output_dir) and data (data_dir).
"""

import os
import re
import shutil
import subprocess
import sys
from tempfile import NamedTemporaryFile
from typing import Dict
import click
import json

import numpy as np
from Bio import SeqIO

# Obsole functions used in old verion of this script


def _determine_subtype(subtype):
    """
    Funka uzywan w nieuzywanej funkcji _run_infinity
    :param subtype:
    :return:
    """

    # ta sekwencja lepeij zeby zawierala tylko HA

    # zwracamy nazwy tak by pasowaly do sciezek z /opt/docker/nextclade/data/flu (konetner illumina do sars-a ! poki co'

    if 'H1N1' in subtype and 'pdm09' in subtype:
        return 'h1n1pdm'
    elif 'H1N1' in subtype and 'pdm09' not in subtype:
        return 'h1n1'
    elif 'H3N2' in subtype:
        return 'h3n2'
    elif 'B/Yam' in subtype:
        return 'yam'
    elif 'B/Vic' in subtype:
        return 'vic'
    elif 'unknown' in subtype:
        raise Exception(f'unknown subtype')
    else:
        raise Exception(f'Unknown case {subtype}')


def _run_infinity(file, input_fasta):
    """
    Funkcja do puszczania infinity programu okreslajacego clad do ktorego nalezy podana sekwencja HA.
    Aktualnie obsolete zastapione tylko nextclade, choc w przyszlosci moze nada sie do avian ?
    :param file:
    :param input_fasta:
    :return:
    """
    subtype = ""
    if 'run_infinity.R' not in os.listdir('.'):
        raise Exception('Pamietaj o umieszczeni uskryptu run_infinity.R w katalogu gdzie wywolujesz skrypt!')
    komenda = f'Rscript run_infinity.R {file.name}'
    infinity = subprocess.Popen(komenda, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    for line in infinity.stderr:
        line = line.decode('UTF-8').rstrip()
        if 'Error' in line:
            raise Exception(f"Infinity could not process provided sequence from {input_fasta}")

    for line in infinity.stdout:
        line = line.decode('UTF-8').rstrip()
        if 'Clade' in line:
            continue
        print(line)
        subtype = _determine_subtype(line.split(';')[2])
    os.remove(file.name)
    return subtype


def _get_protein_sequence(segment_fasta, referene_fasta, gff_file, what, out_dir, qc_threshold, typ, sample_name):

    # Obsolete function do przewidywania podtypu probki. Uzywana byla w EQA2023 gdzie inputem do zadania
    # byla po prostu fasta bez informacji o zrodle pochodzenia

    # Stare komentarze
    # referencyjna sekwencja
    # uwaga w /opd.docker/nextclade/data/flu itd .. w genmap musialem recznie poprawic
    # opis z gene_mamae "ALA" na gene_name="ALA" !!!!
    # zrobilem to tylko dla HA, NA i PA bo te geny sa wazne dla opornosci

    if f'Reference_{what}_{typ}.fasta' not in os.listdir('.'):
        komenda = f'nextalign run -r {referene_fasta} -m {gff_file} -g {what} -O {out_dir} -n {what} {referene_fasta}'
        p = subprocess.Popen(komenda, shell=True, stdout=subprocess.PIPE)
        p.communicate()
        shutil.copyfile(f'./{out_dir}/{what}_gene_{what}.translation.fasta', f'Reference_{what}_{typ}.fasta')
        shutil.rmtree(f'{out_dir}')

    # teraz sekwencja z probki
    with open(f'QC_report_{what}.txt', 'w') as f:
        komenda = f'nextalign run -r {referene_fasta} -m {gff_file} -g {what} -O {out_dir} -n {what} {segment_fasta}'
        p = subprocess.Popen(komenda, shell=True, stdout=subprocess.PIPE)
        p.communicate()
        os.remove(f'{segment_fasta}')
        tekst, stosunek = check_quality_protein_sequence(f'{out_dir}/{what}_gene_{what}.translation.fasta')
        f.write(tekst)

        if stosunek < qc_threshold:
            shutil.copyfile(f'{out_dir}/{what}_gene_{what}.translation.fasta', f'{sample_name}_{what}.fasta')
        shutil.rmtree(f'{out_dir}')

    return True


def check_quality_protein_sequence(plik, lan):
    """
    Check presence of 'X' and 'N' in protein sequence. If the combined proportion
    of these characters is above a threshold, the sequence is flagged as low quality.
    The sequence is aligned to reference and may include '-' characters which are excluded.

    :param plik: FASTA file path
    :param lan: Language code ('en' or 'pl') for message
    :return: (message_text, fraction_of_X_and_N)
    """
    tekst = "Podany plik jest pusty" if lan == 'pl' else "Provided file does not contain any valid sequence"
    stosunek = 1.0 # if no file is provided return this calue

    record = SeqIO.parse(plik, 'fasta')
    for r in record:
        ilosc = int(r.seq.count('X')) + int(r.seq.count('N'))
        dlugosc = float(len(r.seq.replace('-', '')))
        stosunek = float("%.4f" % (ilosc / dlugosc))

        if lan == 'pl':
            tekst = (f'Sekwencja {r.id} ma długość:\t{dlugosc}\tIlosc X i N:\t{ilosc}\t'
                     f'Procentowa zawartosc X i N:\t{stosunek * 100}%\n')
        else:
            tekst = (f'Sequence {r.id} has length:\t{dlugosc}\tCount of X and N:\t{ilosc}\t'
                     f'Percentage of X and N:\t{stosunek * 100}%\n')
        return tekst, stosunek

    return tekst, stosunek


def get_fasta_header(input_fasta):
    record = SeqIO.parse(input_fasta, 'fasta')
    for r in record:
        return f'>{r.description}'


def get_fasta(input_fasta, sample, what, lan):
    """
    :param input_fasta: Fasta file with multiple sequences
    :param sample: Sample name used only to name temporary files
    :param what: Segment name to extract; must appear in FASTA header
    :param lan: Language for error messages ('en' or 'pl')
    :return: Tuple of temporary file handle and error message (empty string if no error)
    """
    record = SeqIO.parse(input_fasta, 'fasta')
    error = ""
    two_HA = 0

    for r in record:
        if what in r.id:
            if two_HA > 0:
                # More than one matching segment found
                with NamedTemporaryFile(dir='.', prefix=f'{sample}_', suffix=f'_{what}.fasta', mode='w', delete=False) as file:
                    file.write(f'>{what}\n')
                    file.write(f'XXX\n')
                if lan == 'pl':
                    error += f'Plik {input_fasta} zawiera wiecej niż jedną sekwencje {what}. '
                else:
                    error += f'File {input_fasta} contains more than one sequence named {what}. '
            with NamedTemporaryFile(dir='.', prefix=f'{sample}_', suffix=f'_{what}.fasta', mode='w', delete=False) as file:
                file.write(f'>{r.id}\n')
                file.write(f'{str(r.seq)}\n')
            two_HA += 1

    # If no sequence matched, write dummy file
    if two_HA == 0:
        with NamedTemporaryFile(dir='.', prefix=f'{sample}_', suffix=f'_{what}.fasta', mode='w', delete=False) as file:
            file.write(f'>{what}\n')
            file.write(f'XXX\n')
        if lan == 'pl':
            error += f'Plik {input_fasta} nie zawiera sekwencji {what}.'
        else:
            error += f'File {input_fasta} contains no sequence named {what}.'

    return file, error



def align_fasta(fasta1_file: str, *args, **kwargs) -> Dict:
    """
    funkcja do alignowania sekwencji.  Sekwencje albo sa w oddzielnych plikach i wtedy laczymy je w jednym
    tmp file (usuwanym po wykonaniu funkcji), albo w jednym pliku. Do alignowania uzywamy maffta w strategii
    auto
    :param fasta1_file: str, sciezka do pliku fasta zawierajacego jedna lub wiecej sekwencji
    :param args: tupple, sciezka do dowolnej liczy plikow fasta
    :return: dict, slownik ktora jako klucze zawiera identyfikatory sekwencji z podanych pliku/plikow a jako klucze
    alignowano sekwencje
    """

    # Najpierw normalizujemy sciezki  isprawdzamy czy podane plik/pliki istnieja
    fasta1_file = [os.path.abspath(fasta1_file)]
    stan1 = os.path.isfile(fasta1_file[0])
    if not stan1:
        raise Exception('Nie podano poprawnej sciezki do pliku!')
    if len(args) > 0:
        fasta1_file.extend([os.path.abspath(x) for x in args])
        stan2 = np.all([os.path.isfile(plik) for plik in fasta1_file])
        if not stan2:
            raise Exception('Podane dodatkowe pliki nie istnieja')

    for plik in fasta1_file:
        with open('tmp.fasta', 'a+') as f, open(plik, 'r') as f1:
            for line in f1:
                f.write(line)

    polecenie = 'mafft --auto --quiet --inputorder tmp.fasta'
    if 'fast' in kwargs.keys():
        polecenie = 'mafft --retree 1 --quiet --inputorder tmp.fasta'  # komenda z
    alignment = subprocess.Popen(polecenie, shell=True, stdout=subprocess.PIPE)

    # generowanie slownika
    slownik_alignmentu = {}
    for line in alignment.stdout:
        line = line.decode('UTF-8').rstrip()
        if len(re.findall(">", line)) > 0:
            klucz = line[:]
            if klucz not in slownik_alignmentu.keys():
                slownik_alignmentu[klucz] = ''
        else:
            slownik_alignmentu[klucz] = slownik_alignmentu[klucz] + line
    # pozbywamy sie tepowego pliku
    os.remove('tmp.fasta')

    return slownik_alignmentu


# 5 mutacje z odpowiedniej tabelki

def determine_muation(ref_seq, target_seq):
    """
    Dwa stringi zawierajace A-Z i "-" lub "X" (ale tylko w sekwencji targetu), mamy nastepujace stany,
    1. brak mutacji
    2. mutacja roznica charakteru miedzy referencja a targetem
    3. rozpoczecie wstawki
    4. kontynuacja wstawki
    5. rozpoczecie insercji
    6. kontynuacja insercji
    raportujemy jako
    R4L punktowa
    R4_del2 - delecja 2 aminokwasow w target po R4
    R4_ins4 - insercja 4 aminokwasow w target po R4
    :param ref_seq: string, sekwecja referencji po alignment
    :param target_seq: string, sekwencja target po alignment
    :return: lista. Zaweira liste mutacji zodentyfikowane w probce stosujac numeracje aminokwasowa sekwencji ref


    """

    numeracje_ref = 1  # od 1
    previous_ref = 'X'
    previous_target = "X"
    poczatek_delecji = 0
    dlugosc_delecji = 0
    poczatek_insercji = 0
    dlugosc_insercji = 0
    lista_mutacji = [] # Lista mutacji zgodnie z numeracja sekwencji REFERENYJNEJ

    for i in range(len(ref_seq)):

        # sprawdz czy nie skonczyles insercji
        if dlugosc_insercji > 0 and ref_seq[i] != '-':
            lista_mutacji.append(f'{ref_seq[poczatek_insercji - 1]}{poczatek_insercji}ins{dlugosc_insercji}')
            dlugosc_insercji = 0
            poczatek_insercji = 0
        # sprawdz czy nie skonczyles delecji

        if dlugosc_delecji > 0 and target_seq[i] != '-':
            # lista_mutacji.append(f'{ref_seq[poczatek_delecji - 1]}{poczatek_delecji}del{dlugosc_delecji}')
            lista_mutacji.append(f'del_{poczatek_delecji}_{poczatek_delecji + dlugosc_delecji}')
            dlugosc_delecji = 0
            poczatek_delecji = 0

        if ref_seq[i] == target_seq[i]:
            pass
        elif ref_seq[i] != target_seq[i] and ref_seq[i] != '-' and target_seq[i] != '-':
            # prawdiwa mutacja
            lista_mutacji.append(f'{ref_seq[i]}{numeracje_ref}{target_seq[i]}')
        elif ref_seq[i] != target_seq[i] and ref_seq[i] == '-' and previous_ref != '-':
            # zaczynamy insercje
            poczatek_insercji = numeracje_ref
            dlugosc_insercji = 1
        elif ref_seq[i] != target_seq[i] and ref_seq[i] == '-' and previous_ref == '-':
            dlugosc_insercji += 1
        elif ref_seq[i] != target_seq[i] and target_seq[i] == '-' and previous_target != '-':
            # poczatek delecji
            poczatek_delecji = numeracje_ref
            dlugosc_delecji = 1
        elif ref_seq[i] != target_seq[i] and target_seq[i] == '-' and previous_target == '-':
            dlugosc_delecji += 1
        else:
            print(f'Unknown case na pozycji alignmentu {i} ref {ref_seq[(i - 3):(i + 1)]}target {target_seq[(i - 3):(i + 1)]} ')

        if ref_seq[i] != '-':
            numeracje_ref += 1

        previous_ref = ref_seq[i]
        previous_target = target_seq[i]

    # Na koniec zrzuc otwarta delecje lub insercje
    if dlugosc_insercji > 0 and ref_seq[i] != '-':
        lista_mutacji.append(f'{ref_seq[poczatek_insercji - 1]}{poczatek_insercji}ins{dlugosc_insercji}')

    if dlugosc_delecji > 0 and target_seq[i] != '-':
        lista_mutacji.append(f'{ref_seq[poczatek_delecji - 1]}{poczatek_delecji}ins{dlugosc_delecji}')

    return lista_mutacji


def higest_rest(tekst):
    if 'HRI' in tekst:
        return 'HRI'
    elif 'RI' in tekst:
        return 'RI'
    elif 'NI' in tekst:
        return 'NI'
    elif '?' in tekst:
        # nie badano, zakladamy normal inhibition
        return 'NI'
    else:
        return 'Err'

def parse_resistance_NA(plik):
    """
    Parser do pliku z opornosci jest 5 kolumn pierwsza to mutacja (badz kombinacja mutacji oddzielonych +, lub delecja_od_do)
    kolejne 4 kolumy to tekstowy opis opornosci kolejnych 4 lekow:
    Oseltamivir, Zanamivir, Peramivir, Laninamivir
    Na razie wystarczy wiedziec czy jest tam NI, RI, HRI
    :param plik:
    :return:
    """
    slownik_opornosci_oseltamivir = {}
    slownik_opornosci_zanamivir = {}
    slownik_opornosci_peramivir = {}
    slownik_opornosci_laninamivir = {}
    with open(plik) as f:
        for line in f:
            if 'Mutation' in line:
                continue
            try:
                klucz, oselt, zana, pera, lana = line.split('\t')
                slownik_opornosci_oseltamivir[klucz] = higest_rest(oselt)
                slownik_opornosci_zanamivir[klucz] = higest_rest(zana)
                slownik_opornosci_peramivir[klucz] = higest_rest(pera)
                slownik_opornosci_laninamivir[klucz] = higest_rest(lana)
            except:
                print(f'Error for {line}')
    return slownik_opornosci_oseltamivir, slownik_opornosci_zanamivir, slownik_opornosci_peramivir, slownik_opornosci_laninamivir


def parse_resistance_PA(plik):
    """
    Parser do pliku z opornosci, sa 2 kolumny pierwsza to mutacja (tylko punktowe) opornosc na baloxavir jest kolumnie 2
    Na razie wystarczy wiedziec czy jest tam NI, RI, HRI. W odroznieniu od NA tu NIE ma naglowka
    :param plik:
    :return:
    """
    slownik_opornosci = {}

    with open(plik) as f:
        for line in f:
            if 'Mutation' in line:
                continue
            try:
                klucz, balo = line.split('\t')
                slownik_opornosci[klucz] = higest_rest(balo)
            except:
                print(f'Error for {line}')

    return slownik_opornosci


def get_sample_status(lista_mutacji, slownik_opornosci):
    mutacje_opornosciowe = {}
    for klucz in slownik_opornosci:
        status = slownik_opornosci[klucz]
        if status in ['RI', 'HRI']:
            if set(klucz.split('+')).issubset(set(lista_mutacji)):
                # W przypadku mutacji "skomplikowanych" jak E99G+H255Y patrzymy czy obie skladowe mutacje
                # wystepuja wsrod mutacji waznych
                mutacje_opornosciowe[klucz] = status
        else:
            # mutacje nieistotne dla danego leku, nie raportuje raczej bo po co
            continue

    status = 'S'
    # teraz okreslamy jaka jest najmocniejsza mutacja i zwracamy stringa "S" dla NI, I dla RI i R dla HRI i 'U' jesli nie ma opornosci
    for klucz in mutacje_opornosciowe:
        if mutacje_opornosciowe[klucz] == 'RI' and status in ['S']:
            status = 'I'
        elif mutacje_opornosciowe[klucz] == 'HRI' and status in ['S', 'I']:
            status = 'R'

    return mutacje_opornosciowe, status

def prepare_json_for_drug(drug_name, drug_status, mutation_slownik_mutacji_ref,
                          ref_target_lookup_local, ref_N2_lookup_local ={}):
    """
    Funkcja generuje json dla analizowanego leku zgodnie z ustalonym schematem jsonowym
    @param drug_name: nazwa leku
    @type drug_name: str
    @param drug_status: status opornosci na dany lek
    @type drug_status: str
    @param mutation_slownik_mutacji_ref: slownik mutacji powodujacych opornosn na dany lek wraz z ich sila
    @type mutation_slownik_mutacji_ref: dict
    @param ref_N2_lookup_local: slownik mapujacy wszystkie mozliwe mutacje powodujace opornosc na lek na numeracje w N2
    @type ref_N2_lookup_local: dict
    @param ref_target_lookup_local: slownik mapujacy mutacje w probce miedzy numeracje referencji a sample'a
    @type ref_target_lookup_local:
    @return: slownik zgodny ze schematem jsonowym
    @rtype: dict
    """
    json_tmp = {}  # to bedzie obiekt do json-a
    json_tmp["drug_name"] = drug_name
    json_tmp["drug_resistance_status"] = drug_status
    json_tmp['mutation_list_data_reference_numbering'] = []



    for mutacja_ref, status_mutacji in mutation_slownik_mutacji_ref.items():
        json_tmp['mutation_list_data_reference_numbering'].append({"mutation_name": mutacja_ref,
                                                                   "mutation_effect": status_mutacji})

    if len(list(ref_N2_lookup_local.keys())) > 0:
        # N2 nie analizujemy cla baloxaviru
        json_tmp['mutation_list_data_N2_numbering'] = []
        for mutacja_ref, status_mutacji in mutation_slownik_mutacji_ref.items():

            N2_numbering = ref_N2_lookup_local[mutacja_ref]

            if "+" in mutacja_ref:
                mutacja_N2_numbering = ""
                mutacja_ref_components = mutacja_ref.split('+')  # rozbijamy skomplikowana mutacje na skladowe
                lista_N2_numbers = N2_numbering.split("+")  # to samo robimy z odpowiadajaca numeracja N2

                for tmp, new_N2 in zip(mutacja_ref_components, lista_N2_numbers):
                    if mutacja_N2_numbering != "":
                        mutacja_N2_numbering += "+"
                    ref_seq, _, target_seq = re.findall('(^\\D+)(\\d+)(\\D+)', tmp)[0]
                    mutacja_N2_numbering += f'{ref_seq}{new_N2}{target_seq}'
            else:
                ref_seq, _, target_seq = re.findall('(^\\D+)(\\d+)(\\D+)', mutacja_ref)[0]
                mutacja_N2_numbering = f'{ref_seq}{N2_numbering}{target_seq}'

            json_tmp['mutation_list_data_N2_numbering'].append({"mutation_name": mutacja_N2_numbering,
                                                                "mutation_effect": status_mutacji})

    json_tmp['mutation_list_data'] = []
    for mutacja_ref, status_mutacji in mutation_slownik_mutacji_ref.items():
        if "+" in mutacja_ref:
            mutacja_taget_numbering = ""
            mutacja_ref_components = mutacja_ref.split('+')  # rozbijamy skomplikowana mutacje na skladowe
            for tmp in mutacja_ref_components:
                if mutacja_taget_numbering != "":
                    mutacja_taget_numbering += "+"
                mutacja_taget_numbering += ref_target_lookup_local[tmp]

        else:
            mutacja_taget_numbering = ref_target_lookup_local[mutacja_ref]

        json_tmp['mutation_list_data'].append({"mutation_name": mutacja_taget_numbering,
                                               "mutation_effect": status_mutacji})

    return json_tmp

@click.command()
@click.option('-i', '--input_fasta', help='[INPUT] a fasta file of NA and PA segements',
              type=click.Path(), required=False)
@click.option('-t', '--subtype', help='[INPUT] a subtype identified for this sample',
              type=str, required=False)
@click.option('-n', '--sample_name', help='[INPUT] sample identifier',
              type=str, required=False)
@click.option('-p', '--data_path', help='[INPUT] a path to directory with resistance data',
              type=click.Path(), required=False)
@click.option('-q', '--output_path', help='[OUTPUT] a path where or the results of resistance analysis '
                                          'are saved', type=click.Path(), required=False)
@click.option('-s', '--status', help='[INPUT] Status filed for json output',
              type=click.Choice(['tak', 'nie', 'blad'], case_sensitive=False),  required=True)
@click.option('-r', '--error', help='[INPUT] PREDEFINED error message that is put in json. '
                                    'Only used when status was set to nie or blad',
              type=str,  required=False, default="")
@click.option('-o', '--output_json', help='[Output] Name of a file with json output',
              type=str,  required=True)
@click.option('--lan', help='Language for error messages (en/pl)',
              type=click.Choice(['pl', 'en']), default='en', show_default=True)
def main_program(status, output_json, input_fasta, subtype, sample_name, data_path, output_path, lan, error=""):
    if status != "tak":
        json_output = {"resistance_status": status,
                       "resistance_error_message": error}
        with open(output_json, 'w') as f1:
            f1.write(json.dumps(json_output,  ensure_ascii=False,  indent=4))
        return True
    else:

        plik_fasta = input_fasta  # plik z sekwencjami bialek, moze byc multifasta
        podtyp = subtype  # podtyp analizowanego wirusa (obslugujem H1N1, H3N2, H5N1, H7N9, Yamagata, Victoria)
        sample_name = sample_name  # nazwa sample uzywana bedzie w podplikach tworzonych wprzez skrypt
        data_dir = data_path  # sciezka gdzie trzymane sa dane w kontenerze
        output_dir = output_path  # sciezka w ktorym katalogu zapisywac wyniki

        error_msg_NA = ""  # Error dla analizy opornosci zwiazanych z egmentem NA
                          # ze wzgledu na charakter analizy bedzie rosl wraz z pojawiajacymi sie bledami
        error_msg_PA = ""  # j.w. dla segmentu PA. JESLI oba stringi beda nie-puste status analizy zmieni sie na blad.

        # Definicja zmiennych
        lista_mutacji_NA_opornosc_oselt = ['UNK']
        lista_mutacji_NA_opornosc_zana = ['UNK']
        lista_mutacji_NA_opornosc_pera = ['UNK']
        lista_mutacji_NA_opornosc_lani = ['UNK']
        lista_mutacji_PA_opornosc_balo = ['UNK']

        status_oselt = 'UNK'
        status_zana = 'UNK'
        status_pera = 'UNK'
        status_lani = 'UNK'
        status_balo = 'UNK'

        # plik* to obiekty clasy templfile ich nazwa jest w obiek.name, trzymaja genomiczna sekwencje danego segmentu
        plik_referencja_NA = f"/{data_dir}/resistance/{podtyp}/{podtyp}_NA_reference.fasta"
        opornosci_NA = f"{data_dir}/resistance/{podtyp}/{podtyp}_NA_resistance.txt"
        N2_mapping_table = f"{data_dir}/resistance/{podtyp}/N2_numbering.txt"

        ref_N2_lookup = {}
        with open(N2_mapping_table) as f:
            for line in f:
                line = line.split()
                line[-1] = line[-1].rstrip()
                ref_N2_lookup[line[0]] = line[1]

        plik_fasta_NA, error_NA_file = get_fasta(input_fasta=plik_fasta,
                                                 what='NA',
                                                 sample=sample_name,
                                                 lan=lan)

        # error_NA_file is already in the specified language
        if error_NA_file != "":
            error_msg_NA += error_NA_file
            # Na tym eapie nie ma sekwencji bialka NA w input
        else:
            # mam sekwencje bialka NA sprawdzam jego jakosc
            tekst_NA_protein, stosunek_NA_protein = check_quality_protein_sequence(plik=plik_fasta_NA.name,
                                                                                   lan=lan)
            if stosunek_NA_protein > 0.2:
                error_msg_NA += tekst_NA_protein
            else:
                # Na tym etapie mam sekwncje bialka NA o dobrej jakosc moge przystapic do analizy
                NA_alignment = align_fasta(plik_fasta_NA.name, plik_referencja_NA)
                NA_ref_header = get_fasta_header(plik_referencja_NA)
                NA_target_header = get_fasta_header(plik_fasta_NA.name)
                if NA_ref_header == NA_target_header:
                    raise Exception(f'Sekwencje NA maja taki sam header, wychodze')
                # To pisze w miejscu wywolania i po pierwsze tworzy liste mutacji
                # w postaci {aminokwas_referencyjny}_{numer w referncji}_{aminokwas w target}
                # delecje i insercje opisane sa w postaci f'del_{poczatek_delecji}_{poczatek_delecji+dlugosc_delecji}'
                # i f'{ref_seq[poczatek_insercji - 1]}{poczatek_insercji}ins{dlugosc_insercji}'


                # Mutacje lda json dla pola mutation_list_data_reference_numbering

                lista_mutacji_reference_NA = determine_muation(ref_seq=NA_alignment[NA_ref_header],
                                                               target_seq=NA_alignment[NA_target_header])

                # "Odwracamy" co jest ref a co targetem, dostajemy liste mutacji dla json dla pla
                # mutation_list_data

                lista_mutacji_targetu_NA = determine_muation(target_seq=NA_alignment[NA_ref_header],
                                                             ref_seq=NA_alignment[NA_target_header])

                # W tym slowniku trzymamy odpowiadajace sobie mutacje z 2 roznych systemow numeracji
                ref_target_lookup = {}
                for ref_mutation, target_mutation in zip(lista_mutacji_reference_NA, lista_mutacji_targetu_NA):
                    ref_target_lookup[ref_mutation] = target_mutation

                with open(f'{output_dir}/Mutation_report_NA.txt', 'w') as f:
                    f.write(f'{sample_name}\t{";".join(lista_mutacji_reference_NA)}\n')
                    f.write(f'{sample_name}_reverse\t{";".join(lista_mutacji_targetu_NA)}\n')

                # slowniki opornosci
                slownik_opornosci_oseltamivir_NA, \
                    slownik_opornosci_zanamivir_NA, \
                    slownik_opornosci_peramivir_NA, \
                    slownik_opornosci_laninamivir_NA = parse_resistance_NA(opornosci_NA)

                # wyniki

                slownik_mutacji_NA_opornosc_oselt, status_oselt = get_sample_status(lista_mutacji_reference_NA,
                                                                                  slownik_opornosci_oseltamivir_NA)

                slownik_mutacji_NA_opornosc_zana, status_zana = get_sample_status(lista_mutacji_reference_NA,
                                                                                slownik_opornosci_zanamivir_NA)

                slownik_mutacji_NA_opornosc_pera, status_pera = get_sample_status(lista_mutacji_reference_NA,
                                                                                slownik_opornosci_peramivir_NA)

                slownik_mutacji_NA_opornosc_lani, status_lani = get_sample_status(lista_mutacji_reference_NA,
                                                                                slownik_opornosci_laninamivir_NA)

                # w liscie lista_mutacji_NA_opornosc_* mamy de facto mutacje zgodnie z numeracja referencji
                # ale z dokumentu WHO (czyli np. mielismy dwie mutacje H275Y i I436N, ktore w dokumencie WHO
                # wystepuja jako H275Y+I436N musimy wiec
                # A.zachowac ta wartosc
                # B dodac ta samo wartosc ale w numeracji naszej probki "Y275H+N430N". To mamy w ref_target_lookup
                # C. dodac jak numerowane bylyby te zmiany wzgledem sekwencji N2. To mamy w ref_N2_lookup

                oseltamivir_json = prepare_json_for_drug(drug_name="Oseltamivir",
                                                         drug_status=status_oselt,
                                                         mutation_slownik_mutacji_ref=slownik_mutacji_NA_opornosc_oselt,
                                                         ref_N2_lookup_local=ref_N2_lookup,
                                                         ref_target_lookup_local=ref_target_lookup)

                zanamivir_json = prepare_json_for_drug(drug_name="Zanamivir",
                                                       drug_status=status_zana,
                                                       mutation_slownik_mutacji_ref=slownik_mutacji_NA_opornosc_zana,
                                                       ref_N2_lookup_local=ref_N2_lookup,
                                                       ref_target_lookup_local=ref_target_lookup)

                peramivir_json = prepare_json_for_drug(drug_name="Peramivir",
                                                       drug_status=status_pera,
                                                       mutation_slownik_mutacji_ref=slownik_mutacji_NA_opornosc_pera,
                                                       ref_N2_lookup_local=ref_N2_lookup,
                                                       ref_target_lookup_local=ref_target_lookup)

                laninamivir_json = prepare_json_for_drug(drug_name="Laninamivir",
                                                         drug_status=status_lani,
                                                         mutation_slownik_mutacji_ref=slownik_mutacji_NA_opornosc_lani,
                                                         ref_N2_lookup_local=ref_N2_lookup,
                                                         ref_target_lookup_local=ref_target_lookup)

        baloxavir_json = {}
        if podtyp in ['H1N1', 'H3N2', 'Yamagata', 'Victoria']:
            # opornosci na PA sa znane tylko dala tych podtypow

            plik_referencja_PA = f"/{data_dir}/resistance/{podtyp}/{podtyp}_PA_reference.fasta"
            opornosci_PA = f"/{data_dir}/resistance/{podtyp}/{podtyp}_PA_resistance.txt"

            plik_fasta_PA, error_PA_file = get_fasta(input_fasta=plik_fasta,
                                                     what='PA',
                                                     sample=sample_name,
                                                     lan=lan)
            if error_PA_file != "":
                error_msg_PA += error_PA_file
                # Na tym eapie nie ma sekwencji bialka NA w input
            else:
                tekst_PA_protein, stosunek_PA_protein = check_quality_protein_sequence(plik=plik_fasta_PA.name,
                                                                                       lan=lan)
                if stosunek_PA_protein > 0.2:
                    # sekwencja bialka NA zawiera zbyd duzo N i X
                    error_msg_PA += tekst_PA_protein
                else:

                    PA_alignment = align_fasta(plik_fasta_PA.name, plik_referencja_PA)
                    PA_ref_header = get_fasta_header(plik_referencja_PA)
                    PA_target_header = get_fasta_header(plik_fasta_PA.name)
                    if PA_ref_header == PA_target_header:
                        raise Exception(f'Sekwencje PA maja taki sam header, wychodze')

                    lista_mutacji_reference_PA = determine_muation(ref_seq=PA_alignment[PA_ref_header],
                                                                   target_seq=PA_alignment[PA_target_header])

                    lista_mutacji_targetu_PA = determine_muation(target_seq=PA_alignment[PA_ref_header],
                                                                 ref_seq=PA_alignment[PA_target_header])

                    ref_target_lookup = {}
                    for ref_mutation, target_mutation in zip(lista_mutacji_reference_PA, lista_mutacji_targetu_PA):
                        ref_target_lookup[ref_mutation] = target_mutation


                    slownik_opornosci_baloxavir_PA = parse_resistance_PA(opornosci_PA)
                    lista_mutacji_PA_opornosc_balo, status_balo = get_sample_status(lista_mutacji_reference_PA,
                                                                                    slownik_opornosci_baloxavir_PA)

                    baloxavir_json = prepare_json_for_drug(drug_name="Baloxavir",
                                                           drug_status=status_balo,
                                                           mutation_slownik_mutacji_ref=lista_mutacji_PA_opornosc_balo,
                                                           ref_target_lookup_local=ref_target_lookup)
        else:
            error_msg_PA = "Available only for H1N1, H3N2, Yamagata, and Victoria subtype" if lan == "en" else "Ta analiza wykonywana jest tylko dla podtypow  H1N1, H3N2, Yamagata, oraz Victoria"

        resistance_data_to_json = []
        if len(error_msg_NA) > 0 and  len(error_msg_PA) > 0:
            json_output = {"resistance_status": "blad",
                           "resistance_error_message": f'{error_msg_NA}\t{error_msg_PA}'}
        elif len(error_msg_NA) > 0 and len(error_msg_PA) == 0:
            if len(list(baloxavir_json.keys())) > 0:
                resistance_data_to_json.append(baloxavir_json)
                json_output = {"resistance_status": "tak",
                               "resistance_data": resistance_data_to_json}
            else:
                msg = 'TBD' if lan == "en" else 'TBD'
                json_output = {"resistance_status": "blad",
                               "resistance_error_message": msg}
        elif len(error_msg_PA) > 0 and len(error_msg_NA) == 0:

            resistance_data_to_json.append(oseltamivir_json)
            resistance_data_to_json.append(zanamivir_json)
            resistance_data_to_json.append(peramivir_json)
            resistance_data_to_json.append(laninamivir_json)
            json_output = {"resistance_status": "tak",
                           "resistance_data": resistance_data_to_json}
        else:

            resistance_data_to_json.append(oseltamivir_json)
            resistance_data_to_json.append(zanamivir_json)
            resistance_data_to_json.append(peramivir_json)
            resistance_data_to_json.append(laninamivir_json)
            if len(list(baloxavir_json.keys())) > 0:
                resistance_data_to_json.append(baloxavir_json)

            json_output = {"resistance_status": "tak",
                           "resistance_data": resistance_data_to_json}

        with open(output_json, 'w') as f1:
            f1.write(json.dumps(json_output,  ensure_ascii=False, indent=4))
        return True


if __name__ == '__main__':
    if len(sys.argv) == 1:
        main_program(['--help'])
    else:
        main_program(sys.argv[1:])


