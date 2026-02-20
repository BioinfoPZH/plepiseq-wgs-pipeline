#!/usr/bin/env python3

"""
Skrypt który na podstawie dopasowania sekwencji referencyjnej z targetowa (bez N-ek)
wygeneruje vcf-a
"""

import os
import re
import subprocess
import sys
from typing import Dict

import numpy as np
import vcf
from iranges import IRanges
from minineedle import needle, core
import miniseq

def _first_scalar(value):
    try:
        return int(value[0])
    except Exception:
        return int(value)


def _extract_iranges_bounds(mutacja):
    """
    Parse IRanges iteration item across versions.
    Returns python-slice bounds: [start, end_exclusive).
    """
    # Older iteration yielded tuples like (idx, range_obj), newer may yield only range_obj.
    candidate = mutacja
    if isinstance(mutacja, (list, tuple)) and len(mutacja) > 1:
        candidate = mutacja[1]

    if not hasattr(candidate, 'start') or not hasattr(candidate, 'end'):
        raise TypeError(f'Unsupported IRanges item: {type(mutacja)} -> {mutacja}')

    start = _first_scalar(candidate.start)
    # width is the stable cross-version definition of interval span.
    if hasattr(candidate, 'width'):
        end = start + _first_scalar(candidate.width)
    else:
        end = _first_scalar(candidate.end)
        if end <= start:
            end = start + 1

    return start, end


def align_fasta_nw(fasta1_file: str, *args, **kwargs) -> Dict:
    fasta1_file = [os.path.abspath(fasta1_file)]
    stan1 = os.path.isfile(fasta1_file[0])
    if not stan1:
        raise Exception('Nie podano poprawnej sciezki do pliku!')
    if len(args) > 0:
        fasta1_file.extend([os.path.abspath(x) for x in args])
        stan2 = np.all([os.path.isfile(plik) for plik in fasta1_file])
        if not stan2:
            raise Exception('Podane dodatkowe pliki nie istnieja')

    with open('tmp.fasta', 'w') as f:
        for plik in fasta1_file:
            with open(plik, 'r') as f1:
                for line in f1:
                    f.write(line)

    slownik_alignmentu = {}
    fasta = miniseq.FASTA(filename="tmp.fasta")
    seq1, seq2 = fasta[0], fasta[1]
    alignment1: needle.NeedlemanWunsch[str] = needle.NeedlemanWunsch(seq1, seq2)
    alignment1.align()
    al1, al2 = alignment1.get_aligned_sequences(core.AlignmentFormat.str)
    slownik_alignmentu[f'>{seq1.get_identifier()}'] = al1
    slownik_alignmentu[f'>{seq2.get_identifier()}'] = al2

    os.remove('tmp.fasta')

    return slownik_alignmentu

def align_fasta_muscle(fasta1_file: str, *args, **kwargs) -> Dict:
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

    polecenie = ('muscle3 -quiet -in tmp.fasta -out tmp_aln.fasta')

    alignment = subprocess.Popen(polecenie, shell=True, stdout=subprocess.PIPE)
    alignment.wait()
    # generowanie slownika
    slownik_alignmentu = {}
    for line in open('tmp_aln.fasta'):
        line = line.rstrip()
        if len(re.findall(">", line)) > 0:
            klucz = line[:]
            if klucz not in slownik_alignmentu.keys():
                slownik_alignmentu[klucz] = ''
        else:
            slownik_alignmentu[klucz] = slownik_alignmentu[klucz] + line
    # pozbywamy sie tepowego pliku
    os.remove('tmp.fasta')
    os.remove('tmp_aln.fasta')

    return slownik_alignmentu


def prep_mutation_list(slownik, ref_name, target_name, n=1):
    """
    Funkcja generuje slownik ktorego kluczem jest dwuelementowa lista np 10:['A'. 'AAT'] oznaczaloby ze
    mamy na 10 pozycji insercje AT
    :param slownik: slownik alignmentu sekwencji referencyjnej
    :param ref_name: nazwa sekwencji referencyjnej w slowniku
    :param target_name: mazwa sekwencji targetowej w slowniku
    :param n: maksymalna odleglosc miedzy mutacjami aby zostaly zuanane za jedna zmiane i "polaczone"
    :return: bool
    """

    sekwencja_ref = slownik[ref_name]
    sekwencja_target = slownik[target_name]
    stany = []  # to bedzie lista "stanow" dla kazdej pozycji w alignmencie 0 jesli nie ma mutacji 1 jesli jest
    numeracja_referencji = []  # to jest slave stanow ale w przypadku delecji w referencji nie zmieniamy numeru
    # tzn mozemy miec liste [1,1,1,2,3,4] dla alignmentu [A--GC]

    i = 1
    # zapelniamy stany
    for k, j in zip(sekwencja_ref, sekwencja_target):
        if k == j or (k != j and k == 'N' or j == 'N'):
            # brak mutacji
            stany.append(0)
        else:
            stany.append(1)

        if k != '-':
            numeracja_referencji.append(i)
            i += 1
        else:
            numeracja_referencji.append(i)

    # konwerujemy liste indeksow tak by zamienic je w rnage-a tnz [0,1], [3,10] oznaczaloby ze na pozycji 0 i 3 do 9 wlacznie byly ciagle 1

    lista_zakresow = []
    poprzednia_wartosc = 0

    for indeks, wartosc in enumerate(stany):
        if wartosc == 1 and poprzednia_wartosc == 0:
            # otwieram okno
            start = indeks
        elif wartosc == 1 and poprzednia_wartosc == 1:
            pass
        elif wartosc == 0 and poprzednia_wartosc == 1:
            koniec = indeks
            lista_zakresow.append([start, koniec])

        poprzednia_wartosc = wartosc

    # gdyby mutacja byla na ostatnim elemencie
    if wartosc == 1:
        try:
            lista_zakresow.append([start, koniec])
        except Exception:
            pass

    if len(lista_zakresow) == 0:
        # brak mutacji
        # dummy slownik mutacji
        slownik_mutacji = {1: ['A', 'A']}
        return slownik_mutacji

    # tworzenie irange
    # potrzebujemy dwoch list o rownej dlugosci jedna zawiera poczatki zakresu
    # druga zawiera dlugosci (konstruktor nie pozwala dac start, end jak w R)
    starts = []
    widths = []
    for i, j in lista_zakresow:
        starts.append(i)
        widths.append(j - i)
    x = IRanges(start=starts, width=widths)
    try:
        zasiegi_mutacji_reduced = x.reduce(min_gap_width=n + 1)
    except Exception:
        # w przypadku pustego irange'a reduce zwraca blad wiec tworzymy sztucznie zasieg obejmujacy pierwsza pozycje

        zasiegi_mutacji_reduced = IRanges(start=[0], width=[1])

    slownik_mutacji = {}
    for mutacja in zasiegi_mutacji_reduced:
        indeks_poczatku, indeks_konca = _extract_iranges_bounds(mutacja)

        if '-' in sekwencja_target[indeks_poczatku:indeks_konca] or '-' in sekwencja_ref[indeks_poczatku:indeks_konca]:
            # dla indeli zaczynamy raportowac mutacje na ostatniej wspolnej pozycji
            if indeks_poczatku != 0:
                indeks_poczatku -= 1
            else:
                pass
                # sekwencja zaczyna sie od delecji nie wiem jak to wygladac moze w vcf

        # zamieniamy indeks z alignmentu na numeracje w referencji

        slownik_mutacji[numeracja_referencji[indeks_poczatku]] = [sekwencja_ref[indeks_poczatku:indeks_konca],
                                                                  sekwencja_target[indeks_poczatku:indeks_konca]]
    return slownik_mutacji


def create_vcf_file(slownik, vcf_template, vcf_output, ref_name):
    """
    Stworz vcf-a na podstawie slownika, uwaga dziala tylko na jednosegmentowe genomy !
    :param slownik:
    :param vcf_template:
    :param vcf_output:
    :return:
    """
    vcf_template = vcf.Reader(filename=vcf_template)
    vcf_output = vcf.Writer(open(vcf_output, 'w'), vcf_template)

    for record in vcf_template:
        pass
        # potrzebujemy aby poprawnie nizej definiowac record.INFO itd ...

    for klucz, wartosc in slownik.items():
        ref_clean = wartosc[0].replace('-', '')
        alt_clean = wartosc[1].replace('-', '')
        a = vcf.model._Record(CHROM=ref_name.replace('>', ''),
                              POS=klucz,
                              ID='.',
                              REF=ref_clean,
                              ALT=[vcf.model._Substitution(nucleotides=alt_clean)],
                              # ALT jest lista klasy ._Substitution \
                              QUAL=60,
                              FILTER=[],
                              INFO=record.INFO, FORMAT=record.FORMAT, samples=record.samples,
                              sample_indexes=record._sample_indexes)
        vcf_output.write_record(a)
    return True


if __name__ == '__main__':
    sekwencja_referencji = sys.argv[1]
    sekwencja_targetu = sys.argv[2]
    n = int(sys.argv[3])

    vcf_template = sys.argv[4]
    vcf_output = sys.argv[5]

    #  uliniawianie sekwencji referencyjnej i "konsensusowej"
    #  slownik_alignmentu = align_fasta_muscle(sekwencja_referencji, sekwencja_targetu)

    slownik_alignmentu = align_fasta_nw(sekwencja_referencji, sekwencja_targetu)

    nazwa_referencji = open(sekwencja_referencji).readlines()[0].strip()
    nazwa_targetu = open(sekwencja_targetu).readlines()[0].strip()

    # print(slownik_alignmentu)

    # tworzenie slownika mutacji w postaci klucz pozycja mutacji wartosc dwuelementowa lista gdzie 1-element
    # to sekwencja referencyjna a drugi sekwencja z konsnesusu np. :
    # {241: ['C', 'T'], 467: ['T', 'C'], ..
    slownik_mutacji = prep_mutation_list(slownik=slownik_alignmentu,
                                         ref_name=nazwa_referencji,
                                         target_name=nazwa_targetu,
                                         n=n)

    # skladanie vcf-a
    # uwaga templateowy vcf musi byc przygotowany pod konkretny organizm
    # bo nie wiem jak zadziala gdy pole contig nie bedzie sie zgadzalo z kolumna chrom

    create_vcf_file(slownik=slownik_mutacji,
                    vcf_template=vcf_template,
                    vcf_output=vcf_output,
                    ref_name=nazwa_referencji)
