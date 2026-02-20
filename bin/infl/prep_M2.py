#!/usr/bin/env python3

"""
Skrpyt sluzy do otrzymania sekwencji bialka M2 ktorego nei da sie za bardzo
otrzymac przy pomocy klasycznego nextclade, bialko kodowane jest w segmencie
w dwoch czesciach np dla h3n2 w regionie 26 do 51 i od 740 do 1007
Co potrzebujemy w pliku gff umieszcamy takie wiersze

chr7_MP .       exon    26      51      .       +       .       exon_name=M2_1
chr7_MP .       exon    740     1007    .       +       .       exon_name=M2_2

i

chrX_M2 .       gene    1       294     .       +       .       gene_name=M2

Pierwsze dwa pozluza do wyciagniecia z alignmentu referencyjnego segmentu M2 i
outputu skryptu odpowiednich regionow, umieszczenie ich w czasowych plikach i wywolanie
nextclade'a
"""

import os
import re
import subprocess
import sys

import numpy as np


def align_fasta(fasta1_file: str, *args, **kwargs):
    """
    funkcja do alignowania sekwencji.  Sekwencje albo sa w oddzielnych plikach i wtedy laczymy je w jednym
    tmp file (usuwanym po wykonaniu funkcji), albo w jednym pliku. Do alignowania uzywamy maffta w strategii
    auto
    :param fasta1_file: str, sciezka do pliku fasta zawierajacego jedna lub wiecej sekwencji
    :param args: tupple, sciezka do dowolnej liczy plikow fasta
    :return: dict, slownik ktora jako klucze zawiera identyfikatory sekwencji z podanych pliku/plikow a jako klucze
    alignowano sekwencje
    """

    # Najpierw normalizujemy sciezki i sprawdzamy czy podane plik/pliki istnieja
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

    polecenie = ('mafft --auto --quiet --inputorder tmp.fasta')
    if 'fast' in kwargs.keys():
        polecenie = ('mafft --retree 1 --quiet --inputorder tmp.fasta')  # komenda z
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


def parse_gff3(plik):
    """
    Funkcja wyciaga z gf granice sekwencji kodujacej bialko M2
    plik gff jest tak przygotowany ze te dwa fragmenty maja jako
    kolumne feature (czyli 3 kolumna w pliku) slowo exon
    :param plik:
    :return:
    """
    lista_pozycji = []
    for line in open(plik):
        line = line.split()
        if line[2] == 'exon':
            lista_pozycji.extend(list(range(int(line[3]) - 1, int(line[4]))))
    return lista_pozycji


def trim_alignment(sekwencja):
    front = []
    back = []
    i = 0
    for element in sekwencja:
        if element == '-':
            front.append(i)
        else:
            break
        i += 1
    i = 0
    for element in sekwencja[::-1]:
        if element == "-":
            back.append(len(sekwencja) - i - 1)
        else:
            break
    return front, back


def extract_regions_from_alignment(slownik_alignmentu, lista_pozycji, dir_output, name):
    """
    Funkcja ktora wyciaga z alignmentu okreslone regiony *fefiniowane jako kluczy slownika slownik_regionow
    i zapisuje do plikow w katalogu dir_output z naglowkami name1 i name2
    :param slownik_alignmentu:
    :param slownik_regionow:
    :param dir_output:
    :param name:
    :return:
    """
    # i -pozycja kursora w sekwencji referencyjnej
    # ta pozycja musi sie zgadzać z pozycjami w gff
    # omija "-" bo te nie sa w końcu częścią referencji
    # j - pozycja w alignment
    i = j = 0
    sekwencja_referencyjna = ''
    sekwencja_moja = ''

    # zapiszmy indeksy w alignment ktore stanowia delecje na 3' i 5' koncach sekwencji referencyjnej
    # te pozucje bedziemy omijac
    front, back = trim_alignment(slownik_alignmentu['>chr7_MP'])
    for element in slownik_alignmentu['>chr7_MP']:
        if j in front or j in back:
            # przesun kursor w alignment ale nic nie rob
            # to jest potrzebne tylko po to by "omijac"  niezalignowany fragment na 3' koncu
            j += 1
            continue
        if i in lista_pozycji:
            sekwencja_referencyjna += slownik_alignmentu['>chr7_MP'][j]
            sekwencja_moja += slownik_alignmentu['>MP'][j]
        if element == '-':
            # przesun tylko kursor alignmentu w swkencji referenyjnej jest "-"
            j += 1
        else:
            i += 1
            j += 1

    with open(f'{dir_output}/M2.fasta', 'w') as f:
        f.write(f'>chrX_M2\n{sekwencja_referencyjna}\n')

    with open(f'{dir_output}/consensus_M2.fasta', 'w') as f:
        f.write(f'>M2\n{sekwencja_moja.replace("-", "")}\n')
    return True

if __name__ == '__main__':
    podtyp = sys.argv[1]
    plik_wynik = sys.argv[2]
    dir_dane = sys.argv[3]
    dir_input = sys.argv[4]
    dir_output = sys.argv[5]

    # wyciaganie informacji jaki region mnie interesuje
    moj_gff = f'{dir_dane}/{podtyp}/{podtyp}.gff'
    lista_pozycji = parse_gff3(moj_gff)

    # tworzenie alignmentu refeencji z moim MP
    referencja_MP = f'{dir_dane}/{podtyp}/MP.fasta'
    moj_MP = f'{dir_input}/consensus_MP.fasta'
    # krok pierwszy mapowanie krok pierwszy mapo
    slownik_alignmentu = align_fasta(referencja_MP, moj_MP)

    # zapisywanie wynikow
    extract_regions_from_alignment(slownik_alignmentu=slownik_alignmentu,
                                   lista_pozycji=lista_pozycji,
                                   dir_output=dir_output,
                                   name='chrX_M2')
