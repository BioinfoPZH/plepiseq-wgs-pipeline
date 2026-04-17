


process run_pHierCC_local {
  // Query results of local clustering using enterobaso r pubmlst data 
  // Jeden zawierajacy klastrowanie SINGLE linkage zbudowane na 430k profili z enterobase
  // Drugi zawierajacy klastrowanie COMPLETE linkage zbudowane na 430k profili z enterobae
  container  = params.main_image
  containerOptions "--volume ${params.db_absolute_path_on_host}:/db"
  cpus 1
  memory "5 GB"
  time "5m"
  tag "Predicting hierCC with local database for sample $x"
  input:
  tuple val(x), path('cgMLST_parsed_output.txt'), path('cgMLST_sample_full_list_of_allels.txt'), path('cgMLST_closest_ST_full_list_of_allels.txt'), val(SPECIES), val(GENUS),  val(QC_status), val(QC_status_contaminations)
  output:
  tuple val(x), path('parsed_phiercc_minimum_spanning_tree.txt'), path('parsed_phiercc_maximum_spanning_tree.txt'), emit: to_pubdir
  tuple val(x), path('cgMLST_json_phiercc_local.json'), emit: json
  // when:
  // GENUS == 'Salmonella' || GENUS == 'Escherichia' || SPECIES == 'jejuni'
  script:
"""
#!/usr/bin/python
import  sys
import gzip
import re
import numpy as np
import json

species="$SPECIES"
genus="$GENUS"
qc_status="$QC_status"
qc_status_contaminations="$QC_status_contaminations"

if qc_status == "nie" or qc_status_contaminations == "nie":
    with open('parsed_phiercc_minimum_spanning_tree.txt', 'w') as f1, open('parsed_phiercc_maximum_spanning_tree.txt', 'w') as f2:
        f1.write(f'ST\\tComment\\n')
        f1.write(f'unk\\tUnknown species: {species}\\n')

        f2.write(f'ST\\tComment\\n')
        f2.write(f'unk\\tUnknownn species: {species}')

    # json na zle QC
    # results of this module provide only one field to bigger cgMLST data, thus this is just dummy json so that nextflow wont crash
    json_dict = {"dummy" : "dummy"}
    with open('cgMLST_json_phiercc_local.json', "w") as f:
        f.write(json.dumps(json_dict))
    sys.exit(0)


def getST(my_file):
    with open(my_file) as f:
        for line in f:
            line = line.rsplit()
            if line[0] == "ST_sample":
                # pass the first line
                continue
            else:
                # the second line in a file is what we want
                return line[0], line[1], int(line[2])

ST_sample, ST_matching, my_dist = getST('cgMLST_parsed_output.txt')

if genus ==  'Salmonella':
    directory=f'/db/phiercc_local/{genus}/'
    phiercc_header='ST\\tHC0\\tHC2\\tHC5\\tHC10\\tHC20\\tHC50\\tHC100\\tHC200\\tHC400\\tHC900(ceBG)\\tHC2000\\tHC2600\\tHC2850(subsp.)\\n'
    lista_kluczy = ['d0', 'd2', 'd5', 'd10', 'd20', 'd50', 'd100', 'd200' , 'd400', 'd900', 'd2000', 'd2600', 'd2850']
elif genus == 'Escherichia':
    directory=f'/db/phiercc_local/{genus}/'
    phiercc_header='ST\\tHC0\\tHC2\\tHC5\\tHC10\\tHC20\\tHC50\\tHC100\\tHC200\\tHC400\\tHC1100(cgST Cplx)\\tHC1500\\tHC2000\\tHC2350(subsp.)\\n'
    lista_kluczy = ['d0', 'd2', 'd5', 'd10', 'd20', 'd50', 'd100', 'd200' , 'd400', 'd1100', 'd1500', 'd2000', 'd2350']
elif species == 'jejuni':
    # Provisal selecetion of jejuni
    phiercc_header='ST\\tHC5\\tHC10\\tHC25\\tHC50\\tHC100\\tHC200\\n' 
    lista_kluczy = ['d5', 'd10', 'd25', 'd50', 'd100', 'd200']
    directory=f'/db/phiercc_local/{genus}/jejuni/'

else:
    with open('parsed_phiercc_minimum_spanning_tree.txt', 'w') as f1, open('parsed_phiercc_maximum_spanning_tree.txt', 'w') as f2:
        f1.write('Provided species: {species} is not part of any cgMLST scheme')
        f2.write('Provided species: {species} is not part of any cgMLST scheme')
        # json na zly gatunek
        json_dict = {"dummy" : "dummy"}
        with open('cgMLST_json_phiercc_local.json', "w") as f:
            f.write(json.dumps(json_dict)) 
        sys.exit(0)

# 1. Szukanie w wynikach mojego klastrowania z uzyciem single linkage
with open('parsed_phiercc_minimum_spanning_tree.txt', 'w') as f, gzip.open(f'{directory}/profile_single_linkage.HierCC.gz') as f2, open(f'{directory}/profile_single_linkage.HierCC.index') as f3:
    f.write(phiercc_header)
    pointer = 0
    for line in f3:
        line = line.rsplit()
        try:
            if int(ST_matching) < int(line[0]):
                break
            else:
                pointer = int(line[1])

        except ValueError:
            pass
            # pierwszy wiersz w indekszie to naglowek
    # ustaw kursow blizej lokalziacji przed szukanym ST
    f2.seek(pointer)
    for line in f2:
        line = list(map(lambda x: x.decode('utf-8', errors='replace'), line.split()))
        if line[0] == ST_matching:
            if re.findall('Salmo', species):    
                lista_poziomow = [line[1], line[3], line[6], line[11], line[21], line[51], line[101], line[201], line[401], line[901], line[2001], line[2601], line[2851]]
            elif re.findall('Escher', species):
                lista_poziomow = [line[1], line[3], line[6], line[11], line[21], line[51], line[101], line[201], line[401], line[1101], line[1501], line[2001], line[2351]]
            elif re.findall('jejun', species):
                lista_poziomow = [line[6], line[11], line[26], line[51], line[101], line[201]]
            else:
                pass

            try:
                last_index = np.where(list(map(lambda x: int(re.findall('\\d+', x)[0]) < my_dist, lista_kluczy)))[0][-1]
                lista_poziomow[:(last_index + 1)] = [ST_sample] * (last_index +1)
            except IndexError:
                pass
            formatted_string = "\t".join(list(map(str, lista_poziomow)))
            f.write(f'{ST_sample}\\t{formatted_string}\\n')
            # nie ma potrzeby dalszego ogladania pliku
            break
    # check if lista_poziomow exists, in case user have a novel profiles list from enterobase, but phierCC data are obsolete and do not 
    # include novel ST
    try:
        print(lista_poziomow)
    except NameError:
        if re.findall('Salmo', species):
            lista_poziomow = ["-1"] * 13
        elif re.findall('Escher', species):
            lista_poziomow = ["-1"] * 13
        elif re.findall('jejun', species):
            lista_poziomow = ["-1"] * 6
        formatted_string = "\\t".join(list(map(str, lista_poziomow)))
        f.write(f'{ST_sample}\\t{formatted_string}\\n')
    list_to_dump = []
    # header ma slowo ST w nazwie ktore omijamu
    for level_name, level_value in zip(phiercc_header.split("\\t")[1:], lista_poziomow):
        list_to_dump.append({"level": re.findall("\\d+", level_name.rstrip())[0],
                             "group_id": level_value.rstrip()})
    with open('cgMLST_json_phiercc_local.json', "w") as f:
        to_dump = {"hiercc_clustering_internal_data": list_to_dump}
        f.write(json.dumps(to_dump))    
 
# 2. Szukanie pHierCC w wynikach  maximum spanning tree
with open('parsed_phiercc_maximum_spanning_tree.txt', 'w') as f, gzip.open(f'{directory}/profile_complete_linkage.HierCC.gz') as f2, open(f'{directory}/profile_complete_linkage.HierCC.index') as f3:
    f.write(phiercc_header)
    pointer = 0
    for line in f3:
        line = line.rsplit()
        try:
            if int(ST_matching) < int(line[0]):
                break
            else:
                pointer = int(line[1])

        except ValueError:
            pass
            # pierwszy wiersz w indekszie to naglowek
    # ustaw kursow blizej lokalziacji przed szukanym ST
    f2.seek(pointer)
    for line in f2:
        line = list(map(lambda x: x.decode('utf-8', errors='replace'), line.split()))
        if line[0] == ST_matching:
            if re.findall('Salmo', species):
                lista_poziomow = [line[1], line[3], line[6], line[11], line[21], line[51], line[101],  line[201], line[401], line[901], line[2001], line[2601], line[2851]]
            elif re.findall('Escher', species):
                lista_poziomow = [line[1], line[3], line[6], line[11], line[21], line[51], line[101],  line[201], line[401], line[1101], line[1501], line[2001], line[2351]]
            elif re.findall('jejun', species):
                lista_poziomow = [line[6], line[11], line[26], line[51], line[101], line[201]]
            else:
                pass
            try:
                last_index = np.where(list(map(lambda x: int(re.findall('\\d+', x)[0]) < my_dist, lista_kluczy)))[0][-1] 
                lista_poziomow[:(last_index + 1)] = [ST_sample] * (last_index +1)
            except IndexError:
                pass 
            formatted_string = "\\t".join(list(map(str, lista_poziomow)))
            f.write(f'{ST_sample}\\t{formatted_string}\\n')
            # nie ma potrzeby dalszego ogladania pliku
            break
    try:
        print(lista_poziomow)
    except NameError:
        if re.findall('Salmo', species):
            lista_poziomow = ["-1"] * 13
        elif re.findall('Escher', species):
            lista_poziomow = ["-1"] * 13
        elif re.findall('jejun', species):
            lista_poziomow = ["-1"] * 6
        formatted_string = "\t".join(list(map(str, lista_poziomow)))
        f.write(f'{ST_sample}\\t{formatted_string}\\n')
"""
}