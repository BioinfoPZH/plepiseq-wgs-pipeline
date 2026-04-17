process run_pHierCC_enterobase {
  // Funkcja odpytuje API Enterobase w celu wyciagniecia profuili z tej bazu
  // It work only for Salmonella and Escherichia as Campylo is not present in Enterobase
  container  = params.main_image
  tag "Predicting hierCC from enterobase for sample $x"
  cpus 1
  memory "5 GB"
  time "5m"
  input:
  tuple val(x), path('cgMLST_parsed_output.txt'), path('cgMLST_sample_full_list_of_allels.txt'), path('cgMLST_closest_ST_full_list_of_allels.txt'), val(SPECIES), val(GENUS), val(QC_status), val(QC_status_contaminations)
  output:
  tuple val(x), path('parsed_phiercc_enterobase.txt'), val(SPECIES), val(GENUS), val(QC_status), val(QC_status_contaminations)
  // when:
  // GENUS == 'Salmonella' || GENUS == 'Escherichia'
  script:
"""
#!/usr/bin/python
### kod pobrany ze strony https://enterobase.readthedocs.io/en/latest/api/api-getting-started.html ###

import  sys
from urllib.request import urlopen
from urllib.error import HTTPError
import urllib
import base64
import json
import gzip
import re
import numpy as np
import time

species="$SPECIES"
genus="$GENUS"
qc_status="$QC_status"
qc_status_contaminations="$QC_status_contaminations"

if qc_status == "nie" or qc_status_contaminations == "nie":
    with open('parsed_phiercc_enterobase.txt', 'w') as f1:
        f1.write(f'ST\\tComment\\n')
        f1.write(f'unk\\tUnknown species: {species}\\n')

   
    # json na zle QC
    # json przerzucony jest do modulu extract_historical_data
    sys.exit(0)

# time.sleep(np.random.randint(2,20))

API_TOKEN = "${params.enterobase_api_token}"

def __create_request(request_str):
    base64string = base64.b64encode('{0}: '.format(API_TOKEN).encode('utf-8'))
    headers = {"Authorization": "Basic {0}".format(base64string.decode())}
    request = urllib.request.Request(request_str, None, headers)
    return request

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

if genus == 'Salmonella':
    DATABASE="senterica" 
    scheme_name="cgMLST_v2"
    phiercc_header='ST\\tHC0\\tHC2\\tHC5\\tHC10\\tHC20\\tHC50\\tHC100\\tHC200\\tHC400\\tHC900(ceBG)\\tHC2000\\tHC2600\\tHC2850(subsp.)\\n'
    lista_kluczy = ['d0', 'd2', 'd5', 'd10', 'd20', 'd50', 'd100', 'd200' , 'd400', 'd900', 'd2000', 'd2600', 'd2850']
elif genus == 'Escherichia':
    DATABASE="ecoli"
    scheme_name="cgMLST"
    phiercc_header='ST\\tHC0\\tHC2\\tHC5\\tHC10\\tHC20\\tHC50\\tHC100\\tHC200\\tHC400\\tHC1100(cgST Cplx)\\tHC1500\\tHC2000\\tHC2350(subsp.)\\n'
    lista_kluczy = ['d0', 'd2', 'd5', 'd10', 'd20', 'd50', 'd100', 'd200' , 'd400', 'd1100', 'd1500', 'd2000', 'd2350']
else:
    with open('parsed_phiercc_enterobase.txt', 'w') as f:
        f.write('Provided genus: {genus} is not part of the Enterobase')
    sys.exit(0)
    # json for wrong species
    # json przerzucony jest do modulu extract_historical_data

with open('parsed_phiercc_enterobase.txt', 'w') as f:
    f.write(phiercc_header)
    address = f"https://enterobase.warwick.ac.uk/api/v2.0/{DATABASE}/{scheme_name}/sts?st_id={ST_matching}&scheme={scheme_name}&limit=5"
    try:
        response = urlopen(__create_request(address))
        data = json.load(response)
        lista_poziomow = [data['STs'][0]['info']['hierCC'][x] for x in lista_kluczy] # lista z uporzadkowanymi poziomami
        # modify outpuy to include distance > 0 that mean we have "local" STs
        try:
            last_index = np.where(list(map(lambda x: int(re.findall('\\d+', x)[0]) < my_dist, lista_kluczy)))[0][-1]
            lista_poziomow[:(last_index+1)] = [ST_sample] * (last_index + 1)
        except IndexError:
            pass
        formatted_string = "\t".join(list(map(str, lista_poziomow)))
        f.write(f'{ST_sample}\\t{formatted_string}\\n')
    except HTTPError as Response_error:
        print(f"{Response_error.code} {Response_error.reason}. URL: {Response_error.geturl()}\\n Reason: {Response_error.read()}")
        sys.exit(1)

"""
}

process run_pHierCC_pubmlst {
  // For Pubmlst we cannot ask the website directly for phiercc-like level, we must query downloaded database 
  container  = params.main_image
  containerOptions "--volume ${params.db_absolute_path_on_host}:/db"
  cpus 1
  memory "1 GB"
  time "5m"
  tag "Predicting hierCC with local database for sample $x"
  input:
  tuple val(x), path('cgMLST_parsed_output.txt'), path('cgMLST_sample_full_list_of_allels.txt'), path('cgMLST_closest_ST_full_list_of_allels.txt'), val(SPECIES), val(GENUS),  val(QC_status), val(QC_status_contaminations)
  output:
  tuple val(x), path('parsed_phiercc_pubmlst.txt'), val(SPECIES), val(GENUS),  val(QC_status), val(QC_status_contaminations)
  //when:
  //SPECIES == 'jejuni'
  script:
"""
#!/usr/bin/python
import  sys
import gzip
import re

# Fix to use data downloaded with numpy>2 should be removed when we update bacterial container
import numpy
sys.modules['numpy._core'] = numpy.core
sys.modules['numpy._core.multiarray'] = numpy.core.multiarray
sys.modules['numpy._core._multiarray_umath'] = numpy.core._multiarray_umath
import numpy as np

species="$SPECIES"
genus="$GENUS"
qc_status="$QC_status"
qc_status_contaminations="$QC_status_contaminations"

if qc_status == "nie" or qc_status_contaminations == "nie":
    with open('parsed_phiercc_pubmlst.txt', 'w') as f1:
        f1.write(f'ST\\tComment\\n')
        f1.write(f'unk\\tUnknown species: {species}\\n')


    # json na zle QC
    # json przerzucony jest do modulu extract_historical_data
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


if species != "jejuni":
    with open('parsed_phiercc_pubmlst.txt', 'w') as f1:
        f1.write(f'ST\\tComment\\n')
        f1.write(f'unk\\tUnknown species: {species}\\n')


    # json na zly gatunek
    # json przerzucony jest do modulu extract_historical_data
    sys.exit(0)

ST_sample, ST_matching, my_dist = getST('cgMLST_parsed_output.txt')

phiercc_header='ST\\tHC5\\tHC10\\tHC25\\tHC50\\tHC100\\tHC200\\n'
lista_kluczy = ['d5', 'd10', 'd25', 'd50','d100', 'd200']
directory=f'/db/pubmlst/{genus}/jejuni/'


STs_data = np.load(f'{directory}/sts_table.npy', allow_pickle=True).item() 

# Matching ST must be in the data

# in case pubmlst data were not updated 
# STs_data might not include ST_matching from profile file and we get KeyError
# for now we just copy -1 mutliple times, like in case of enterobase local files

try:
    lista_poziomow = [STs_data[ST_matching][level] for level in lista_kluczy]
except KeyError:
    lista_poziomow = [int(-1)] * len(lista_kluczy)

try:
    last_index = np.where(list(map(lambda x: int(re.findall('\\d+', x)[0]) < my_dist, lista_kluczy)))[0][-1]
    lista_poziomow[:(last_index + 1)] = [ST_sample] * (last_index +1)
except IndexError:
    pass

with open('parsed_phiercc_pubmlst.txt', 'w') as f:
    f.write(phiercc_header)
    formatted_string = "\t".join(list(map(str, lista_poziomow)))
    f.write(f'{ST_sample}\\t{formatted_string}\\n')

"""
}