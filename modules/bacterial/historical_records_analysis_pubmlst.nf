process extract_historical_data_pubmlst {
  // The script is nearly identical to extract_historical_data_enterobase
  // However it is eqecuted on a different "branch" of a pipeline
  // and must be duplicated 
  container  = params.main_image
  tag "Extracting historical data for sample $x"
  containerOptions "--volume ${params.db_absolute_path_on_host}:/db"
  cpus 1
  memory "5 GB"
  time "5m"
  publishDir "${params.results_dir}/${x}/", mode: 'copy', pattern: "pubmlst_historical_data.txt" 
  input:
  tuple val(x), path('parsed_phiercc_pubmlst.txt'), val(SPECIES), val(GENUS), val(QC_status), val(QC_status_contaminations)
  output:
  tuple val(x), path('pubmlst_historical_data.txt'), val(SPECIES), val(GENUS), val(QC_status), val(QC_status_contaminations), emit: to_plot
  tuple val(x), path('pubmlst.json'), emit: json
  // when:
  // SPECIES == 'jejuni'

  script:
"""
#!/usr/bin/python
import re
import json
import sys
# Fix to use data downloaded with numpy>2 should be removed when we update bacterial container to numpy>2 
import numpy
sys.modules['numpy._core'] = numpy.core
sys.modules['numpy._core.multiarray'] = numpy.core.multiarray
sys.modules['numpy._core._multiarray_umath'] = numpy.core._multiarray_umath
import numpy as np

species="${SPECIES}"

qc_status="$QC_status"
qc_status_contaminations="$QC_status_contaminations"

if qc_status == "nie" or qc_status_contaminations == "nie":
    with open('pubmlst_historical_data.txt', 'w') as f1:
        f1.write(f'ST\\tComment\\n')
        f1.write(f'unk\\tUnknown species: {species}\\n')

    # json na zle QC
    json_dict = {"dummy" : "dummy"}
    with open('pubmlst.json', "w") as f:
        f.write(json.dumps(json_dict))
    sys.exit(0)

if species != 'jejuni':
    with open('pubmlst_historical_data.txt', 'w') as f1:
        f1.write(f'ST\\tComment\\n')
        f1.write(f'unk\\tUnknown species: {species}\\n')

    # json na zle QC
    json_dict = {"dummy" : "dummy"}
    with open('pubmlst.json', "w") as f:
        f.write(json.dumps(json_dict))
    sys.exit(0)



def get_hiercc_level(my_file):
    with open(my_file) as f:
        i = 0
        for line in f:
            if i == 0:
                klucze = line.rsplit()
            elif i == 1:
                wartosci = line.rsplit()
            i+=1
    slownik = {x:y for x,y in zip(klucze,wartosci)}
    return slownik



directory='/db/pubmlst/Campylobacter/jejuni/' # where are the data for this species 
scheme_name='C. jejuni / C. coli cgMLST v2' # name of the scheme in that database 

slownik_hiercc = get_hiercc_level('parsed_phiercc_pubmlst.txt') # data for our sample 

phiercc_level_userdefined = '25' # pubmlst keeps only values of 5, 10, 25, 50, 100 and 200 for Campylo

common_STs = slownik_hiercc[f'HC{phiercc_level_userdefined}'] 


straindata = np.load(f'{directory}/straindata_table.npy',  allow_pickle=True)
straindata = straindata.item()

dane_historyczne = {} # strain id is a key, as a value we keep country and date
for strain, wartosc in straindata.items():
    if str(wartosc['hiercc'][f'd{phiercc_level_userdefined}']) == str(common_STs):
        for scheme in  wartosc['sts']:
            if scheme['scheme_name'] == scheme_name:
                dane_historyczne[strain] = [wartosc['country'], wartosc['year'], scheme['st_id']]

# zapisujemy dane
with open('pubmlst_historical_data.txt', 'w') as f:
    f.write(f'Strain_id\\tCountry\\tYear\\tST\\n')
    for klucz, wartosc in dane_historyczne.items():
        if wartosc[0] == 'United States' or wartosc[0] == 'USA':
            f.write(f'{klucz}\\tUnited States of America\\t{wartosc[1]}\\t{wartosc[2]}\\n')
        elif 'UK' in wartosc[0]:
            f.write(f'{klucz}\\tUnited Kingdom\\t{wartosc[1]}\\t{wartosc[2]}\\n')
        else:
            f.write(f'{klucz}\\t{wartosc[0]}\\t{wartosc[1]}\\t{wartosc[2]}\\n')

list_withphiercc_to_dump = []
with open('parsed_phiercc_pubmlst.txt') as f1, open('pubmlst.json', 'w') as f2:
    naglowek, wartosci = f1.readlines()
    for level_name, level_value in zip(naglowek.split("\\t")[1:], wartosci.split("\\t")[1:]):
        list_withphiercc_to_dump.append({"level": re.findall("\\d+", level_name.rstrip())[0],
                                         "group_id": level_value.rstrip()})

    to_dump = {"hiercc_clustering_external_data" : list_withphiercc_to_dump,
               "external_historical_data" : [{"level" : phiercc_level_userdefined,
               "data_file" : "${params.results_dir}/${x}/pubmlst_historical_data.txt"}]}
    f2.write(json.dumps(to_dump))
"""
}


process plot_historical_data_pubmlst {
  container  = params.main_image
  cpus 1
  memory "2 GB"
  time "5m"
  tag "Plot historical data for sample $x"
  input:
  tuple val(x), path('pubmlst_historical_data.txt'), val(SPECIES), val(GENUS), val(QC_status), val(QC_status_contaminations)
  output:
  tuple val(x), path('*html')
  // when:
  // SPECIES == 'jejuni'
  script:
// The script requires a geojeson file that is a part of our container
"""

if [[ ${QC_status} == "nie"  || ${QC_status_contaminations} == "nie" ]]; then
    # Tworzenie json i output
    touch empty.html

else
  if [ ${SPECIES} == "jejuni" ]; then
    python /data/plot_historical_data_plotly.py pubmlst_historical_data.txt pubmlst_historical_data 2009
  else
    touch empty.html
    # json na zly gatunek
  fi
fi
"""

}