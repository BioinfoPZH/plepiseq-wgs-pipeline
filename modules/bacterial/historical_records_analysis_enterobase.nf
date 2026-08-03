


process extract_historical_data_enterobase {
  container  = params.main_image
  tag "Extracting historical data for sample $x"
  containerOptions "--volume ${params.db_absolute_path_on_host}:/db"
  publishDir "${params.results_dir}/${x}/", mode: 'copy', pattern: "enterobase_historical_data.txt"
  cpus 1
  memory "15 GB"
  time "10m"
  input:
  tuple val(x), path('parsed_phiercc_enterobase.txt'), val(SPECIES), val(GENUS),  val(QC_status), val(QC_status_contaminations)
  output:
  tuple val(x), path('enterobase_historical_data.txt'), val(SPECIES), val(GENUS), val(QC_status), val(QC_status_contaminations), emit: to_pubdir
  tuple val(x), path('enterobase.json'), emit: json
  // when:
  // GENUS == 'Salmonella' || GENUS == 'Escherichia'
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
genus="$GENUS"

qc_status="$QC_status"
qc_status_contaminations="$QC_status_contaminations"

if qc_status == "nie" or qc_status_contaminations == "nie":
    with open('enterobase_historical_data.txt', 'w') as f1:
        f1.write(f'ST\\tComment\\n')
        f1.write(f'unk\\tUnknown species: {species}\\n')

    # json na zle QC
    json_dict = {"dummy" : "dummy"}
    with open('enterobase.json', "w") as f:
        f.write(json.dumps(json_dict))
    sys.exit(0)

if genus != 'Salmonella' and genus != 'Escherichia':
    with open('enterobase_historical_data.txt', 'w') as f1:
        f1.write(f'ST\\tComment\\n')
        f1.write(f'unk\\tUnknown species: {species}\\n')

    # json na zle QC 
    json_dict = {"dummy" : "dummy"}
    with open('enterobase.json', "w") as f:
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

# we keep the data as a dict with "level" i.e. 'HC2' as a key and cluster id i.e. 40 as value
slownik_hiercc = get_hiercc_level('parsed_phiercc_enterobase.txt')

# As a default we look for all the strains that belong to the same cluster at THIS level
# BE AWARE THAT SOMETIMES KEYS HAVE STRANGE NOTATION LIKE "HC1100(cgST Cplx)", THIS WANT WORK IN THAT CASE
try: 
    if re.findall('Salmo', species): 
        phiercc_level_userdefined = '5'
        scheme_name="cgMLST_v2"
    elif re.findall('Escher', species):
        phiercc_level_userdefined = '20' # Escherichia seems to be much more diverse
        scheme_name="cgMLST"
    common_STs = slownik_hiercc[f'HC{phiercc_level_userdefined}']
except KeyError:
    print('Provided key does not exist')
    sys.exit(0)

directory=f'/db/enterobase/{genus}/'
# Now we look for ALL STs belonging to the same cluster as our cluster given phiercc_level
# for that we query our local enterobase instance
expected_ST = {}
STs = np.load(f'{directory}/sts_table.npy', allow_pickle=True)
STs = STs.item()

for klucz,wartosc in STs.items(): 
    if wartosc[f'd{phiercc_level_userdefined}'] == common_STs:
        expected_ST[klucz] = ''
    		

straindata = np.load(f'{directory}/straindata_table.npy',  allow_pickle=True)
straindata = straindata.item()

dane_historyczne = {} # kluczem jest nazwa szczepu , wartoscia 2 elementowa lista z krajem i rokiem
for strain, wartosc in straindata.items():
    for scheme in wartosc['sts']:
        if scheme_name in scheme.values() and str(scheme['st_id']) in expected_ST.keys():
            dane_historyczne[strain] = [wartosc['country'], wartosc['collection_year'], scheme['st_id']]  

# zapisujemy dane
with open('enterobase_historical_data.txt', 'w') as f:
    f.write(f'Strain_id\\tCountry\\tYear\\tST\\n')
    for klucz, wartosc in dane_historyczne.items():
        if wartosc[0] == 'United States' or wartosc[0] == 'USA':
            f.write(f'{klucz}\\tUnited States of America\\t{wartosc[1]}\\t{wartosc[2]}\\n')
        else:
            f.write(f'{klucz}\\t{wartosc[0]}\\t{wartosc[1]}\\t{wartosc[2]}\\n')

# tworzymy jsona z polami dla hiercc_clustering_external_data i hiercc_historical_data
list_withphiercc_to_dump = []
with open('parsed_phiercc_enterobase.txt') as f1, open('enterobase.json', 'w') as f2:
    naglowek, wartosci = f1.readlines()
    for level_name, level_value in zip(naglowek.split("\\t")[1:], wartosci.split("\\t")[1:]):
        list_withphiercc_to_dump.append({"level": re.findall("\\d+", level_name.rstrip())[0],
                                         "group_id": level_value.rstrip()})

    to_dump = {"hiercc_clustering_external_data" : list_withphiercc_to_dump,
               "external_historical_data" : [{"level" : phiercc_level_userdefined,
                                            "data_file" : "${params.results_dir}/${x}/enterobase_historical_data.txt"}]}
    f2.write(json.dumps(to_dump))
"""
}

process plot_historical_data_enterobase {
  container  = params.main_image
  tag "Plot historical data for sample $x"
  cpus 1
  memory "5 GB"
  time "5m"
  input:
  tuple val(x), path('enterobase_historical_data.txt'), val(SPECIES), val(GENUS), val(QC_status), val(QC_status_contaminations)
  output:
  tuple val(x), path('enterobase_historical_data.html')
  // when:
  // GENUS == 'Salmonella' || GENUS == 'Escherichia'
  script:
// The script requires a geojeson file that is a part of our container 
// The 3 parameters are input file, output prefix, year fromwhich plot the data, data before that year are ignored (to save html size)
// Same options are used for pubmlst version of that script
"""
if [[ ${QC_status} == "nie"  || ${QC_status_contaminations} == "nie" ]]; then
    # Tworzenie json i output
    touch enterobase_historical_data.html
    
else
  if [[ ${GENUS} == "Salmonella" || ${GENUS} == "Escherichia" ]]; then
    python /data/plot_historical_data_plotly.py enterobase_historical_data.txt enterobase_historical_data 2009
  else
    touch enterobase_historical_data.html
    # json na zly gatunek
  fi
fi
"""

}