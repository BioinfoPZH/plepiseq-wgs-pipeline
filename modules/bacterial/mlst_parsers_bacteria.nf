

process parse_7MLST {
  // Process that extract a ST given a set of loci calculated with run_7MLST process
  // Process returns to 3 files 
  
  // // One MLST_parsed_output.txt with four columns (1st is ST identified for a sample, 2nd is the closest ST fromamong known ST in database. 
  // // 3nd is the distance between a sample allelic profile and allelic profile of closes matching ST, can be between
  // //  0 to 7, locus with no allelic varaiant are omitted); 4th column is a Comment
  // // If distance is 0 1st and 2nd column should be identical
  
  // // Second file MLST_sample_full_list_of_allels.txt contains eight columns (1st is the ST identified for a sample, followed by allel versions for each locus in a given scheme)
  
  // // Third file is MLST_closest_ST_full_list_of_allels.txt contains eight columns (1st is the ST identified for a sample, followed by allel versions for each locus of that ST)
  // // If the distance between between identified and expexted profiles is 0 (As indicated in MLST_parsed_output.txt) this file should be identical to MLST_sample_full_list_of_allels.txt

  container  = params.main_image
  containerOptions "--volume ${params.db_absolute_path_on_host}/mlst:/db"
  cpus 1
  memory "2 GB"
  time "2m"
  tag "Parsing MLST for sample $x"
  // maxForks 1 We will use delayed channel tactics to assure "novel" STs are correctly added to the file. THIS WILL NOT WORK WITH SLURM.
  input:
  tuple val(x), path('MLSTout.txt'), val(SPECIES), val(GENUS), val(QC_status), val(QC_status_contaminations) 
  output:
  tuple val(x), path('MLST_parsed_output.txt'), path('MLST_sample_full_list_of_allels.txt'), path('MLST_closest_ST_full_list_of_allels.txt'), emit: to_pubdir
  tuple val(x), path('MLST.json'), emit: json
  // when:
  // GENUS == 'Salmonella' || GENUS == 'Escherichia' || GENUS == 'Campylobacter'
  script:
"""
#!/usr/bin/python
import sys
import re
import json

sys.path.append('/data')
from all_functions_salmonella import *

species="$SPECIES"
genus="$GENUS"
qc_status="$QC_status"
qc_status_contaminations="$QC_status_contaminations"



### Determine correct paths to /db
### Prepare dummy output if provided SPECIES is not handled

if qc_status == "nie" or qc_status_contaminations == "nie":
    with open('MLST_parsed_output.txt', 'w') as f1, open('MLST_sample_full_list_of_allels.txt', 'w') as f2, open('MLST_closest_ST_full_list_of_allels.txt', 'w') as f3:
        f1.write(f'ST\\tComment\\n')
        f1.write(f'unk\\tUnknown species: {species}\\n')

        f2.write(f'ST\\tComment\\n')
        f2.write(f'unk\\tUnknownn species: {species}')

        f3.write(f'ST\\tComment\\n')
        f3.write(f'unk\\tUnknown species: {species}')
    # json na zle QC
    json_dict = {"scheme_name": "MLST", "status": qc_status, "error_message": "This module was eneterd with failed QC and poduced no valid output"}
    with open('MLST.json', "w") as f:
        f.write(json.dumps(json_dict))
    sys.exit(0)

if re.findall('Salmo', genus) or re.findall('Esch', genus):
    sciezka=f'/db/{genus}/'
elif species in ['concisus','fetus','helveticus','hyointestinalis','insulaenigrae','jejuni','lanienae','lari','sputorum','upsaliensis']:
    sciezka=f'/db/{genus}/{species}/'
else:
    # To nie powinno sie nigdy wykonac bo gatunek jest switchem na QC w module run+initial_mlst
    # Ale gdyby jednak sie wykonywalo to znaczy ze jest cos nie tak z pipeline
    with open('MLST_parsed_output.txt', 'w') as f1, open('MLST_sample_full_list_of_allels.txt', 'w') as f2, open('MLST_closest_ST_full_list_of_allels.txt', 'w') as f3:
        f1.write(f'ST\\tComment\\n')
        f1.write(f'unk\\tUnknown species: {species}\\n')
       
        f2.write(f'ST\\tComment\\n')
        f2.write(f'unk\\tUnknownn species: {species}')

        f3.write(f'ST\\tComment\\n')
        f3.write(f'unk\\tUnknown species: {species}')
    json_dict = {"scheme_name": "MLST", "status": "error", "error_message": "This module was eneterd with wrong species and poduced no valid output"}
    with open('MLST.json', "w") as f:
        f.write(json.dumps(json_dict))
    sys.exit(0)

### Deterine ST of out Sample
known_profiles, klucze_sorted = create_profile(f'{sciezka}/profiles.list')
identified_profile = parse_MLST_fasta('MLSTout.txt')
matching_ST, min_value, matching_ST_profile, sample_profile, all_loci_names = getST(MLSTout = identified_profile, \
                                                                              profile_file = f'{sciezka}/profiles.list')

# Struktura alleli zwracana do jsona
missing_loci_value = 0
list_to_dump = []
for loci_name, allele_value in zip(all_loci_names.split("\\t"), sample_profile.split("\\t")):
    list_to_dump.append({"locus_name": str(loci_name),
                         "locus_allel_id": int(allele_value)})
    if int(allele_value) == -1:
        missing_loci_value += 1
### matching_ST is a string
### matching_ST_profile, sample_profile, all_loci_names are all tab-separated strings
### min_valu is int

### Prep output

with open('MLST_closest_ST_full_list_of_allels.txt', 'w') as f3:
    f3.write(f'ST\\t{all_loci_names}\\n')
    f3.write(f'{matching_ST}\\t{matching_ST_profile}\\n')

with open('MLST_parsed_output.txt', 'w') as f1, open('MLST_sample_full_list_of_allels.txt', 'w') as f2:
    if min_value == 0:
        f1.write(f'ST_sample\\tST_database\\tDistance\\tComment\\n')
        f1.write(f'{matching_ST}\\t{matching_ST}\\t{min_value}\\tIn external database\\n')

        f2.write(f'ST\\t{all_loci_names}\\n')
        f2.write(f'{matching_ST}\\t{sample_profile}\\n')
        json_dict = {"scheme_name" : "MLST", 
                     "status" : "tak", 
                     "profile_id" : matching_ST,
                     "missing_allels_value": missing_loci_value,
                     "closest_external_profile_id" : matching_ST,
                     "closest_external_profile_distance" : min_value,  
                     "duplicated_allels_value" : 0,
                     "profile_allel_list" : list_to_dump,
                     "comment" : "In external database"}
        with open('MLST.json', "w") as f:
            f.write(json.dumps(json_dict))        

    else:
        # Unknown profile
        # Check if this profile is in a "local" database and if not create a new entry
        known_profiles_local, klucze_sorted_local = create_profile(f'{sciezka}/local/profiles_local.list')
        matching_ST_local, min_value_local, matching_ST_profile_local, _, _ =  getST(MLSTout = identified_profile, \
                                                                               profile_file = f'{sciezka}/local/profiles_local.list')
    
        if min_value_local == 0:
            # found profile in local database
            # we still print info what is the distance to known ST in external database
            f1.write(f'ST_sample\\tST_database\\tDistance\\tComment\\n')
            f1.write(f'{matching_ST_local}\\t{matching_ST}\\t{min_value}\\tIn local database\\n')

            f2.write(f'ST\\t{all_loci_names}\\n')
            f2.write(f'{matching_ST_local}\\t{sample_profile}\\n')
            json_dict = {"scheme_name" : "MLST",
                         "status" : "tak",
                         "profile_id" : matching_ST_local,
                         "closest_external_profile_id" : matching_ST,
                         "closest_external_profile_distance" : min_value,
                         "missing_allels_value": missing_loci_value,
                         "duplicated_allels_value" : 0,
                         "profile_allel_list" : list_to_dump,
                         "comment" : "In local database"}
            with open('MLST.json', "w") as f:
                f.write(json.dumps(json_dict))
        else:
            # new profile appending it to local database with new number  
            last_ST = 0
            with open(f'{sciezka}/local/profiles_local.list') as f:
                for line in f:
                    line = line.rsplit()
                    if 'local' in line[0]:
                        last_ST = line[0].split('_')[1]
            novel_profile_ST = f'local_{int(last_ST)+1}'

            write_novel_sample(f'{novel_profile_ST}\\t{sample_profile}\\n', f'{sciezka}/local/profiles_local.list')
            
            f1.write(f'ST_sample\\tST_database\\tDistance\\tComment\\n')
            f1.write(f'{novel_profile_ST}\\t{matching_ST}\\t{min_value}\\tNovel in local database\\n')

            f2.write(f'ST\\t{all_loci_names}\\n')
            f2.write(f'{novel_profile_ST}\\t{sample_profile}\\n')
            json_dict = {"scheme_name" : "MLST",
                         "status" : "tak",
                         "profile_id" : novel_profile_ST,
                         "closest_external_profile_id" : matching_ST,
                         "closest_external_profile_distance" : min_value,
                         "missing_allels_value": missing_loci_value,
                         "duplicated_allels_value" : 0,
                         "profile_allel_list" : list_to_dump,
                         "comment" : "Novel local database"}
            with open('MLST.json', "w") as f:
                f.write(json.dumps(json_dict))

"""
}




process parse_cgMLST {
  // Extracting ST given cgMLST profile, the output is identical to that from parse_7MLST
  container  = params.main_image
  containerOptions "--volume ${params.db_absolute_path_on_host}/cgmlst:/db"
  tag "Parsing cgMLST for sample $x"
  cpus 1
  memory "12 GB"
  time "40m"
  input:
  tuple val(x), path('cgMLST.txt'), path('cgMLST_all_identical_allels.txt'), val(SPECIES), val(GENUS), val(QC_status), val(QC_status_contaminations)
  output:
  tuple val(x), path('cgMLST_parsed_output.txt'), path('cgMLST_sample_full_list_of_allels.txt'), path('cgMLST_closest_ST_full_list_of_allels.txt'), val(SPECIES), val(GENUS), val(QC_status), val(QC_status_contaminations), emit: standard
  path('cgMLST_all_identical_allels.txt'), emit: to_pubdir
  tuple val(x), path('cgMLST_part1.json'), emit: json 
  // when:
  // GENUS == 'Salmonella' || GENUS == 'Escherichia' || SPECIES == 'jejuni'
  script:
"""
#!/usr/bin/python
import  sys
import re
import json

sys.path.append('/data')
from all_functions_salmonella import *

species="$SPECIES"
genus="$GENUS"
qc_status="$QC_status"
qc_status_contaminations="$QC_status_contaminations"


if qc_status == "nie" or qc_status_contaminations == "nie":
    with open('cgMLST_parsed_output.txt', 'w') as f1, open('cgMLST_sample_full_list_of_allels.txt', 'w') as f2, open('cgMLST_closest_ST_full_list_of_allels.txt', 'w') as f3:
        f1.write(f'ST\\tComment\\n')
        f1.write(f'unk\\tUnknown species: {species}\\n')

        f2.write(f'ST\\tComment\\n')
        f2.write(f'unk\\tUnknownn species: {species}')

        f3.write(f'ST\\tComment\\n')
        f3.write(f'unk\\tUnknown species: {species}')
    json_dict = {"scheme_name": "cgMLST", "status": qc_status, "error_message": "This module was eneterd with failed QC and poduced no valid output"}
    with open('cgMLST_part1.json', "w") as f:
        f.write(json.dumps(json_dict))
    sys.exit(0)


if genus == 'Salmonella':
    sciezka=f'/db/{genus}/'
elif genus == 'Escherichia':
    sciezka=f'/db/{genus}/'
elif species == 'jejuni':
    sciezka=f'/db/{genus}/jejuni/'
else:
    # W odroznieniu od MLST to moze sie stac, bo mamy cgMLST tylko dla C.jejuni/coli
    with open('cgMLST_parsed_output.txt', 'w') as f1, open('cgMLST_sample_full_list_of_allels.txt', 'w') as f2, open('cgMLST_closest_ST_full_list_of_allels.txt', 'w') as f3:
        f1.write(f'ST\\tComment\\n')
        f1.write(f'unk\\tUnknown species: {species}\\n')

        f2.write(f'ST\\tComment\\n')
        f2.write(f'unk\\tUnknownn species: {species}')

        f3.write(f'ST\\tComment\\n')
        f3.write(f'unk\\tUnknown species: {species}')
    # json na zly gatunek
    json_dict = {"scheme_name": "cgMLST", "status": "error", "error_message": "This module was eneterd with wrong species and poduced no valid output"}
    with open('cgMLST_part1.json', "w") as f:
        f.write(json.dumps(json_dict))
    sys.exit(0) 

identified_profile = parse_MLST_blastn('cgMLST.txt')

matching_ST, min_value,  matching_ST_profile, sample_profile, all_loci_names = getST(MLSTout = identified_profile, \
                                                                                     profile_file = f'{sciezka}/profiles.list')

list_to_dump = []
missing_loci_value = 0
for loci_name, allele_value in zip(all_loci_names.split("\\t"), sample_profile.split("\\t")):
    list_to_dump.append({"locus_name": str(loci_name),
                         "locus_allel_id": int(allele_value)})
    if int(allele_value) == -1:
        missing_loci_value += 1

# Extract duplicated loci
duplicated_loci_value = 0
with open('cgMLST_all_identical_allels.txt') as f:
    for line in f:
        line = line.split("\\t")
        locus_name, allel_number = line[0], int(line[1].rstrip())
        if allel_number > 1:
            duplicated_loci_value += 1

# Extract number of loci with no hits 

      

# Write info regarding closest ST from EXTERNAL database
with open('cgMLST_closest_ST_full_list_of_allels.txt', 'w') as f3:
    f3.write(f'ST\\t{all_loci_names}\\n')
    f3.write(f'{matching_ST}\\t{matching_ST_profile}\\n')

with open('cgMLST_parsed_output.txt', 'w') as f1, open('cgMLST_sample_full_list_of_allels.txt', 'w') as f2:

    # for f1 and f2 the only difference is ST assigned to the sample
        
    if min_value == 0:
        f1.write(f'ST_sample\\tST_database\\tDistance\\tComment\\n')
        f1.write(f'{matching_ST}\\t{matching_ST}\\t{min_value}\\tIn external database\\n')

        f2.write(f'ST\\t{all_loci_names}\\n')
        f2.write(f'{matching_ST}\\t{sample_profile}\\n')
        json_dict = {"scheme_name" : "cgMLST",
                     "status" : "tak",
                     "profile_id" : matching_ST,
                     "missing_allels_value": missing_loci_value,
                     "closest_external_profile_id" : matching_ST,
                     "closest_external_profile_distance" : min_value,
                     "duplicated_allels_value" : duplicated_loci_value,
                     "profile_allel_list" : list_to_dump}
         
        with open('cgMLST_part1.json', "w") as f:
            f.write(json.dumps(json_dict))

   
    else:
        # look for a profile in "local" database 
        matching_ST_local, min_value_local, matching_ST_profile_local, _, _ = getST(MLSTout = identified_profile,
                                                                                    profile_file = f'{sciezka}/local/profiles_local.list')
        if min_value_local == 0:
            f1.write(f'ST_sample\\tST_database\\tDistance\\tComment\\n')
            f1.write(f'{matching_ST_local}\\t{matching_ST}\\t{min_value}\\tIn local database\\n')

            f2.write(f'ST\\t{all_loci_names}\\n')
            f2.write(f'{matching_ST_local}\\t{sample_profile}\\n')
            json_dict = {"scheme_name" : "cgMLST",
                         "status" : "tak",
                         "profile_id" : matching_ST_local,
                         "missing_allels_value": missing_loci_value,
                         "closest_external_profile_id" : matching_ST,
                         "closest_external_profile_distance" : min_value,
                         "duplicated_allels_value" : duplicated_loci_value,
                         "profile_allel_list" : list_to_dump,
                         "comment" : "In local database"}

            with open('cgMLST_part1.json', "w") as f:
                f.write(json.dumps(json_dict))

        else:
            # new allelic profile not present in either external or local databases
            last_ST = 0
            with open(f'{sciezka}/local/profiles_local.list') as f:
                for line in f:
                    line = line.rsplit()
                    if 'local' in line[0]:
                        last_ST = line[0].split('_')[1]
        
        
            novel_profile_ST = f'local_{int(last_ST) + 1}'
            
            write_novel_sample(f'{novel_profile_ST}\\t{sample_profile}\\n', f'{sciezka}/local/profiles_local.list')
        
            f1.write(f'ST_sample\\tST_database\\tDistance\\tComment\\n')
            f1.write(f'{novel_profile_ST}\\t{matching_ST}\\t{min_value}\\tNovel in local database\\n')

            f2.write(f'ST\\t{all_loci_names}\\n')
            f2.write(f'{novel_profile_ST}\\t{sample_profile}\\n')
            json_dict = {"scheme_name" : "cgMLST",
                         "status" : "tak",
                         "profile_id" : novel_profile_ST,
                         "missing_allels_value": missing_loci_value,
                         "closest_external_profile_id" : matching_ST,
                         "closest_external_profile_distance" : min_value,
                         "duplicated_allels_value" : duplicated_loci_value,
                         "profile_allel_list" : list_to_dump,
                         "comment" : "Novel in local database"}

            with open('cgMLST_part1.json', "w") as f:
                f.write(json.dumps(json_dict))

"""
}