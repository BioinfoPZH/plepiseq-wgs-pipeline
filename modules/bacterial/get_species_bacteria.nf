process get_species_illumina {
// Process laczy ouputy predykcji z krakena2, metaphlan i kmerfindera
container  = params.main_image
cpus 1
memory "1 GB"
time "5m"
tag "Predicting species for ${x}"
input:
tuple val(x), path('report_kraken2.txt'), path('report_kraken2_individualreads.txt'), path('Summary_kraken_genera.txt'), path('Summary_kraken_species.txt'), path('report_metaphlan_SGB.txt'), path('report_metaphlan_species.txt'), path('report_metaphlan_genera.txt'),  path('results.spa'), path('results.txt'), val(KMERFINDER_SPECIES), val(KMERFINDER_GENUS), val(QC_STATUS), val(TOTAL_BASES)

output:
tuple val(x), env(FINALE_SPECIES), env(FINAL_GENUS), env(QC_status_contaminations), emit: species_and_qcstatus
path('predicted_genus_and_species.txt'), emit: to_pubdir 
tuple val(x), path('contaminations.json'), path('Genus_species.json'), emit: json
tuple val(x), env(QC_status_contaminations), emit: qcstatus_only
tuple val(x), env(FINAL_GENUS), emit: genus_only

script:
"""
if [ ${QC_STATUS} == "nie" ]; then
  QC_status_contaminations="nie"
  FINALE_SPECIES="unknown"
  FINAL_GENUS="unknown"
  echo "This module was eneterd with failed QC and poduced no valid output" >> predicted_genus_and_species.txt
  # contaminations json
  python /opt/docker/EToKi/externals/json_output_contaminations.py -k report_kraken2.txt -g report_metaphlan_genera.txt -x report_metaphlan_species.txt -y results.txt -s nie -m "Species prediction module recieved incorrect data" -o contaminations.json
  echo -e "{\\"pathogen_predicted_genus\\":\\"\${FINAL_GENUS}\\",
            \\"pathogen_predicted_species\\":\\"\${FINALE_SPECIES}\\"}" >> Genus_species.json

else 
  QC_status_contaminations="tak"
  PRE_FINALE_SPECIES=""
  cat report_kraken2.txt | grep -w "S" | sort -rnk1 | head -1 | awk '{print \$6,\$7}' >> intermediate.txt
  grep -v '^#' report_metaphlan_species.txt \
  | awk '\$1!="UNCLASSIFIED"{print \$1"\\t"\$3}' \
  | sort -rnk2 \
  | head -1 \
  | awk '{
    sub(/.*\\|/, "", \$1)   
    sub(/^s__/, "", \$1)   
    gsub(/_/, " ", \$1)
    print \$1}' >> intermediate.txt
  echo ${KMERFINDER_SPECIES} | cut -d ' ' -f1-2 >> intermediate.txt

  KRAKEN_GENUS_LEVEL=`cat report_kraken2.txt | grep -w "S" | sort -rnk1 | head -1 | awk '{print int(\$1)}'`
  METAPHLAN_GENUS_LEVEL=`cat report_metaphlan_species.txt  | grep -v "#" | sort -rnk3 | head -1 | awk '{print int(\$3)}'`
  KMERFINDER_COVERAGE=`cat results.txt | head -2 |  tail -1 | cut -f9 | awk '{print int (\$1)}'`

  if [[ \${KRAKEN_GENUS_LEVEL} -lt ${params.main_genus_value} && \${METAPHLAN_GENUS_LEVEL} -lt ${params.main_genus_value} && \${KMERFINDER_COVERAGE} -lt ${params.kmerfinder_coverage} ]]; then
    # Pipeline nie wykrywa gatunku jesli:
    # kraken2 i metaphlan zwracaja ponziej 50% odczytow nalezacych do glownego gatunku
    # i kmerfinder zwraca pokrycie pierwszego gatunku ponizej 20
    # jestemy tu bardzo liberalni co akceptujemy do pipeline'u 
    QC_status_contaminations="nie"
    FINALE_SPECIES="unknown"
    FINAL_GENUS="unknown"
    echo -e "The sample is contaminated or lacks sufficient number of reads" >> predicted_genus_and_species.txt

    if [ "${params.lan}" == "pl" ]; then
      ERROR_MSG=`echo Ta próbka nie przeszła podstawowej kontroli jakości w tym module; ilosc odczytow dla dominujacego rodzaju to "\${KRAKEN_GENUS_LEVEL}" wedlug programu kraken2, a "\${METAPHLAN_GENUS_LEVEL}" wedlug programu metaphlan. Przewidywane pokrycie głównego gatunku wedlug programu kmerfinder wynosi "\${KMERFINDER_COVERAGE}"`
    else
      ERROR_MSG=`echo This sample fails basic QC for this module. Number of reads associated with a dominant genus is "\${KRAKEN_GENUS_LEVEL}" according to kraken2, and "\${METAPHLAN_GENUS_LEVEL}" according to metaphlan. Predicted coverage for the main species according to kmerfinder is "\${KMERFINDER_COVERAGE}"`
    fi


    python /opt/docker/EToKi/externals/json_output_contaminations.py -k report_kraken2.txt -g report_metaphlan_genera.txt -x report_metaphlan_species.txt -y results.txt -s blad -m "\${ERROR_MSG}" -o contaminations.json
    echo -e "{\\"pathogen_predicted_genus\\":\\"\${FINAL_GENUS}\\",
            \\"pathogen_predicted_species\\":\\"\${FINALE_SPECIES}\\"}" >> Genus_species.json
  else
    PRE_FINALE_SPECIES=`cat intermediate.txt | sed -E 's/[[:space:]]+\$//' | sort | uniq -c | tr -s " " | sort -rnk1 | head -1 | cut -d " " -f3,4`

    if [[ "\${PRE_FINALE_SPECIES}" == *"Salmonel"* ]]; then
      FINALE_SPECIES="\${PRE_FINALE_SPECIES}"
      GENOME_SIZE=5400000
    elif [[ "\${PRE_FINALE_SPECIES}" == *"Escher"* || "\${PRE_FINALE_SPECIES}" == *"Shigella"* ]]; then
      FINALE_SPECIES="Escherichia coli"
      GENOME_SIZE=4800000
    elif [[ "\${PRE_FINALE_SPECIES}" == "Campylobacter coli" ]]; then
      FINALE_SPECIES="jejuni"
      GENOME_SIZE=1800000
    elif [[ "\${PRE_FINALE_SPECIES}" == "Campylobacter jejuni" ]]; then
      FINALE_SPECIES="jejuni"
      GENOME_SIZE=1800000
    elif [[ "\${PRE_FINALE_SPECIES}" == "Campylobacter concisus" ]]; then
      FINALE_SPECIES="concisus"
      GENOME_SIZE=1800000
    elif [[ "\${PRE_FINALE_SPECIES}" == "Campylobacter curvus" ]]; then
      FINALE_SPECIES="concisus"
      GENOME_SIZE=1800000
    elif [[ "\${PRE_FINALE_SPECIES}" == "Campylobacter fetus" ]]; then
      FINALE_SPECIES="fetus"
      GENOME_SIZE=1800000
    elif [[ "\${PRE_FINALE_SPECIES}" == "Campylobacter helveticus" ]]; then
      FINALE_SPECIES="helveticus"
      GENOME_SIZE=1800000
    elif [[ "\${PRE_FINALE_SPECIES}" == "Campylobacter hyointestinalis" ]]; then
      FINALE_SPECIES="hyointestinalis"
      GENOME_SIZE=1800000
    elif [[ "\${PRE_FINALE_SPECIES}" == "Campylobacter insulaenigrae" ]]; then
      FINALE_SPECIES="insulaenigrae"
      GENOME_SIZE=1800000
    elif [[ "\${PRE_FINALE_SPECIES}" == "Campylobacter lanienae" ]]; then
      FINALE_SPECIES="lanienae"
      GENOME_SIZE=1800000
    elif [[ "\${PRE_FINALE_SPECIES}" == "Campylobacter lari" ]]; then
      FINALE_SPECIES="lari"
      GENOME_SIZE=1800000
    elif [[ "\${PRE_FINALE_SPECIES}" == "Campylobacter sputorum" ]]; then
      FINALE_SPECIES="sputorum"
      GENOME_SIZE=1800000
    elif [[ "\${PRE_FINALE_SPECIES}" == "Campylobacter upsaliensis" ]]; then
      FINALE_SPECIES="upsaliensis"
      GENOME_SIZE=1800000
    else
      FINALE_SPECIES="\${PRE_FINALE_SPECIES}"
      GENOME_SIZE=6000000
    fi

    cat report_kraken2.txt | grep -w "G" | sort -rnk1 | head -1 | awk '{print \$6,\$7}'  >> intermediate_genus.txt
    # cat report_metaphlan_genera.txt  | grep -v "#" | sort -rnk3 | head -1 | awk '{print \$1" "}' | sed s'/g__//'g | sed s'/_/ /'g >> intermediate_genus.txt
    
    grep -v '^#' report_metaphlan_genera.txt \
    | awk '\$1!="UNCLASSIFIED"{print \$1"\\t"\$3}' \
    | sort -rnk2 \
    | head -1 \
    | awk '{
    sub(/.*\\|/, "", \$1)   
    sub(/^g__/, "", \$1)   
    sub(/_/, " ", \$1)   
    gsub(/_/, " ", \$1)
    print \$1}' >> intermediate_genus.txt

    echo -e "${KMERFINDER_GENUS} " >> intermediate_genus.txt

    #Ecoli and Schigella are the same thing
    FINAL_GENUS=`cat intermediate_genus.txt | sed s'/Shigella/Escherichia/'g | sed -E 's/[[:space:]]+\$//'  | sort | uniq -c | tr -s " " | sort -rnk1 | head -1 | cut -d " " -f3`
    echo -e "Final genus:\t\${FINAL_GENUS}\nFinal species:\t\${FINALE_SPECIES}" >> predicted_genus_and_species.txt

    echo -e "{\\"pathogen_predicted_genus\\":\\"\${FINAL_GENUS}\\",
            \\"pathogen_predicted_species\\":\\"\${FINALE_SPECIES}\\"}" >> Genus_species.json

    # ostani przelacnik liczba zasad to co najmniej 30x dlugosc "oczekiwanego" genomu
    TEORETICAL_COVERAGE=`awk -v g_size="\${GENOME_SIZE}" -v t_bases="${TOTAL_BASES}" 'BEGIN {print t_bases/g_size}'`
    if [ `awk -v tot_cov=\${TEORETICAL_COVERAGE} -v exp_cov=${params.main_species_coverage} 'BEGIN {if(tot_cov > exp_cov) {print 1} else {print 0}}'` -eq 1 ]; then
      # Tu jestesmy lagodniejsi, ostateczna wartosc sredniego pokrycia uzyjemy dopiero przy skladaniu genomu
      # aLe przy liczbie zasad mniejszej niz 20 x teoretycznego pokrycia nawet nie ma co probowac
      QC_status_contaminations="tak"
      python /opt/docker/EToKi/externals/json_output_contaminations.py -k report_kraken2.txt -g report_metaphlan_genera.txt -x report_metaphlan_species.txt -y results.txt -s tak -o contaminations.json
    else

        if [ "${params.lan}" == "pl" ]; then
          ERROR_MSG="Ta próbka nie przeszła podstawowej kontroli jakości w tym module. Przewidywane teoretyczne pokrycie \${TEORETICAL_COVERAGE} jest poniżej progu ${params.main_species_coverage}"
        else   
          ERROR_MSG="This sample fails basic QC for this module. Predicted theoretical coverage \${TEORETICAL_COVERAGE} is below threshold ${params.main_species_coverage}"
        fi

      python /opt/docker/EToKi/externals/json_output_contaminations.py -k report_kraken2.txt -g report_metaphlan_genera.txt -x report_metaphlan_species.txt -y results.txt -s blad -m "\${ERROR_MSG}" -o contaminations.json
      QC_status_contaminations="nie"
    fi
  fi
fi
"""
}


process get_species_nanopore {
  // Process laczy ouputy predykcji z krakena2, metaphlan i kmerfindera
  container  = params.main_image
  cpus 1
  memory "1 GB"
  time "5m"
  tag "Predicting species for ${x}"
  input:
  tuple val(x), path('report_kraken2.txt'), path('report_kraken2_individualreads.txt'), path('Summary_kraken_genera.txt'), path('Summary_kraken_species.txt'),  path('results.spa'), path('results.txt'), val(KMERFINDER_SPECIES), val(KMERFINDER_GENUS), val(QC_STATUS), val(TOTAL_BASES)

  output:
  tuple val(x), env(FINALE_SPECIES), env(FINAL_GENUS), env(QC_status_contaminations), emit: species_and_qcstatus
  path('predicted_genus_and_species.txt'), emit: to_pubdir
  tuple val(x), path('contaminations.json'), path('Genus_species.json'), emit: json
  tuple val(x), env(QC_status_contaminations), emit: qcstatus_only
  tuple val(x), env(FINAL_GENUS), emit:genus_only
  script:
"""
if [ ${QC_STATUS} == "nie" ]; then
  QC_status_contaminations="nie"
  FINALE_SPECIES="unknown"
  FINAL_GENUS="unknown"
  echo "This module was eneterd with failed QC and poduced no valid output" >> predicted_genus_and_species.txt
  python /opt/docker/EToKi/externals/json_output_contaminations.py -k report_kraken2.txt -g skip -x skip -y results.txt -s nie -m "Species prediction module recieved incorrect data" -o contaminations.json
   echo -e "{\\"pathogen_predicted_genus\\":\\"\${FINAL_GENUS}\\",
            \\"pathogen_predicted_species\\":\\"\${FINALE_SPECIES}\\"}" >> Genus_species.json
else
  QC_status_contaminations="tak"
  PRE_FINALE_SPECIES=""
  cat report_kraken2.txt | grep -w "S" | sort -rnk1 | head -1 | awk '{print \$6,\$7}' >> intermediate.txt
  echo ${KMERFINDER_SPECIES} | cut -d ' ' -f1-2  >> intermediate.txt
  
  KRAKEN_GENUS_LEVEL=`cat report_kraken2.txt | grep -w "S" | sort -rnk1 | head -1 | awk '{print int(\$1)}'`
  KMERFINDER_COVERAGE=`cat results.txt | head -2 |  tail -1 | cut -f9 | awk '{print int (\$1)}'`

  if [[ \${KRAKEN_GENUS_LEVEL} -lt $params.main_genus_value} && \${KMERFINDER_COVERAGE} -lt ${params.kmerfinder_coverage} ]]; then
    # kraken2 zwraca mniej nizd 50% odczytow nalezacych do glownego gatunku
    # kmerfinder zwraca pokrycie pierwszego gatunku ponizej 20
    QC_status_contaminations="nie"
    FINALE_SPECIES="unknown"
    FINAL_GENUS="unknown"
    echo -e "The sample is contaminated or lacks sufficient number of reads" >> predicted_genus_and_species.txt

    if [ "${params.lan}" == "pl" ]; then
      ERROR_MSG='Ta próbka nie przeszła podstawowej kontroli jakości w tym module. Liczba odczytów przypisana do dominującego rodzaju to \${KRAKEN_GENUS_LEVEL} według programu kraken2, a przewidywane pokrycie głównego gatunku według programu kmerfinder wynosi \${KMERFINDER_COVERAGE}'
    else
      ERROR_MSG=`echo This sample fails basic QC for this module. Number of reads associated with a dominant genus is \${KRAKEN_GENUS_LEVEL} according to kraken2 and predicted coverage for the main species according to kmerfinder is \${KMERFINDER_COVERAGE}`
    fi    

    python /opt/docker/EToKi/externals/json_output_contaminations.py -k report_kraken2.txt -g skip -x skip -y results.txt -s blad -m "\${ERROR_MSG}" -o contaminations.json
    echo -e "{\\"pathogen_predicted_genus\\":\\"\${FINAL_GENUS}\\",
            \\"pathogen_predicted_species\\":\\"\${FINALE_SPECIES}\\"}" >> Genus_species.json

  else
    PRE_FINALE_SPECIES=`cat intermediate.txt | sort | uniq -c | tr -s " " | sort -rnk1 | head -1 | cut -d " " -f3,4`

    if [[ "\${PRE_FINALE_SPECIES}" == *"Salmonel"* ]]; then
      FINALE_SPECIES="\${PRE_FINALE_SPECIES}"
      GENOME_SIZE=5400000
    elif [[ "\${PRE_FINALE_SPECIES}" == *"Escher"* || "\${PRE_FINALE_SPECIES}" == *"Shigella"* ]]; then
      FINALE_SPECIES="Escherichia coli"
      GENOME_SIZE=4800000
    elif [[ "\${PRE_FINALE_SPECIES}" == "Campylobacter coli" ]]; then
      FINALE_SPECIES="jejuni"
      GENOME_SIZE=1800000
    elif [[ "\${PRE_FINALE_SPECIES}" == "Campylobacter jejuni" ]]; then
      FINALE_SPECIES="jejuni"
      GENOME_SIZE=1800000
    elif [[ "\${PRE_FINALE_SPECIES}" == "Campylobacter concisus" ]]; then
      FINALE_SPECIES="concisus"
      GENOME_SIZE=1800000
    elif [[ "\${PRE_FINALE_SPECIES}" == "Campylobacter curvus" ]]; then
      FINALE_SPECIES="concisus"
      GENOME_SIZE=1800000
    elif [[ "\${PRE_FINALE_SPECIES}" == "Campylobacter fetus" ]]; then
      FINALE_SPECIES="fetus"
      GENOME_SIZE=1800000
    elif [[ "\${PRE_FINALE_SPECIES}" == "Campylobacter helveticus" ]]; then
      FINALE_SPECIES="helveticus"
      GENOME_SIZE=1800000
    elif [[ "\${PRE_FINALE_SPECIES}" == "Campylobacter hyointestinalis" ]]; then
      FINALE_SPECIES="hyointestinalis"
      GENOME_SIZE=1800000
    elif [[ "\${PRE_FINALE_SPECIES}" == "Campylobacter insulaenigrae" ]]; then
      FINALE_SPECIES="insulaenigrae"
      GENOME_SIZE=1800000
    elif [[ "\${PRE_FINALE_SPECIES}" == "Campylobacter lanienae" ]]; then
      FINALE_SPECIES="lanienae"
      GENOME_SIZE=1800000
    elif [[ "\${PRE_FINALE_SPECIES}" == "Campylobacter lari" ]]; then
      FINALE_SPECIES="lari"
      GENOME_SIZE=1800000
    elif [[ "\${PRE_FINALE_SPECIES}" == "Campylobacter sputorum" ]]; then
      FINALE_SPECIES="sputorum"
      GENOME_SIZE=1800000
    elif [[ "\${PRE_FINALE_SPECIES}" == "Campylobacter upsaliensis" ]]; then
      FINALE_SPECIES="upsaliensis"
      GENOME_SIZE=1800000
    else
      FINALE_SPECIES="\${PRE_FINALE_SPECIES}"
      GENOME_SIZE=6000000
    fi

    cat report_kraken2.txt | grep -w "G" | sort -rnk1 | head -1 | awk '{print \$6,\$7}' >> intermediate_genus.txt
    echo -e "${KMERFINDER_GENUS} " >> intermediate_genus.txt

    FINAL_GENUS=`cat intermediate_genus.txt | sort | uniq -c | tr -s " " | sort -rnk1 | head -1 | cut -d " " -f3`

    echo -e "Final genus:\t\${FINAL_GENUS}\nFinal species:\t\${FINALE_SPECIES}" >> predicted_genus_and_species.txt
    echo -e "{\\"pathogen_predicted_genus\\":\\"\${FINAL_GENUS}\\",
            \\"pathogen_predicted_species\\":\\"\${FINALE_SPECIES}\\"}" >> Genus_species.json

    TEORETICAL_COVERAGE=`awk -v g_size="\${GENOME_SIZE}" -v t_bases="${TOTAL_BASES}" 'BEGIN {print t_bases/g_size}'`
    if [ `awk -v tot_cov=\${TEORETICAL_COVERAGE} -v exp_cov=${params.main_species_coverage} 'BEGIN {if(tot_cov > exp_cov) {print 1} else {print 0}}'` -eq 1 ]; then
      QC_status_contaminations="tak"
      python /opt/docker/EToKi/externals/json_output_contaminations.py -k report_kraken2.txt -g skip -x skip -y results.txt -s tak -o contaminations.json
    else
      QC_status_contaminations="nie"
      
      if [ "${params.lan}" == "pl" ]; then
        ERROR_MSG="Ta próbka nie przeszła podstawowej kontroli jakości w tym module. Przewidywane teoretyczne pokrycie \${TEORETICAL_COVERAGE} jest poniżej progu ${params.main_species_coverage}"
      else
        ERROR_MSG="This sample fails basic QC for this module. Predicted theoretical coverage \${TEORETICAL_COVERAGE} is below threshold ${params.main_species_coverage}"
      fi

      python /opt/docker/EToKi/externals/json_output_contaminations.py -k report_kraken2.txt -g skip -x skip -y results.txt -s blad -m "\${ERROR_MSG}" -o contaminations.json
    fi
  fi
fi

"""
}