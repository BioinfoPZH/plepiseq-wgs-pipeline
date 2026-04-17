process run_7MLST {
  container  = params.main_image
  containerOptions "--volume ${params.db_absolute_path_on_host}/mlst:/db"
  tag "Predicting MLST for sample $x"
  cpus 1
  memory "10 GB"
  time "5m"
  input:
  tuple val(x), path(fasta), val(QC_status), val(SPECIES), val(GENUS), val(QC_status_contaminations)
  output:
  tuple val(x), path('MLSTout.txt'), val(SPECIES), val(GENUS), val(QC_status), val(QC_status_contaminations)
  // when:
  // GENUS == 'Salmonella' || GENUS == 'Escherichia' || GENUS == 'Campylobacter'
  script:
  """
  if [[ ${QC_status} == "nie"  || ${QC_status_contaminations} == "nie" ]]; then
    touch MLSTout.txt
    # json na zle QC
  else
    CAMPYLO_SPECIES='concisus fetus helveticus hyointestinalis insulaenigrae jejuni lanienae lari sputorum upsaliensis'
    if [[ "${SPECIES}" == *"Salmo"* || "${SPECIES}" == *"Escher"* ]]; then
      /opt/docker/EToKi/EToKi.py MLSTdb -i /db/${GENUS}/all_allels.fasta -x 0.8 -m 0.5 -r MLST_Achtman_ref.fasta -d MLST_database.tab
      /opt/docker/EToKi/EToKi.py MLSType -i $fasta -r MLST_Achtman_ref.fasta -k ${x} -o MLSTout.txt -d MLST_database.tab
    elif [[ \${CAMPYLO_SPECIES[@]} =~ "${SPECIES}" ]]; then
      # sciezka dla Campylobacter
      /opt/docker/EToKi/EToKi.py MLSTdb -i /db/${GENUS}/${SPECIES}/all_allels.fasta -x 0.8 -m 0.5 -r MLST_Achtman_ref.fasta -d MLST_database.tab
     /opt/docker/EToKi/EToKi.py MLSType -i $fasta -r MLST_Achtman_ref.fasta -k ${x} -o MLSTout.txt -d MLST_database.tab
    else
       echo "Provided species ${SPECIES} is not part of any MLST databases" >> MLSTout.txt
       # json na zly gatunek
    fi # koniec if-a na zly gatunek
  fi # koniec if-a na zle QC
  """
}



process run_cgMLST {
  container  = params.main_image
  containerOptions "--volume ${params.db_absolute_path_on_host}/cgmlst:/db"
  tag "Predicting cgMLST for sample $x"
  cpus params.threads
  memory "40 GB"
  time "40m"
  input:
  tuple val(x), path(fasta), val(QC_status), val(SPECIES), val(GENUS), val(QC_status_contaminations)
  output: 
  tuple val(x), path('cgMLST.txt'), path('cgMLST_all_identical_allels.txt'), val(SPECIES), val(GENUS), val(QC_status), val(QC_status_contaminations)
  // when:
  // GENUS == 'Salmonella' || GENUS == 'Escherichia' || SPECIES == 'jejuni'
  // among Campylobacter only c.jejuni has cgMLST scheme
  script:
  """
  if [[ ${QC_status} == "nie"  || ${QC_status_contaminations} == "nie" ]]; then
    touch cgMLST.txt; touch cgMLST_all_identical_allels.txt
    # json dla zlego QC
  else
    if [[ "${GENUS}" == *"Salmo"* ]]; then 
         /data/run_blastn_ver11.sh $fasta ${task.cpus} /db/${GENUS}/
    elif [[ "${GENUS}" == *"Esche"* ]]; then
         /data/run_blastn_ver11.sh $fasta ${task.cpus} /db/${GENUS}/
    elif [ "${SPECIES}" == "jejuni" ]; then
         /data/run_blastn_ver11.sh $fasta ${task.cpus} /db/${GENUS}/jejuni/
    else
         # This should never happen as earlier modules should always switch QC_status to "nie" 
         echo "Provided species $SPECIES is not part of any cgMLST databases" >> log.log
    fi # koniec if-a na zly gatunek
    cat log.log | cut -f1,2 > cgMLST.txt
  fi # koniec if-a na zle QC
  """
}