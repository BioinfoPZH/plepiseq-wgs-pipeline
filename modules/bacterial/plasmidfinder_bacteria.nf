process run_plasmidfinder {
  container  = params.main_image
  containerOptions "--volume ${params.db_absolute_path_on_host}:/db"
  cpus 1
  memory "5 GB"
  time "5m"
  tag "Predicting plasmids for sample $x"
  input:
  tuple val(x), path(fasta), val(QC_status), val(SPECIES), val(GENUS), val(QC_status_contaminations)
  output:
  tuple val(x), path('plasmidfinder/results_tab.tsv'), emit: to_pubdir
  tuple val(x), path('plasmidfinder.json'), emit: json
  
  // when:
  // GENUS == 'Salmonella' || GENUS == 'Escherichia' || GENUS == 'Campylobacter'
  script:
  """
  # -i to oczywiscie input na podstawie jego rozszerzenia program wybiera metode do analizy (kma dla fastq i blastn dla fasta)
  # -o to katalog z wynikami, musi istniec
  # -p to sciezka do katalog z bazami (tymi z repo plasmidfinder_db)
  # -l to minimalny procent sekwencji jaki musi alignowac sie na genom
  # -t to minimalne sequence identity miedzy query a subject
  # -x printuj dodatkowe dane w output (alignmenty)
  
  if [[ ${QC_status} == "nie"  || ${QC_status_contaminations} == "nie" ]]; then
    mkdir plasmidfinder; touch plasmidfinder/results_tab.tsv
    if [ "${params.lan}" == "pl" ]; then
      ERR_MSG="Ten moduł został uruchomiony na próbce, która nie przeszła kontroli jakości."
    else
      ERR_MSG="This module was eneterd with failed QC and poduced no valid output"
    fi
    
    python /opt/docker/EToKi/externals/plasmidfinder_parser.py  -i plasmidfinder/results_tab.tsv -s "nie" -r "\${ERR_MSG}" -o plasmidfinder.json
    # json zle QC
  else  
    if [[ ${GENUS} == "Salmonella" || ${GENUS} == "Escherichia" || ${GENUS} == "Campylobacter" ]]; then
      mkdir plasmidfinder # program wymaga tworzenia katalogu samodzielnie
      /opt/docker/plasmidfinder/plasmidfinder.py  -i $fasta -o plasmidfinder -p /db/plasmidfinder_db -l 0.6 -t 0.9 -x
      python /opt/docker/EToKi/externals/plasmidfinder_parser.py  -i plasmidfinder/results_tab.tsv -s "tak" -o plasmidfinder.json
    else
      mkdir plasmidfinder; touch plasmidfinder/results_tab.tsv
      if [ "${params.lan}" == "pl" ]; then
        ERR_MSG=`echo Ten modul jest przeznaczony do analizy bakterii z rodzajów: Salmonella, Escherichia oraz Campylobacter. W tej próbce wykryto: ${GENUS}`
      else
        ERR_MSG=`echo This module works with the following genera: Salmonella, Escherichia, or Campylobacter. Following genus was identified in this sample is: ${GENUS}`
      fi
      
      python /opt/docker/EToKi/externals/plasmidfinder_parser.py  -i plasmidfinder/results_tab.tsv -s "nie" -r "\${ERR_MSG}" -o plasmidfinder.json
      # json zly gatunek
    fi
  fi
  cp plasmidfinder/results_tab.tsv .
  """
}