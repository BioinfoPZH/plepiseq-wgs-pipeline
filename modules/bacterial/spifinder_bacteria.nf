process run_spifinder {
  // works only for salmonella
  container  = params.main_image
  containerOptions "--volume ${params.db_absolute_path_on_host}:/db"
  tag "Predicting virulence islands for sample $x"
  cpus 1
  memory "1 GB"
  time "5m"
  input:
  tuple val(x), path(fasta), val(QC_status), val(SPECIES), val(GENUS), val(QC_status_contaminations)
  output:
  tuple val(x), path('spifinder_results/results_tab.tsv'), emit: to_pubdir
  tuple val(x), path('spifinder.json'), emit: json
  // when:
  // GENUS == 'Salmonella'  
  script:
  """
  mkdir spifinder_results # program wymaga tworzenia katalogu samodzielnie
  # -mp opcja jak szukamy wysp kma jest gdy inputem sa dane surowe, blastn gdy podajemy zlozony genom/contigi
  # -p sciezka do bazy w external_databases
  # -l i -t to parametrty na alignment coverage i seq id
  # -x to rozszerzony output

  if [[ ${QC_status} == "nie"  || ${QC_status_contaminations} == "nie" ]]; then  
    touch spifinder_results/results_tab.tsv

    if [ "${params.lan}" == "pl" ]; then
      ERR_MSG="Ten moduł został uruchomiony na próbce, która nie przeszła kontroli jakości."
    else
      ERR_MSG="This module was eneterd with failed QC and poduced no valid output"
    fi

    python /opt/docker/EToKi/externals/spifinder_parser.py  -i spifinder_results/dummy_file.txt  -s "nie" -r "\${ERR_MSG}" -o spifinder.json
    # json na zle QC
  else
    if [ ${GENUS} == "Salmonella" ]; then
      python /opt/docker/spifinder/spifinder.py -i $fasta -o spifinder_results -mp blastn -p /db/spifinder_db/ -l 0.6 -t 0.9 -x
      python /opt/docker/EToKi/externals/spifinder_parser.py  -i spifinder_results/results_tab.tsv  -s "tak" -o spifinder.json
    else
      touch spifinder_results/results_tab.tsv
      # json na zly gatunek
      
      if [ "${params.lan}" == "pl" ]; then
        ERR_MSG=`echo Ten modul jest przeznaczony do analizy bakterii z rodzajów: Salmonella. W tej próbce wykryto: ${GENUS}`
      else
        ERR_MSG=`echo This module works with the following genera: Salmonella. Following genus was identified in this sample is: ${GENUS}`
      fi
      
      python /opt/docker/EToKi/externals/spifinder_parser.py  -i spifinder_results/dummy_file.txt  -s "nie" -r "\${ERR_MSG}" -o spifinder.json
    fi # koniec if-a na zly gatunek
  fi # koniec if-a na zle QC
  cp spifinder_results/results_tab.tsv .
  """
}