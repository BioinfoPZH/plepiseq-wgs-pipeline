process run_virulencefinder {
  container  = params.main_image
  containerOptions "--volume ${params.db_absolute_path_on_host}:/db"
  cpus 1
  memory "5 GB" 
  time "5m"
  tag "Predicting VirulenceFactors with run_virulencefinder for sample $x"
  input:
  tuple val(x), path(fasta),  val(QC_status), val(SPECIES), val(GENUS), val(QC_status_contaminations)
  output:
  tuple val(x), path('results_tab.tsv')
  tuple val(x), path('virulencefinder.json'), emit: json
  // when:
  // GENUS == 'Escherichia'
  script:
  """
  # -i to oczywiscie input na podstawie jego rozszerzenia program wybiera metode do analizy (kma dla fastq i blastn dla fasta)
  # -o to katalog z wynikami, musi istniec
  # -p to sciezka do katalog z bazami (katalog z repo virulencefinder_db)
  # -l to minimalny procent sekwencji jaki musi alignowac sie na genom
  # -t to minimalne sequence identity miedzy query a subject
  # -d to nazwa bazy/organizmu
  # -x printuj dodatkowe dane w output (alignmenty)
  # nie wiem czy to kwestia niezgodnosci mojego virulencefindera z bazami
  # ale program printuje mase output (choc tworzy poprawne pliki w koncu to parser blasta)
  mkdir virulencefinder # program wymaga tworzenia katalogu samodzielnie

  if [[ ${QC_status} == "nie"  || ${QC_status_contaminations} == "nie" ]]; then
    touch results_tab.tsv
    if [ "${params.lan}" == "pl" ]; then
      ERR_MSG="Ten moduł został uruchomiony na próbce, która nie przeszła kontroli jakości."
    else
      ERR_MSG="This module was eneterd with failed QC and poduced no valid output"
    fi

    python /opt/docker/EToKi/externals/virulencefinder_parser.py  -i  results_tab.tsv -s "nie" -r "\${ERR_MSG}" -o virulencefinder.json
    # json na zle QC
  else
    if [ ${GENUS} == "Escherichia" ]; then
      /opt/docker/virulencefinder/virulencefinder.py  -i $fasta -o virulencefinder -p /db/virulencefinder_db  -d virulence_ecoli -l 0.6 -t 0.9 -x >> log 2>&1
      cp virulencefinder/results_tab.tsv .
      python /opt/docker/EToKi/externals/virulencefinder_parser.py  -i results_tab.tsv -s "tak" -o virulencefinder.json
    else
      touch results_tab.tsv
      if [ "${params.lan}" == "pl" ]; then
        ERR_MSG=`echo Ten modul jest przeznaczony do analizy bakterii z rodzajów: Escherichia. W tej próbce wykryto: ${GENUS}`
      else
        ERR_MSG=`echo This module works with the following genera: Escherichia. Following genus was identified in this sample is: ${GENUS}`
      fi

      python /opt/docker/EToKi/externals/virulencefinder_parser.py  -i  results_tab.tsv -s "nie" -r "\${ERR_MSG}" -o virulencefinder.json
      #json na zly gatunek
    fi # koniec if-a na zly gatunek
  fi # koniec if-a na zle QC
  """
}