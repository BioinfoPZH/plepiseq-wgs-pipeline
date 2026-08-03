process run_ectyper {
  // This process works only for Escherichia
  container  = params.main_image
  cpus { params.threads > 3 ? 3 : params.threads }
  memory "10 GB"
  time "5m"
  tag "Predicting OH for sample $x with ectyper"
  input:
  tuple val(x), path(fasta), val(QC_status), val(SPECIES), val(GENUS), val(QC_status_contaminations)
  output:
  tuple val(x), path('output.tsv'), emit: to_pubdir
  tuple val(x), path('ectyper.json'), emit: json
  
  // when:
  // GENUS == 'Escherichia'
  script:
  """
  # opcje:
  # -i to input, 
  # -c to liczba core'ow proces jest szybki wiec zostawiamy 1
  # -hpid to minimalny seq identity dla antygenu H ustawiam na 90 zamiast default (95) po analizie Strain-u 5 z EQA 2023
  # -o to katalog z output
  mkdir ectyper_out

  if [[ ${QC_status} == "nie"  || ${QC_status_contaminations} == "nie" ]]; then
    # json na zle QC
    touch output.tsv
    if [ "${params.lan}" == "pl" ]; then
      ERR_MSG="Ten moduł został uruchomiony na próbce, która nie przeszła kontroli jakości."
    else
      ERR_MSG="This module was eneterd with failed QC and poduced no valid output"
    fi 

    python /opt/docker/EToKi/externals/ectyper_parser.py  -i output.tsv -s "nie" -r "\${ERR_MSG}" -o ectyper.json
  else
    if [ ${GENUS} == "Escherichia" ]; then
      ectyper -i $fasta -c ${task.cpus} -hpid 90 -o ectyper_out
      cp ectyper_out/output.tsv .
      python /opt/docker/EToKi/externals/ectyper_parser.py  -i output.tsv -s "tak" -o ectyper.json
    else
       #json na zly gatunek
       touch output.tsv
       if [ "${params.lan}" == "pl" ]; then
         ERR_MSG="Ten moduł jest przeznaczony wyłącznie dla bakterii z rodzaju Escherichia"
       else
         ERR_MSG="This module works only with bacteria from Escherichia genus"
       fi
       
       python /opt/docker/EToKi/externals/ectyper_parser.py  -i output.tsv -s "nie" -r "\${ERR_MSG}" -o ectyper.json
    fi # koniec if-a na zly gatunek
  fi # koniec if-a na zle QC
  """
}