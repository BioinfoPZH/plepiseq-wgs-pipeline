process run_Seqsero {
  // SeqSero only works for Salmonella
  container  =  params.main_image
  tag "Predicting OH for sample $x with Seqsero"
  cpus { params.threads > 3 ? 3 : params.threads }
  memory "5 GB"
  time "5m"
  input:
  tuple val(x), path(fasta), val(QC_status), val(SPECIES), val(GENUS), val(QC_status_contaminations)
  output:
  tuple val(x), path('seqsero/SeqSero_result.txt'), emit: to_pubdir
  tuple val(x), path('seqsero.json'), emit: json
  // when:
  // GENUS == 'Salmonella'
  script:
  """
  # -m to rodzaj algorytmu -m to chyba opart o k-mery
  # -t 4 to informacja ze inputem sa contigi z genomem
  # -p to procki, proces jest szybki i raport nextflow sugeruje uzyci bliskie 1 CPU

  if [[ ${QC_status} == "nie"  || ${QC_status_contaminations} == "nie" ]]; then
    mkdir seqsero
    touch seqsero/SeqSero_result.txt
    touch SeqSero_result.tsv
    
    if [ "${params.lan}" == "pl" ]; then
      ERR_MSG="Ten moduł został uruchomiony na próbce, która nie przeszła kontroli jakości."
    else
      ERR_MSG="This module was eneterd with failed QC and poduced no valid output"
    fi

    python /opt/docker/EToKi/externals/seqsero_parser.py  -i SeqSero_result.tsv -s "nie" -r "\${ERR_MSG}" -o seqsero.json    
    #json na zle QC
  else
    if [ ${GENUS} == "Salmonella" ]; then
      python /opt/docker/SeqSero2/bin/SeqSero2_package.py -m k -t 4 -p ${task.cpus} -i $fasta -d seqsero
      python /opt/docker/EToKi/externals/seqsero_parser.py  -i seqsero/SeqSero_result.tsv -s "tak" -o seqsero.json
    else
      mkdir seqsero
      touch seqsero/SeqSero_result.txt
      
      if [ "${params.lan}" == "pl" ]; then
        ERR_MSG="Ten moduł jest przeznaczony wyłącznie dla bakterii z rodzaju Salmonella"
      else
        ERR_MSG="This module works only with bacteria from Salmonella genus"
      fi

      touch SeqSero_result.tsv
      python /opt/docker/EToKi/externals/seqsero_parser.py  -i SeqSero_result.tsv -s "nie" -r "\${ERR_MSG}" -o seqsero.json    
      #json na zly gatunek
    fi # koniec if-a na zly gatunek
  fi # koniec if-a na zle QC
  cp -r seqsero/* .
  """
}