process extract_final_contigs {
  // Liczenie pokrycia dla kazdego contiga polaczona z filtorwanie odczytow
  // Przy uzyciu NASZEGO skryptu
  // Modukl przekazuje bez mozliwosci zmiany QC_stauts
  container  = params.main_image
  tag "Coverage-based filtering for sample $x"
  cpus 1
  memory "5 GB"
  time "5m"
  input:
  tuple val(x), path(bam1), path('genomic_fasta.fasta'), val(QC_status)
  output:
  tuple val(x), path('final_scaffold_filtered.fa'), val(QC_status), emit: ONLY_GENOME
  tuple val(x), path('final_scaffold_filtered.fa'), path('Rejected_contigs.fa'), val(QC_status), emit: ONLY_GENOME_AND_REJECT
  // final_scaffold_filtered.fa to nazwa ustawiona NA SZTYWNO w skrypcie coverage_filter.py
  script:
  """
  if [ ${QC_status} == "nie" ]; then
    touch final_scaffold_filtered.fa
    touch Rejected_contigs.fa   
  else
    /opt/docker/EToKi/externals/samtools index  $bam1
    # Na rzyczenie tomka nie przechodza dalej contigi ktore maja
    # 1.pokrycie nizsze niz 0.1 sredniego pokrycia w probce
    # I (AND)
    # 2.pokrycie jest ponizej niz 20
    # oba parametry ustawione sa jako cechy pipeline na poczatku 
    python /opt/docker/EToKi/externals/coverage_filter.py genomic_fasta.fasta $bam1 ${params.min_coverage_ratio} ${params.final_coverage}
  fi
  """
}

process extract_final_stats {
  container  = params.main_image
  cpus 1
  memory "1 GB"
  time "5m"
  tag "Calculating basic statistics for sample $x"
  publishDir "${params.results_dir}/${x}/", mode: 'copy', pattern: "${x}.fasta"
  input:
  tuple val(x), path(fasta), path(fasta_reject), val(QC_status), val(GENUS)
  output:
  tuple val(x), path('Summary_statistics.txt'), path('Summary_statistics_with_reject.txt'), env(QC_status_exit), emit: STATISTICS
  tuple val(x), path(fasta), env(QC_status_exit), emit: GENOME
  tuple val(x), path('bacterial_genome_data.json'), emit: json
  tuple val(x), path('${x}.fasta'), emit: to_pubdir
  script:
  """
  if [[ "${GENUS}" == *"Salmo"* ]]; then
    GENOME_SIZE=5400000
  elif [[ "${GENUS}" == *"Escher"* ]]; then
    GENOME_SIZE=4800000
  elif [ ${GENUS} == "Campylobacter" ]; then
    GENOME_SIZE=1800000
  else
    # Bez znaczenia bo moduly wyzej zwroca QC_status nie w takiej sytuacji, ale tutaj moze wejsc ponownie ten zly genus
    # a moj skrypt do parsowania wymaga podania tej wartosci
    GENOME_SIZE=6000000
  fi 

  if [ ${QC_status} == "nie" ]; then
     touch Summary_statistics.txt
     touch Summary_statistics_with_reject.txt
     echo -e ">dummy\nN" > ${x}.fasta
     
     if [ "${params.lan}" == "pl" ]; then
       ERR_MSG="Ten moduł został uruchomiony na próbce, która nie przeszła kontroli jakości."
     else
       ERR_MSG="This module was eneterd with failed QC and poduced no valid output"
     fi

     QC_status_exit=`python /opt/docker/EToKi/externals/extract_final_stats_parser.py -l ${params.L50} -n ${params.contig_number} -g \${GENOME_SIZE} -c ${params.min_genome_length} -p ${params.final_coverage} -s ${QC_status} -r "\${ERR_MSG}" -o bacterial_genome_data.json --lan ${params.lan}`
  else
    cat $fasta $fasta_reject >> all_contigs.fasta
    cp $fasta ${x}.fasta
    python  /opt/docker/EToKi/externals/calculate_stats.py $fasta all_contigs.fasta
    
    QC_status_exit=`python /opt/docker/EToKi/externals/extract_final_stats_parser.py -i Summary_statistics.txt -j Summary_statistics_with_reject.txt -l ${params.L50} -n ${params.contig_number} -g \${GENOME_SIZE} -c ${params.min_genome_length} -p ${params.final_coverage} -s tak -o bacterial_genome_data.json --lan ${params.lan}`

  fi
  """
}