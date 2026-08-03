process run_flye {
  // nano-raw to input dla wersji przed guppym 5+ z duza liczba bledow
  // -g to estymowana wielkosc ocekiwanego genomu
  // -t to liczba watkow CPU
  // -i to liczba powtorzec wygladzania genomy
  // --no-alt-contig to informacja aby nie podawac haplotypow gdyby byly, w koncu salmonella to monoploid ...
  // --deterministic -  perform disjointig assembly single-threaded program zwraca dla tych samych danych rozne wyniki
  // wedlug issue z githuba https://github.com/mikolmogorov/Flye/issues/640
  // ten problem dalej istnieje 

  container  = params.main_image
  tag "Predicting scaffold with flye for sample $x"
  cpus { params.threads > 15 ? 15 : params.threads }
  memory "60 GB"
  time "1h 20m"
  // in --deterministic the process is slow because in uses 1 cpu for some task 
  // maxForks 10 
  input:
  tuple val(x), path(fastq_gz), val(SPECIES), val(GENUS), val(QC_status)
  output:
  tuple val(x), path('output/assembly.fasta')
  
  script:
  """
  # /data/Flye to sciezka z Flye instalowanego z github, uwaga
  # w kontenerze tez jest flye instalowant przez etoki i ten jest w PATH

  if [ ${QC_status} == "nie" ]; then
    mkdir output
    echo ">dummy_contig" >> output/assembly.fasta
    echo "AAAAAAAAAAAAA" >> output/assembly.fasta
  else
    if [[ "${GENUS}" == *"Salmo"* ]]; then
        GENOME_SIZE="5.4m"
    elif [[ "${GENUS}" == *"Escher"* ]]; then
        GENOME_SIZE="4.6m"
    elif [ ${GENUS} == "Campylobacter" ]; then
        GENOME_SIZE="1.8m"
    else
        GENOME_SIZE="5m" # trafilem na zly organizm wiec wpisuje 5m ten genom i tak nie bedzie wykorzystany
    fi
    # For R10 recommedned flag is --nano-hq
    # --nano-raw was used for R7-R9
    /opt/docker/Flye/bin/flye --nano-hq ${fastq_gz} -g \${GENOME_SIZE} -o output -t ${task.cpus} -i 3 --no-alt-contig --deterministic
  fi
  """
 
}