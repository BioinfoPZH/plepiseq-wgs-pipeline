process run_kraken2_illumina {
  // kraken2 instalowany jest przez ETOKI i jest w path kontenera do salmonelli
  tag "Run kraken2 for sample:${x}"
  container  = params.main_image
  containerOptions "--volume ${params.db_absolute_path_on_host}:/db"
  cpus { params.threads > 10 ? 10 : params.threads }
  memory '120 GB'
  time "10m"
  input:
  tuple val(x), path(reads), val(QC_STATUS), val(TOTAL_BASES)

  output:
  tuple val(x), path('report_kraken2.txt'), path('report_kraken2_individualreads.txt'), path('Summary_kraken_genera.txt'), path('Summary_kraken_species.txt')

  script:
  """
  if [ ${QC_STATUS} == "nie" ]; then
    # upstream module failed we produce empty files do the pipeline can execute
    touch report_kraken2.txt
    touch report_kraken2_individualreads.txt
    touch Summary_kraken_genera.txt
    touch Summary_kraken_species.txt
  else
    kraken2 --db /db/kraken2 \
            --report report_kraken2.txt \
            --threads ${task.cpus} \
            --gzip-compressed \
            --minimum-base-quality ${params.quality} \
            --use-names ${reads[0]} ${reads[1]} >> report_kraken2_individualreads.txt 2>&1
    # parse kraken extract two most abundant FAMILIES
    LEVEL="G" # G - genus, S - species
    SPEC1=`cat report_kraken2.txt  | awk '{if(\$1 != 0.00) print \$0}' | grep -w \${LEVEL} | sort -rnk 1 | head -1 | tr -s " " | cut -f6 | tr -d "="`
    SPEC2=`cat report_kraken2.txt  | awk '{if(\$1 != 0.00) print \$0}' | grep -w \${LEVEL} | sort -rnk 1 | head -2 | tail -1 | tr -s " " | cut -f6 | tr -d "="`
    ILE1=`cat report_kraken2.txt  | awk '{if(\$1 != 0.00) print \$0}'| grep -w \${LEVEL} | sort -rnk 1 | head -1 | tr -s " " | cut -f1 | tr -d " "`
    ILE2=`cat report_kraken2.txt  | awk '{if(\$1 != 0.00) print \$0}'| grep -w \${LEVEL} | sort -rnk 1 | head -2 | tail -1 | tr -s " " | cut -f1 | tr -d " "`

    echo -e "${x}\t\${SPEC1}\${ILE1}%\t\${SPEC2}\${ILE2}%" >> Summary_kraken_genera.txt

    LEVEL="S"
    SPEC1=`cat report_kraken2.txt  | awk '{if(\$1 != 0.00) print \$0}' | grep -w \${LEVEL} | sort -rnk 1 | head -1 | tr -s " " | cut -f6 | tr -d "="`
    SPEC2=`cat report_kraken2.txt  | awk '{if(\$1 != 0.00) print \$0}' | grep -w \${LEVEL} | sort -rnk 1 | head -2 | tail -1 | tr -s " " | cut -f6 | tr -d "="`
    ILE1=`cat report_kraken2.txt  | awk '{if(\$1 != 0.00) print \$0}'| grep -w \${LEVEL} | sort -rnk 1 | head -1 | tr -s " " | cut -f1 | tr -d " "`
    ILE2=`cat report_kraken2.txt  | awk '{if(\$1 != 0.00) print \$0}'| grep -w \${LEVEL} | sort -rnk 1 | head -2 | tail -1 | tr -s " " | cut -f1 | tr -d " "`

    echo -e "${x}\t\${SPEC1}\${ILE1}%\t\${SPEC2}\${ILE2}%" >> Summary_kraken_species.txt
  fi
  """
}



process run_kraken2_nanopore {
  // kopia processu do illuminy, ale z uwzglednieniem ze jest tylko jeden plik z odczytami
  tag "kraken2:${x}"
  container  = params.main_image
  containerOptions "--volume ${params.db_absolute_path_on_host}:/db"
  cpus { params.threads > 10 ? 10 : params.threads }
  memory '120 GB'
  time "10m"
  input:
  tuple val(x), path(reads), val(QC_STATUS), val(TOTAL_BASES)

  output:
  tuple val(x), path('report_kraken2.txt'), path('report_kraken2_individualreads.txt'), path('Summary_kraken_genera.txt'), path('Summary_kraken_species.txt')

  script:
  """
  if [ ${QC_STATUS} == "nie" ]; then
    # upstream module failed we produce empty files do the pipeline can execute
    touch report_kraken2.txt
    touch report_kraken2_individualreads.txt
    touch Summary_kraken_genera.txt
    touch Summary_kraken_species.txt
  else
    kraken2 --db /db/kraken2 \
            --report report_kraken2.txt \
            --threads ${task.cpus} \
            --gzip-compressed \
            --minimum-base-quality ${params.quality} \
            --use-names ${reads} >> report_kraken2_individualreads.txt 2>&1
  
    LEVEL="G"

    SPEC1=`cat report_kraken2.txt  | awk '{if(\$1 != 0.00) print \$0}' | grep -w \${LEVEL} | sort -rnk 1 | head -1 | tr -s " " | cut -f6 | tr -d "="`
    SPEC2=`cat report_kraken2.txt  | awk '{if(\$1 != 0.00) print \$0}' | grep -w \${LEVEL} | sort -rnk 1 | head -2 | tail -1 | tr -s " " | cut -f6 | tr -d "="`
    ILE1=`cat report_kraken2.txt  | awk '{if(\$1 != 0.00) print \$0}'| grep -w \${LEVEL} | sort -rnk 1 | head -1 | tr -s " " | cut -f1 | tr -d " "`
    ILE2=`cat report_kraken2.txt  | awk '{if(\$1 != 0.00) print \$0}'| grep -w \${LEVEL} | sort -rnk 1 | head -2 | tail -1 | tr -s " " | cut -f1 | tr -d " "`

    echo -e "${x}\t\${SPEC1}\${ILE1}%\t\${SPEC2}\${ILE2}%" >> Summary_kraken_genera.txt

    LEVEL="S"
    SPEC1=`cat report_kraken2.txt  | awk '{if(\$1 != 0.00) print \$0}' | grep -w \${LEVEL} | sort -rnk 1 | head -1 | tr -s " " | cut -f6 | tr -d "="`
    SPEC2=`cat report_kraken2.txt  | awk '{if(\$1 != 0.00) print \$0}' | grep -w \${LEVEL} | sort -rnk 1 | head -2 | tail -1 | tr -s " " | cut -f6 | tr -d "="`
    ILE1=`cat report_kraken2.txt  | awk '{if(\$1 != 0.00) print \$0}'| grep -w \${LEVEL} | sort -rnk 1 | head -1 | tr -s " " | cut -f1 | tr -d " "`
    ILE2=`cat report_kraken2.txt  | awk '{if(\$1 != 0.00) print \$0}'| grep -w \${LEVEL} | sort -rnk 1 | head -2 | tail -1 | tr -s " " | cut -f1 | tr -d " "`

    echo -e "${x}\t\${SPEC1}\${ILE1}%\t\${SPEC2}\${ILE2}%" >> Summary_kraken_species.txt
  fi
  """
}


process run_speciesfinder_illumina {
  // This module unlike krakern and metaphlan will pass QC_STATUS and TOTAL_BASES to get_species_illumina
  tag "kmerfinder:${x}"
  container  = params.main_image
  containerOptions "--volume ${params.db_absolute_path_on_host}:/db"
  cpus 1
  memory '30 GB'
  time "10m"

  input:
  tuple val(x), path(reads), val(QC_STATUS), val(TOTAL_BASES)

  output:
  tuple val(x),path('results.spa'), path('results.txt'), env(SPECIES), env(GENUS), val(QC_STATUS), val(TOTAL_BASES)

  script:
  """
  if [ ${QC_STATUS} == "nie" ]; then
    # upstream module failed we produce empty files do the pipeline can execute
    touch results.spa
    touch results.txt
    SPECIES=""
    GENUS=""
  else
    DB_PRFIX=\$(basename /db/speciesfinder/bacteria/*tax | cut -d "." -f1)
    python -m speciesfinder -i ${reads[0]} ${reads[1]} -o ./kmerfider_out -db /db/speciesfinder/bacteria/\${DB_PRFIX} -x -tax /db/speciesfinder/bacteria/\${DB_PRFIX}.tax

    cp kmerfider_out/results.res results.spa
    cp kmerfider_out/results.txt .
  
    SPECIES=`python /data/parse_speciesfinder.py results.spa species`
    GENUS=`python /data/parse_speciesfinder.py results.spa genus`
  fi
  """
}


process run_speciesfinder_nanopore {
  tag "kmerfinder:${x}"
  container  = params.main_image
  containerOptions "--volume ${params.db_absolute_path_on_host}:/db"
  cpus 1
  memory '30 GB'
  time "10m"
  input:
  tuple val(x), path(reads), val(QC_STATUS), val(TOTAL_BASES)

  output:
  tuple val(x),path('results.spa'), path('results.txt'), env(SPECIES), env(GENUS), val(QC_STATUS), val(TOTAL_BASES)

  script:
  """
  if [ ${QC_STATUS} == "nie" ]; then
    # upstream module failed we produce empty files do the pipeline can execute
    touch results.spa
    touch results.txt
    SPECIES=""
    GENUS=""
  else
    DB_PRFIX=\$(basename /db/speciesfinder/bacteria/*tax | cut -d "." -f1)
    python -m speciesfinder -i ${reads} -o ./kmerfider_out -db /db/speciesfinder/bacteria/\${DB_PRFIX} -x -tax /db/speciesfinder/bacteria/\${DB_PRFIX}.tax

    cp kmerfider_out/results.res results.spa
    cp kmerfider_out/results.txt .
  
    SPECIES=`python /data/parse_speciesfinder.py results.spa species`
    GENUS=`python /data/parse_speciesfinder.py results.spa genus`
  fi
  """
}


process run_metaphlan_illumina {
  tag "Run Metaphlan for sample:${x}"
  container  = params.main_image
  containerOptions "--volume ${params.db_absolute_path_on_host}:/db"
  cpus { params.threads > 15 ? 15 : params.threads }
  memory '40 GB'
  time "40m"
  input:
  tuple val(x), path(reads), val(QC_STATUS), val(TOTAL_BASES)

  output:
  tuple val(x), path('report_metaphlan_SGB.txt'), path('report_metaphlan_species.txt'), path('report_metaphlan_genera.txt')

  script:
  """
  if [ ${QC_STATUS} == "nie" ]; then
    # upstream module failed we produce empty files do the pipeline can execute
    touch report_metaphlan_SGB.txt
    touch report_metaphlan_species.txt
    touch report_metaphlan_genera.txt
  else
    metaphlan ${reads[0]},${reads[1]} --mapout metagenome.bowtie2.bz2 --nproc ${task.cpus} --input_type fastq -o profiled_metagenome.txt --db_dir /db/metaphlan/ 
    # Parsujemy wyniki
    metaphlan metagenome.bowtie2.bz2 --input_type mapout --db_dir /db/metaphlan/  --nproc ${task.cpus} --tax_lev 't' -o report_metaphlan_SGB.txt
    metaphlan metagenome.bowtie2.bz2 --input_type mapout --db_dir /db/metaphlan/  --nproc ${task.cpus} --tax_lev 's' -o report_metaphlan_species.txt
    metaphlan metagenome.bowtie2.bz2 --input_type mapout --db_dir /db/metaphlan/  --nproc ${task.cpus} --tax_lev 'g' -o report_metaphlan_genera.txt
  fi
  """
}