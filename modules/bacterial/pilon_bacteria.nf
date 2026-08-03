process run_pilon {
  // Dokladne uzycie pilona jest tu https://github.com/broadinstitute/pilon/wiki/Requirements-&-Usage
  container  = params.main_image
  tag "Pilon for sample $x"
  cpus params.threads
  memory "40 GB"
  time "20m"
  input:
  tuple val(x), path(bam1), path(bam2), path('genomic_fasta.fasta'), val(QC_status)
  output:
  tuple val(x), path('latest_pilon.fasta'), path('latest_pilon.changes'), emit: ALL
  tuple val(x), path('latest_pilon.fasta'), val(QC_status), emit: ONLY_GENOME
  script:
  """
  if [ ${QC_status} == "nie" ]; then
    echo ">dummy_contig" >> latest_pilon.fasta
    echo "AAAAAAAAAAAAA" >> latest_pilon.fasta
    touch latest_pilon.changes  
  else 
    # indeksacja bam-ow
    /opt/docker/EToKi/externals/samtools index  $bam1
    /opt/docker/EToKi/externals/samtools index  $bam2

    # wywolanie pilon-a
    java -jar /opt/docker/EToKi/externals/pilon.jar --threads ${task.cpus} --genome genomic_fasta.fasta --frags $bam1 --unpaired $bam2 --output pilon_polish --vcf --changes --outdir pilon_bwa
 
    # przygotowanie outputu
    # cat pilon_bwa/pilon_polish.fasta | awk  -v ALA=${x} 'BEGIN{OFS=""}; {if(\$0 ~ />/) print \$0,"_", ALA; else print \$0}' >> last_pilon.fasta
    cat pilon_bwa/pilon_polish.fasta >> latest_pilon.fasta
    cp  pilon_bwa/pilon_polish.changes latest_pilon.changes
  fi
  """
}
