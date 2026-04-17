process bwa_paired {
  // Funkcja do mapowania odczytow Pair-end
  cpus params.threads
  memory "20 GB"
  time "10m"
  container  = params.main_image
  tag "RE-mapowanie PE dla sample $x"
  input:
  tuple val(x), path('genomic_fasta.fasta'), val(QC_status), path(read_1),  path(read_2)
  output:
  tuple val(x), path('mapowanie_bwa_PE.bam')
  script:
  """

  if [ ${QC_status} == "nie" ]; then
      touch mapowanie_bwa_PE.bam
  else
    /opt/docker/EToKi/externals/bwa index genomic_fasta.fasta
    /opt/docker/EToKi/externals/bwa mem -t ${task.cpus} -T 30 genomic_fasta.fasta ${read_1} ${read_2} |  /opt/docker/EToKi/externals/samtools fixmate -m -@ ${task.cpus} - - | /opt/docker/EToKi/externals/samtools sort -@ ${task.cpus} -  |  /opt/docker/EToKi/externals/samtools markdup -r -@ ${task.cpus}  -O BAM - mapowanie_bwa_PE.bam
  fi
  """
}


process bwa_single {
  // Funkcja do mapowania odczytow Single-end na genom
  cpus params.threads
  memory "10 GB"
  time "5m"
  container  = params.main_image
  tag "RE-mapowanie SE dla sample $x"
  input:
  tuple val(x), path('genomic_fasta.fasta'), val(QC_status), path(reads)
  output:
  tuple val(x), path('mapowanie_bwa_SE.bam')
  // bwa_single przekazuje QC_status do merge_bams, nie ma potrzeby aby robily to oba moduly do bwa
  script:
  """
  if [ ${QC_status} == "nie" ]; then
     touch mapowanie_bwa_SE.bam
  else
    /opt/docker/EToKi/externals/bwa index genomic_fasta.fasta
    /opt/docker/EToKi/externals/bwa mem -t ${task.cpus} -T 30 genomic_fasta.fasta ${reads} |  /opt/docker/EToKi/externals/samtools sort -@ ${task.cpus} -  |  /opt/docker/EToKi/externals/samtools markdup -r -@ ${task.cpus} -O BAM - mapowanie_bwa_SE.bam
  fi
  """
}

process merge_bams {
  // process do mergowania bam-ow
  // Uzywany w workflow calculate_coverage
  container  = params.main_image
  tag "Merging bam files for sample $x"
  cpus 1
  memory "5 GB"
  time "5m"
  input:
  tuple val(x), path(bam1), path(bam2)
  output:
  tuple val(x), path('merged_bams.bam')
  script:
  """
  PAIRED_BAM_SIZE=`wc -l $bam1 | cut -d " " -f1`
  if [ \${PAIRED_BAM_SIZE} -eq 0 ]; then
     touch merged_bams.bam
  else
    /opt/docker/EToKi/externals/samtools merge -f merged_bams.bam $bam1 $bam2
  fi
  """ 
}