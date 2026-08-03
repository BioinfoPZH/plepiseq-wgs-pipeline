process run_minimap2 {
  // Proces do mapowania odczytow na scaffold
  tag "Remapping of reads to predicted scaffold for sample $x"
  container  = params.main_image 
  cpus params.threads
  memory "20 GB"
  time "10m"
  input:
  tuple val(x), path(fasta), path(reads), val(SPECIES), val(GENUS), val(QC_status)
  output:
  tuple val(x), path('sorted.bam'), path(fasta), val(QC_status)
  script:
  """
  # minmap jest zarowno w PATH z etoki/externals jak i w /data/Flye/bin
  if [ ${QC_status} == "nie" ]; then
    touch sorted.bam
    # dummy output aby skypt poszedl dalej
  else
    minimap2 -a -x map-ont -t ${task.cpus} $fasta $reads | samtools view -bS -F 2052 - | samtools sort -@ ${task.cpus} -o sorted.bam -
  fi
  """
}

process run_minimap2_2nd {
  // Proces do mapowania odczytow na scaffold
  // W nanopre w jednym workflow uzywam go 2 razy wiec musze zrobic ta glupia kopie
  tag "Remapping of reads to predicted scaffold for sample $x"
  container  = params.main_image
  cpus params.threads
  memory "20 GB"
  time "10m"
  input:
  tuple val(x), path(fasta), path(reads), val(SPECIES), val(GENUS), val(QC_status)
  output:
  tuple val(x), path('sorted.bam'), path(fasta), val(QC_status)
  script:
  """
  if [ ${QC_status} == "nie" ]; then
    touch sorted.bam
    # dummy output aby skypt poszedl dalej
  else
    # minmap jest zarowno w PATH z etoki/externals jak i w /data/Flye/bin

    minimap2 -a -x map-ont -t ${task.cpus} $fasta $reads | samtools view -bS -F 2052 - | samtools sort -@ ${task.cpus} -o sorted.bam -
  fi
  """
}