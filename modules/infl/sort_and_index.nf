process sort_and_index {
    tag "sort_and_index:${sampleId}"
    container  = params.main_image
    cpus 1
    memory "20 GB"
    input:
    tuple val(sampleId), path(bam), val(QC_status)

    output:
    tuple val(sampleId), path("${bam.baseName}_sorted.bam"), path("${bam.baseName}_sorted.bam.bai"), env(QC_status_exit)

    script:
    def newBam = "${bam.baseName}_sorted.bam"
    """
    if [ ${QC_status} == "nie" ]; then
      touch "${bam.baseName}_sorted.bam"
      touch "${bam.baseName}_sorted.bam.bai"
      QC_status_exit="nie"
    else
      samtools sort -o ${newBam} ${bam}
      samtools index ${newBam}

      # switch QC to "nie" if there are less than 10 valid reads after all the filtering steps
      ILE=`samtools view ${newBam} | wc -l`
      if [ "\${ILE}" -lt 10 ]; then
        QC_status_exit="nie"
      else
        QC_status_exit="tak"
      fi
    fi
    """
}
