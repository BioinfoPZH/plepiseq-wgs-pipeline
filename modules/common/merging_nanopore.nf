process merging_nanopore {
    // Ten proces laczy wyniki maskowania dla sciezki z nanopore dla SARS i RSV
    // Gdzie mamy oddzielne mmapowania dla odczytow strict i overshoot
    tag "merging:${sampleId}"
    container  = params.main_image
    cpus { params.threads > 15 ? 15 : params.threads }
    memory "20 GB"
    input:
    tuple val(sampleId), path('trimmed_first.bam'), path('trimmed_first.bam.bai'),  val(QC_status), path(genome),  path('trimmed_second.bam'), path('trimmed_second.bam.bai'),  val(QC_status_2)

    output:
    tuple val(sampleId), path('merged.bam'), path('merged.bam.bai'), env(QC_status_exit), path(genome),  emit: to_medaka
    tuple val(sampleId), path('merged.bam'), path('merged.bam.bai'), env(QC_status_exit), path(genome),  path('merged_2.bam'), path('merged_2.bam.bai'), emit: to_medaka_2

    script:
    """
    if [ ${QC_status} == "nie" ]; then
      touch merged.bam
      touch merged.bam.bai
      touch merged_2.bam
      touch merged_2.bam.bai
      QC_status_exit="nie"
    else
      samtools merge -o merged_initial.bam trimmed_first.bam trimmed_second.bam
      samtools sort -@ ${task.cpus} -o merged.bam merged_initial.bam
      samtools index merged.bam
      cp merged.bam merged_2.bam
      samtools index merged_2.bam
      ILE=`samtools view merged_2.bam | wc -l`
      # If after merging all the sub-bams the final bam is empty switch QC to nie
      if [ \${ILE} -lt 10 ]; then
          QC_status_exit="nie"
      else
          QC_status_exit="tak"
      fi

    fi
    """
}
