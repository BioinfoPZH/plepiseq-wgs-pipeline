process merging {
    tag "merging:${sampleId}"
    container  = params.main_image
    cpus { params.threads > 5 ? 5 : params.threads }
    memory "20 GB"
    input:
    tuple val(sampleId), path(filtering_bam), path(ivar_bam), val(QC_status)

    output:
    tuple val(sampleId), path('clean_sort_dedup_trimmed_sort.bam'), path('clean_sort_dedup_trimmed_sort.bam.bai'), env(QC_status_exit)

    script:
    """
    if [ ${QC_status} == "nie" ]; then
      touch clean_sort_dedup_trimmed_sort.bam
      touch clean_sort_dedup_trimmed_sort.bam.bai
      QC_status_exit="nie"
    else
      samtools merge -o clean_sort_dedup_trimmed_sort_tmp.bam ${filtering_bam} ${ivar_bam}
      samtools sort -@ ${task.cpus} -o clean_sort_dedup_trimmed_sort.bam clean_sort_dedup_trimmed_sort_tmp.bam
      samtools index clean_sort_dedup_trimmed_sort.bam
      ILE=`samtools view clean_sort_dedup_trimmed_sort.bam | wc -l`
      # If after merging all the sub-bams the final bam is empty switch QC to nie
      if [ \${ILE} -lt 10 ]; then
          QC_status_exit="nie"
      else
          QC_status_exit="tak"
      fi
    fi
    """
}
