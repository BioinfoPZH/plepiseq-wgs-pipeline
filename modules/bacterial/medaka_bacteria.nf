process run_medaka {
  // wygladzanie genomu medaka, nanopolish nie dziala bo nie mamy pliko fast5

  container  = params.medaka_image
  tag "Medaka for sample $x"
  cpus { params.threads > 15 ? 15 : params.threads }
  memory "20 GB"
  time "10m"
  input:
  tuple val(x), path(bam1), path(fasta), val(QC_status)
  output:
  tuple val(x), path('postmedaka.fasta'), emit: ONLY_GENOME
  script:
  """
  if [ ${QC_status} == "nie" ]; then
    echo ">dummy_contig" >> postmedaka.fasta
    echo "AAAAAAAAAAAAA" >> postmedaka.fasta
  else
    # indeksacja bam-ow
    samtools index $bam1
 
  
    medaka inference --model ${params.model_medaka} \
                     --threads ${task.cpus} \
                     $bam1 \
                     forvariants.hdf

  
    medaka vcf forvariants.hdf  $fasta medaka.vcf
    medaka tools annotate medaka.vcf $fasta $bam1 medaka_annotated.vcf
    bcftools sort medaka_annotated.vcf >> medaka_annotated_sorted.vcf
    bgzip medaka_annotated_sorted.vcf
    tabix medaka_annotated_sorted.vcf.gz
  
    qual=12
    min_cov=20
  
    bcftools filter -O z -o medaka_annotated_filtered.vcf.gz -i "GQ >= \${qual} && DP >= \${min_cov}" medaka_annotated_sorted.vcf.gz
    tabix medaka_annotated_filtered.vcf.gz


    cat $fasta | bcftools consensus medaka_annotated_filtered.vcf.gz >> postmedaka.fasta
  fi
  """
}
