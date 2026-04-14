process snpEff_nanopore {
    tag "snpEff:${sampleId}"
    container  = params.main_image
    cpus 1
    memory "20 GB"
    // publishDir "${params.results_dir}/${sampleId}", mode: 'copy', pattern: "${sampleId}_detected_variants_consensus_annotated.txt"

    input:
    //tuple val(sampleId), path(consensus_vcf_gz), path(consensus_vcf_gz_tbi), path(ref_genome_with_index), val(QC_status_vcf), val(SUBTYPE_ID), path('forvariants.bam'), path('forvariants.bam.bai'), val(QC_status)
    
    tuple val(sampleId), path(consensus_vcf_gz), path(consensus_vcf_gz_tbi), val(QC_status_vcf), path('sequences.fa'), path('genes.gtf'), path('forvariants.bam'), path('forvariants.bam.bai'), val(QC_status)
    
    output:
    tuple val(sampleId), path("${sampleId}_detected_variants_consensus_annotated.txt")

    script:
    """


    if [[ ${QC_status_vcf} == "nie" || ${QC_status} == "nie"  ]]; then
      touch ${sampleId}_detected_variants_consensus_annotated.txt
    else

      if [ ${params.species} == "SARS-CoV-2" ]; then
        # predefined build
        snp_eff="MN908947.3"
      elif [ ${params.species} == "RSV" ]; then
        if [ `head -1 sequences.fa |  awk  '{if (substr(\$0, 2)=="PP525321") print 1; else print 0}'` == 1 ]; then
          snp_eff='hRSV_A'
        elif  [ `head -1 sequences.fa | awk  '{if (substr(\$0, 2)=="OR666591") print 1; else print 0}'` == 1 ]; then
          snp_eff='hRSV_B'
        fi
      elif [ ${params.species} == "Influenza" ]; then
        cp sequences.fa /opt/snpEff/data/hybrid/
        cp genes.gtf /opt/snpEff/data/hybrid/
        java -jar /opt/snpEff/snpEff.jar build -noCheckCds -noCheckProtein -gtf22 -v hybrid
        snp_eff="hybrid"
	
      fi

        java -jar /opt/snpEff/snpEff.jar ann -noStats \${snp_eff} ${consensus_vcf_gz} > detected_variants_consensus_annotated.vcf
        bgzip --force detected_variants_consensus_annotated.vcf
        tabix detected_variants_consensus_annotated.vcf.gz

        # split multi segment reference genome into individual fastas
        # Used only to get segment names, otherwise pointless
        cat sequences.fa | awk '{if (substr(\$0, 1, 1)==">") { new_name=\$0; gsub("\\\\.", "_", new_name); gsub("/", "_", new_name);  filename=("reference_"substr(new_name,2) ".fasta"); print \$0 >> filename } else {print toupper(\$0)  >> filename}}'

        # This script produces depth_and_usage.txt which "mimics" part2 file from illumina (it has an extra columnt at pos 1 with segment name)
        calculate_coverage_and_usage.py forvariants.bam  ${consensus_vcf_gz}

        for PLIK in `ls reference_*`; do
          SEGMENT=`basename \${PLIK} ".fasta" | cut -d "_" -f2- `
          SEGMENT_EXACT_NAME=`head -1 \${PLIK} | tr -d ">"` # fo extracting data from vcf

          # extracting infromations from snpEff output for analyzed segment
          # We nned to include CHROM name in the outpur in this version
          bcftools query --format '%CHROM | %POS | %REF %POS %ALT| %ANN \\n' \
                  detected_variants_consensus_annotated.vcf.gz | \
                  cut -d "|" -f1,2,3,5,7,14 |\
                  tr "|" "\\t" | \
                  awk  'BEGIN {OFS = "\\t"} {if ( \$6 == "upstream_gene_variant" || \$6 == "downstream_gene_variant") {gene="."; aa="."} else {gene=\$7; aa=\$8}; print \$1, \$2, gene, \$3, \$4, \$5, aa, \$6}'  | grep \${SEGMENT_EXACT_NAME} > part1_\${SEGMENT}.txt

          ### extracting DP and allele usage from freebayes output for analyzed segment

          cat  depth_and_usage.txt | grep "\${SEGMENT_EXACT_NAME}" | cut -f2- >> part2_\${SEGMENT}.txt 

          ### merging two files if freebayes dosn't have info regarding a variant from consensus.vcf we
          ### put "-" in DP and AF columns
          join -a 1 -1 2 -2 1 -o1.1,1.2,1.3,1.4,1.5,1.6,1.7,1.8,2.2,2.3  -e '-' part1_\${SEGMENT}.txt part2_\${SEGMENT}.txt | sort -nk2 | cut -d " " -f1- | tr " " "\\t" >> detected_variants_consensus_annotated_\${SEGMENT}.txt
        done
        # merge results for all the segments
        cat detected_variants_consensus_annotated_*.txt >> ${sampleId}_detected_variants_consensus_annotated.txt
    
   fi # koniec if-a na QC
   """
}

