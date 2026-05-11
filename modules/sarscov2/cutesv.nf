process introduce_SV_with_cutesv {
    // SARS-CoV-2 Nanopore SV caller. Plugs in between consensus_nanopore and
    // substitute_ref_genome. Mirrors the FASTA-layer integration pattern from
    // modules/common/manta.nf: per-segment loop, bcftools consensus --mark-del X
    // against the segment reference, then insert_SV_python2.py merges the
    // SV-derived deletions into the per-segment SNP consensus.
    tag "cutesv:$sampleId"
    publishDir "${params.results_dir}/${sampleId}/", mode: 'copy', pattern: "output_*.fasta"
    container = params.main_image
    cpus { params.threads > 15 ? 15 : params.threads }
    memory "20 GB"

    input:
    tuple val(sampleId), \
          path(bam), \
          path(bai), \
          path('ref_genome.fasta'), \
          val(QC_status_minimap), \
          path('primers.bed'), \
          path(consensus_files), \
          val(QC_status_consensus)

    output:
    tuple val(sampleId), path('consensus_masked_SV.fa'), path('ref_genome.*'), env(QC_status_exit), emit: fasta_refgenome_and_qc
    tuple val(sampleId), path('consensus_masked_SV.fa'), env(QC_status_exit), emit: fasta_and_qc
    tuple val(sampleId), path('cuteSV.filtered.tsv'), emit: sv_table

    script:
    """
    # ---- cuteSV tuning knobs (edit here; intentionally NOT Nextflow params) ----
    CUTESV_MIN_SUPPORT=2              # cuteSV --min_support (permissive caller threshold)
    CUTESV_MIN_READ_LEN=50            # cuteSV --min_read_len
    CUTESV_MIN_DV=10                  # post-filter: minimum variant-supporting reads
    CUTESV_MIN_VAF=0.5                # post-filter: minimum DV/(DR+DV)
    CUTESV_MAX_SV_LENGTH=3000         # post-filter: upper bound on |SVLEN|
    CUTESV_MIN_SV_LENGTH_FALLBACK=500 # used only if amplicon-1 parsing fails
    # ---------------------------------------------------------------------------

    if [[ "${QC_status_minimap}" == "nie" || "${QC_status_consensus}" == "nie" ]]; then
      # Either upstream stage failed - dummy outputs to keep the channel flowing
      touch consensus_masked_SV.fa
      echo -e "chrom\\tstart\\tend\\tid\\tlen\\tfilter\\tDR\\tDV\\tVAF\\tGQ" > cuteSV.filtered.tsv
      QC_status_exit="nie"
    else
      QC_status_exit="tak"

      # ---- 1. MIN_SV_LEN = floor(0.8 * amplicon-1 outer span) ------------------
      # Matches read_amplicon_scheme / dlugosc_pierwszego_amplikonu logic in
      # bin/sarscov2/simple_filter_nanopore_final_with_windowstep.py.
      LEFT_START=\$(awk '\$4 ~ /_1_LEFT(_alt|_bis)?\$/ {print \$2; exit}' primers.bed)
      RIGHT_END=\$(awk '\$4 ~ /_1_RIGHT(_alt|_bis)?\$/ {print \$3; exit}' primers.bed)
      if [[ -n "\${LEFT_START}" && -n "\${RIGHT_END}" && "\${RIGHT_END}" -gt "\${LEFT_START}" ]]; then
        AMPLICON_LEN=\$((RIGHT_END - LEFT_START))
        MIN_SV_LEN=\$(python3 -c "print(int(0.8 * \${AMPLICON_LEN}))")
      else
        MIN_SV_LEN=\${CUTESV_MIN_SV_LENGTH_FALLBACK}
      fi

      # ---- 2. Run cuteSV (user's empirical command verbatim) -------------------
      # --min_size 30 is the caller's lower bound (permissive). The amplicon-derived
      # MIN_SV_LEN is the strict post-filter applied below.
      samtools faidx ref_genome.fasta
      mkdir -p cuteSV_genotype_tmp
      cuteSV ${bam} ref_genome.fasta cuteSV.genotyped.vcf cuteSV_genotype_tmp \\
        --threads ${task.cpus} \\
        --sample ${sampleId} \\
        --min_size 30 \\
        --min_support \${CUTESV_MIN_SUPPORT} \\
        --min_read_len \${CUTESV_MIN_READ_LEN} \\
        --max_cluster_bias_INS 100 \\
        --diff_ratio_merging_INS 0.3 \\
        --max_cluster_bias_DEL 100 \\
        --diff_ratio_merging_DEL 0.3 \\
        --report_readid \\
        --genotype

      # ---- 3. Extract deletions to TSV (user's exact bcftools format) ----------
      # The %INFO/RE column at field 7 keeps the awk indexing \$8=DR, \$9=DV, \$10=GQ
      # aligned with the user's empirical script.
      bcftools view -i 'INFO/SVTYPE="DEL"' cuteSV.genotyped.vcf \\
        | bcftools query \\
            -f '%CHROM\\t%POS\\t%INFO/END\\t%ID\\t%FILTER\\t%INFO/SVLEN\\t%INFO/RE[\\t%DR\\t%DV\\t%GQ]\\t%INFO/RNAMES\\n' \\
        > cuteSV.deletions.genotyped.tsv

      # ---- 4. Apply the user's awk filter with dynamic MIN_SV_LEN --------------
      awk -F'\\t' \\
          -v MIN="\${MIN_SV_LEN}" \\
          -v MAX="\${CUTESV_MAX_SV_LENGTH}" \\
          -v MIN_DV="\${CUTESV_MIN_DV}" \\
          -v MIN_VAF="\${CUTESV_MIN_VAF}" '
      BEGIN { OFS="\\t"; print "chrom","start","end","id","len","filter","DR","DV","VAF","GQ" }
      {
        len = -\$6
        dr  = \$8
        dv  = \$9
        gq  = \$10
        vaf = (dr + dv > 0) ? dv / (dr + dv) : 0
        if (\$5 == "PASS" && vaf >= MIN_VAF && dv >= MIN_DV && len >= MIN && len <= MAX) {
          print \$1, \$2, \$3, \$4, len, \$5, dr, dv, vaf, gq
        }
      }' cuteSV.deletions.genotyped.tsv > cuteSV.filtered.tsv

      # ---- 5. Re-derive a small filtered VCF from the IDs that survived --------
      # Going TSV->VCF via IDs guarantees the set fed to bcftools consensus is
      # exactly the set reported in cuteSV.filtered.tsv - no expression drift.
      bgzip -f cuteSV.genotyped.vcf
      tabix -p vcf cuteSV.genotyped.vcf.gz
      awk 'NR>1 {print \$4}' cuteSV.filtered.tsv > cuteSV.passed_ids.txt
      if [ -s cuteSV.passed_ids.txt ]; then
        bcftools view -O z -o cuteSV.filtered.vcf.gz \\
          -i 'ID=@cuteSV.passed_ids.txt' cuteSV.genotyped.vcf.gz
      else
        bcftools view -O z -o cuteSV.filtered.vcf.gz -h cuteSV.genotyped.vcf.gz
      fi
      tabix -p vcf cuteSV.filtered.vcf.gz

      # ---- 6. Per-segment apply: bcftools consensus + insert_SV_python2.py ----
      # Split reference into per-segment files (mirrors manta line 79 of manta.nf).
      # For SARS the loop runs once; pattern is preserved for future RSV/Influenza.
      awk '{
        if (substr(\$0, 1, 1)==">") {
          new_name=\$0
          gsub("\\\\.", "_", new_name)
          gsub("/", "_", new_name)
          filename=("reference_"substr(new_name,2) ".fasta")
          print \$0 > filename
        } else {
          print toupper(\$0) >> filename
        }
      }' ref_genome.fasta

      for consensus_file in output_*.fasta; do
        # output_<segment_clean>.fasta -> segment_clean
        segment_clean=\$(basename "\${consensus_file}" .fasta | sed 's|^output_||')
        # Header looks like ">{segment}|{sampleId}" - pull the segment before "|"
        segment=\$(head -1 "\${consensus_file}" | sed 's|^>||' | cut -d'|' -f1)

        bcftools view -r "\${segment}" -O z \\
          -o cuteSV.filtered.\${segment_clean}.vcf.gz \\
          cuteSV.filtered.vcf.gz 2>/dev/null || \\
        bcftools view -O z \\
          -o cuteSV.filtered.\${segment_clean}.vcf.gz \\
          cuteSV.filtered.vcf.gz
        tabix -p vcf cuteSV.filtered.\${segment_clean}.vcf.gz

        N_SEG_SVS=\$(bcftools view -H cuteSV.filtered.\${segment_clean}.vcf.gz | wc -l)
        if [ "\${N_SEG_SVS}" -gt 0 ]; then
          samtools faidx reference_\${segment_clean}.fasta
          cat reference_\${segment_clean}.fasta \\
            | bcftools consensus --mark-del X cuteSV.filtered.\${segment_clean}.vcf.gz \\
            > output_cutesv_\${segment_clean}.fa
          HEADER=\$(head -1 output_cutesv_\${segment_clean}.fa)
          sed -i "s|\${HEADER}|\${HEADER}_cutesv|g" output_cutesv_\${segment_clean}.fa

          /home/bin/sarscov2/insert_SV_python2.py \\
            \${consensus_file} \\
            output_cutesv_\${segment_clean}.fa \\
            output_\${segment_clean}_SV.fasta
          mv output_\${segment_clean}_SV.fasta \${consensus_file}
        fi
        # else: leave \${consensus_file} (the SNP consensus) untouched
      done

      # ---- 7. Concatenate per-segment files and normalize headers --------------
      # Final headers must look like ">{segment}_SV" to match what consensus_nanopore
      # produces for substitute_ref_genome -> nextalign -> nextclade -> snpEff.
      cat output_*.fasta > consensus_masked_SV.fa
      # Two-pass: handle SV-merged (>{segment}|{sampleId}_SV) then non-SV (>{segment}|{sampleId})
      sed -i "s|\\|${sampleId}_SV|_SV|g" consensus_masked_SV.fa
      sed -i "s|\\|${sampleId}|_SV|g" consensus_masked_SV.fa

      # ---- 8. QC gate: reject if >=90% of bases are N (mirrors manta lines 137-156)
      NUMBER_OF_N=\$(cat consensus_masked_SV.fa | grep -v ">" | fold -w1 | sort | uniq -c | grep " N\$" | awk '{print \$1}')
      SEQ_LENGTH=\$(cat consensus_masked_SV.fa | grep -v ">" | fold -w1 | wc -l)
      if [ -z "\${NUMBER_OF_N}" ]; then
        QC_status_exit="tak"
      else
        if [ \$(awk -v n="\${NUMBER_OF_N}" -v total="\${SEQ_LENGTH}" 'BEGIN {wynik=n/total; if (wynik < 0.9) print "1"; else print "0"}') -eq 1 ]; then
          QC_status_exit="tak"
        else
          QC_status_exit="nie"
        fi
      fi
    fi
    """
}
