process introduce_SV_with_cutesv {
    // SARS-CoV-2 Nanopore SV caller. Plugs in between consensus_nanopore and
    // substitute_ref_genome. Mirrors the FASTA-layer integration pattern from
    // modules/common/manta.nf: per-segment loop, bcftools consensus --mark-del X
    // against the segment reference, then insert_SV_python2.py merges the
    // SV-derived deletions into the per-segment SNP consensus.
    tag "cutesv:$sampleId"
    publishDir "${params.results_dir}/${sampleId}/", mode: 'copy', pattern: "output_*.fasta"
    publishDir "${params.results_dir}/${sampleId}/", mode: 'copy', pattern: "${sampleId}.fasta"
    container = params.main_image
    cpus { params.threads > 15 ? 15 : params.threads }
    memory "20 GB"

    input:
    tuple val(sampleId), \
          path(bam), \
          path(bai), \
          val(QC_status_minimap), \
          path('ref_genome.fasta'), \
          path('primers.bed'), \
          path(consensus_files), \
          path('genome.fasta'), \
          val(QC_status_consensus)

    output:
    tuple val(sampleId), path("output_*.fasta"), val(QC_status_consensus), emit: multiple_fastas
    tuple val(sampleId), path('output.fasta'), path('ref_genome.*'), val(QC_status_consensus), emit: fasta_refgenome_and_qc
    tuple val(sampleId), path("consensus.json"), emit: json
    tuple val(sampleId), path("${sampleId}.fasta"), emit: merged_fasta

    script:
    """
    # cuteSV parameters
    CUTESV_MIN_SUPPORT=2              # cuteSV --min_support (permissive caller threshold)
    CUTESV_MIN_READ_LEN=50            # cuteSV --min_read_len
    CUTESV_MIN_DV=10                  # post-filter: minimum variant-supporting reads
    CUTESV_MIN_VAF=0.45               # post-filter: minimum DV/(DR+DV)
    CUTESV_MAX_SV_LENGTH=3000         # post-filter: upper bound on |SVLEN|
    CUTESV_MIN_SV_LENGTH_FALLBACK=500 # used only if amplicon-1 parsing fails

    # depth-based confirmation (step 4b): reject calls without an amplicon-dropout
    # signature even if they survived the per-call DV/VAF/length gate.
    CUTESV_DEPTH_FLANK_BP=400         # bp of flank window on each side of the SV
    CUTESV_MAX_INSIDE_RATIO=0.20      # reject if mean(depth_inside) / mean(depth_flank) > this
    CUTESV_MIN_FLANK_DEPTH=20         # safety floor: below this the ratio is meaningless


    if [[ "${QC_status_minimap}" == "nie" || "${QC_status_consensus}" == "nie" ]]; then
      
      touch output.fasta
      touch output_dummy.fasta
      touch ref_genome.fasta
      touch ref_genome.fasta.fai
      # Emit a minimal valid fasta so downstream zawsze ma sciezke pod genome_file_merged
      printf '>dummy\\nN\\n' > ${sampleId}.fasta
      
      if [ "${params.lan}" == "pl" ]; then
        ERR_MSG="Ten moduł został uruchomiony na próbce, która nie przeszła kontroli jakości."
      else
        ERR_MSG="This sample failed a QC analysis during an earlier phase of the analysis."
      fi
      
      parse_make_consensus.py --status "nie" --error "\${ERR_MSG}" -o consensus.json --genome_file_merged "${params.results_dir}/${sampleId}/${sampleId}.fasta"

    else

      ## 1. determine MIN_SV_LEN (we want to ientify deletion that are >= 0.8 * amplicon length) shorter deletion should be identified with medaka


      LEFT_START=\$(awk '\$4 ~ /_1_LEFT(_alt|_bis)?\$/ {print \$2; exit}' primers.bed)
      RIGHT_END=\$(awk '\$4 ~ /_1_RIGHT(_alt|_bis)?\$/ {print \$3; exit}' primers.bed)

      if [[ -n "\${LEFT_START}" && -n "\${RIGHT_END}" && "\${RIGHT_END}" -gt "\${LEFT_START}" ]]; then
        AMPLICON_LEN=\$((RIGHT_END - LEFT_START))
        MIN_SV_LEN=\$(python3 -c "print(int(0.8 * \${AMPLICON_LEN}))")
      else
        MIN_SV_LEN=\${CUTESV_MIN_SV_LENGTH_FALLBACK}
      fi

      ## 2. Run cuteSV

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
        --genotype >> log 2>&1

      ## 3. Extract deletions to TSV 

      bcftools view -i 'INFO/SVTYPE="DEL"' cuteSV.genotyped.vcf \\
        | bcftools query \\
            -f '%CHROM\\t%POS\\t%INFO/END\\t%ID\\t%FILTER\\t%INFO/SVLEN\\t%INFO/RE[\\t%DR\\t%DV\\t%GQ]\\t%INFO/RNAMES\\n' \\
        > cuteSV.deletions.genotyped.tsv

      ## 4. empirical filters to keep only probable mutations
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

      ## 4b. depth-based confirmation: a real amplicon-dropout deletion has
      ##     near-zero coverage inside its breakpoints and full coverage in the
      ##     flanks. cuteSV can be tricked by chimeric reads or repeat-induced
      ##     soft-clips into nominating regions that have full coverage in
      ##     between - those calls are rejected here. The full audit table is
      ##     always emitted (depth_in / depth_flank / depth_ratio / depth_verdict)
      ##     so post-hoc inspection stays cheap; cuteSV.depth_filtered.tsv then
      ##     contains only the rows that survived.
      {
        echo -e "chrom\\tstart\\tend\\tid\\tlen\\tfilter\\tDR\\tDV\\tVAF\\tGQ\\tdepth_in\\tdepth_flank\\tdepth_ratio\\tdepth_verdict"
        tail -n +2 cuteSV.filtered.tsv | while IFS=\$'\\t' read -r CHROM START END ID LEN FILTER DR DV VAF GQ; do
          CHR_LEN=\$(awk -v c="\$CHROM" '\$1==c {print \$2; exit}' ref_genome.fasta.fai)
          FLO=\$((START - CUTESV_DEPTH_FLANK_BP)); [ "\$FLO" -lt 1 ] && FLO=1
          FHI=\$((END + CUTESV_DEPTH_FLANK_BP))
          if [ -n "\$CHR_LEN" ] && [ "\$FHI" -gt "\$CHR_LEN" ]; then FHI=\$CHR_LEN; fi

          D_IN=\$(samtools depth -a -r "\${CHROM}:\${START}-\${END}" ${bam} \\
                 | awk '{s+=\$3;n++} END {if(n) printf "%.1f", s/n; else print 0}')
          D_FL=\$(samtools depth -a -r "\${CHROM}:\${FLO}-\${START}" -r "\${CHROM}:\${END}-\${FHI}" ${bam} \\
                 | awk '{s+=\$3;n++} END {if(n) printf "%.1f", s/n; else print 0}')

          read RATIO VERDICT < <(awk -v din="\$D_IN" -v dfl="\$D_FL" \\
                                      -v maxr="\${CUTESV_MAX_INSIDE_RATIO}" \\
                                      -v minfl="\${CUTESV_MIN_FLANK_DEPTH}" \\
              'BEGIN {
                 ratio = (dfl > 0) ? din / dfl : 0
                 verdict = (dfl >= minfl && ratio <= maxr) ? "pass" : "fail"
                 printf "%.4f %s\\n", ratio, verdict
               }')

          echo -e "\${CHROM}\\t\${START}\\t\${END}\\t\${ID}\\t\${LEN}\\t\${FILTER}\\t\${DR}\\t\${DV}\\t\${VAF}\\t\${GQ}\\t\${D_IN}\\t\${D_FL}\\t\${RATIO}\\t\${VERDICT}"
        done
      } > cuteSV.depth_audit.tsv

      awk -F'\\t' 'NR==1 || \$NF=="pass"' cuteSV.depth_audit.tsv > cuteSV.depth_filtered.tsv

      ## 5. Re-derive a small filtered VCF from the IDs that survived
      ##    both the per-call gate (step 4) and the depth gate (step 4b).


      bgzip -f cuteSV.genotyped.vcf
      tabix -p vcf cuteSV.genotyped.vcf.gz
      awk 'NR>1 {print \$4}' cuteSV.depth_filtered.tsv > cuteSV.passed_ids.txt

      if [ -s cuteSV.passed_ids.txt ]; then
        bcftools view -O z -o cuteSV.filtered.vcf.gz \\
          -i 'ID=@cuteSV.passed_ids.txt' cuteSV.genotyped.vcf.gz
      else
        bcftools view -O z -o cuteSV.filtered.vcf.gz -h cuteSV.genotyped.vcf.gz
      fi
      tabix -p vcf cuteSV.filtered.vcf.gz

      ## 6. Apply the filtered SV VCF onto genome.fasta (the reference) to produce
      ##    an SV-only genome. --mark-del X replaces each deletion span with a run
      ##    of 'X' characters so the next step (FASTA-level merge with the SNP
      ##    consensus) can detect the SV intervals, exactly like the manta path.
      ##    If cuteSV.filtered.vcf.gz is header-only, bcftools consensus just
      ##    copies genome.fasta through unchanged.
      samtools faidx genome.fasta
      bcftools consensus -f genome.fasta --mark-del X cuteSV.filtered.vcf.gz > genome_SV.fasta


      samtools faidx genome_SV.fasta
      for plik in preSV_output_*.fasta; do
        segment_clean=\$(basename "\$plik" .fasta | sed 's/^preSV_output_//')
        segment_id=\$(head -1 "\$plik" | sed 's/^>//' | cut -d'|' -f1)
        samtools faidx genome_SV.fasta "\${segment_id}" > genome_SV_\${segment_clean}.fasta
        insert_SV_python2.py "\$plik" genome_SV_\${segment_clean}.fasta merged_\${segment_clean}.fasta
        sed -i '1s|_SV\$||' merged_\${segment_clean}.fasta
        mv merged_\${segment_clean}.fasta output_\${segment_clean}.fasta
      done

      #  This step is required only for integration with downstream illumina modules
      # and we need to change name of the genome.fasta file to ref_genome.fasta
      # and index it 
      mv genome.fasta ref_genome.fasta
      bwa index ref_genome.fasta

      # Polaczony plik z wszystkimi segmentami (czyste naglowki, dla uzytkownika).
      # Robione przed sed'em na consensus.json, zeby plik trafil w publishDir z oryginalnymi naglowkami.
      cat output_*.fasta > ${sampleId}.fasta

      # prepare json for this step including list of files 
      ls output_*.fasta | tr " " "\\n" >> list_of_fasta.txt
      parse_make_consensus.py --status "tak" -o consensus.json --input_fastas list_of_fasta.txt --output_path "${params.results_dir}/${sampleId}" --genome_file_merged "${params.results_dir}/${sampleId}/${sampleId}.fasta"
      cat  output_*.fasta >> output.fasta # all segments
      sed -i s"|\\|${sampleId}||"g output.fasta
      sed -i s"|\\|${sampleId}||"g consensus.json
    fi
    """
}
