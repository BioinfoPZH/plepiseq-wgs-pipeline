process introduce_SV_with_manta {
    // This module iterates over all bam_file provided to this module via picard_downsample
    tag "manta:$sampleId"
    publishDir "${params.results_dir}/${sampleId}/", mode: 'copy', pattern: "output*fasta"
    publishDir "${params.results_dir}/${sampleId}/", mode: 'copy', pattern: "${sampleId}.fasta"
    container = params.manta_image
    cpus { params.threads > 15 ? 15 : params.threads }
    memory "20 GB"
    input:
    tuple val(sampleId), path(bam_files), path(bai_files),  path(ref_genome_with_index), path("mediana_per_segment.txt"), val(QC_status_picard), path(consensus_files), val(QC_status_consensus)

    // zarowno zmienna bam_files, bai_files i consnesus_files sa listami
    // moduly wyzej zapewniaja by nazwy plikow byly latwo parsowalne tzn pliki bam maja nazwe downsampling_NAZWA.bam a pliki fasta
    // dla tego segmentu consesnsu_NAZWA.fasta
    // znajac NAZWA mozemy latwo znajdowac odpowiednie pliki

    output:
    tuple val(sampleId), path('consensus_masked_SV.fa'), path(ref_genome_with_index), env(QC_status_exit), emit: fasta_refgenome_and_qc
    tuple val(sampleId), path('consensus_masked_SV.fa'),  env(QC_status_exit), emit: fasta_and_qc
    tuple val(sampleId), path('consensus.json'), emit: json
    tuple val(sampleId), path('output_*.fasta'), emit: to_pubdir
    tuple val(sampleId), path("${sampleId}.fasta"), emit: merged_fasta
    // Dal ulatwienia ? na koniec tego segmentu polaczmy wszystkie segmenty w jeden plik, a jelsi dany modul downstream
    // bedzie wymagal sekwencji konkretnego segmentu to tam zrobimy split-a

    script:
    def final_index = -1
    ref_genome_with_index.eachWithIndex { filename, index ->
        if (filename.toString().endsWith(".fasta")) {
         final_index = index
        }
    }

    
    """
    export LC_ALL=en_US.utf-8  # bez tego nie dziala click

    if [[ ${QC_status_picard} == "nie"  && ${QC_status_consensus} == "nie" ]]; then
      
      # both consensus module and picard failed, dummy output
      touch consensus_masked_SV.fa
      touch output_dummy.fasta
      # Emit a minimal valid fasta so downstream zawsze ma sciezke pod genome_file_merged
      printf '>dummy\\nN\\n' > ${sampleId}.fasta
      QC_status_exit="nie"
      if [ "${params.lan}" == "pl" ]; then
        ERR_MSG="Ten moduł został uruchomiony na próbce, która nie przeszła kontroli jakości."
      else
        ERR_MSG="This sample failed a QC analysis during an earlier phase of the analysis."
      fi
      python3 /home/parse_make_consensus.py --status "nie" --error "\${ERR_MSG}" -o consensus.json --genome_file_merged "${params.results_dir}/${sampleId}/${sampleId}.fasta"

    elif [[ ${QC_status_picard} == "nie" &&  ${QC_status_consensus} == "tak" ]]; then
      # downsampling failed for ALL segemtns, but consensus produced valid output, ALL input fastas becomes  output files for their respective segemnts
      # We only change the header and create all the files
      QC_status_exit="tak"
  
      for plik in `ls con*fasta`; do
        plik_new_name=`basename \${plik} ".fasta" | sed s'|consensus_||'g`
        HEADER=`head -1 \${plik}`
        cat \${plik} | sed s"|\${HEADER}|\${HEADER}\\|${sampleId}|"g > output_\${plik_new_name}.fasta

      done
      
      # Polaczony plik z wszystkimi segmentami (czyste naglowki, dla uzytkownika)
      cat output_*.fasta > ${sampleId}.fasta

      # make json
      ls output*.fasta | tr " " "\\n" >> list_of_fasta.txt
      python3 /home/parse_make_consensus.py --status "tak" -o consensus.json --input_fastas list_of_fasta.txt --output_path "${params.results_dir}/${sampleId}" --genome_file_merged "${params.results_dir}/${sampleId}/${sampleId}.fasta"

      # merge fastas of individual segments to a single file
      cat output*.fasta >> consensus_masked_SV.fa
      # for now downstream modules require _SV in header's
      sed -i s"|\\|${sampleId}|_SV|"g consensus_masked_SV.fa

    else
      QC_status_exit="tak"
      # For, at least, one segment we need to run manta ... however we do not know for which segment
      # for now we analyze each segment separetly (running  manta , introducing mutations and so on)
      # for future reference maybe we can combine these individual steps into one ?

      # We split reference genome into segments, we ensure that after reference_ 
      # segment name is consistence with consensus_ and downsample_ files

      cat ${ref_genome_with_index[final_index]} | awk '{if (substr(\$0, 1, 1)==">") { new_name=\$0; gsub("\\\\.", "_", new_name); gsub("/", "_", new_name);  filename=("reference_"substr(new_name,2) ".fasta"); print \$0 >> filename } else {print toupper(\$0)  >> filename}}'
      
      for bam in `ls *bam`; do
        segment_clean=`basename \${bam}  ".bam" | cut -d "_" -f2- `
        segment_median_coverage=`cat mediana_per_segment.txt | grep -w \${segment_clean} | cut -f2`
        if [ \${segment_median_coverage} -lt "${params.min_median_for_SV}" ]; then
          # After downsampling segment has poor coverage    
          HEADER=`head -1 consensus_\${segment_clean}.fasta`
          cat consensus_\${segment_clean}.fasta | sed s"|\${HEADER}|\${HEADER}\\|${sampleId}|"g > output_\${segment_clean}.fasta
        else
          samtools faidx reference_\${segment_clean}.fasta
          python /opt/docker/manta/bin/configManta.py --bam \${bam} --reference reference_\${segment_clean}.fasta --runDir Manta_results_\${segment_clean}
          python Manta_results_\${segment_clean}/runWorkflow.py -j ${task.cpus} --quiet
          if [ -e Manta_results_\${segment_clean}/results/variants/diploidSV.vcf.gz ]; then
            # Manta produced an output for this segment
            # Wywalamy skomplikowane SV jak translokacje itd typu BND
            # Wwywalamy rowniez takie SV ktore nie sa homozygotyczne, niepwene mutacje z GT 0/1 beda usuwane 
            bcftools view -O z -o manta_results_\${segment_clean}.vcf.gz -i '(FILTER="PASS" | FILTER="MaxDepth" | FILTER="NoPairSupport") && SVTYPE != "BND" && GT!="het"' Manta_results_\${segment_clean}/results/variants/diploidSV.vcf.gz
            tabix manta_results_\${segment_clean}.vcf.gz
            ILE_SV=`zcat manta_results_\${segment_clean}.vcf.gz | grep SVTYPE | grep -v INFO | grep -v bcftools | wc -l`

            if [ \${ILE_SV} -gt 0 ]; then
              # MAnta produced at least one  valid SV we must integrate them into input fasta
              cat reference_\${segment_clean}.fasta | bcftools consensus --mark-del X -s - manta_results_\${segment_clean}.vcf.gz  > output_manta_\${segment_clean}.fa
              # add _manta do fasta header for manta
              HEADER=`head -1 output_manta_\${segment_clean}.fa`
              sed -i s"|\${HEADER}|\${HEADER}_manta|"g output_manta_\${segment_clean}.fa
              /home/bin/sarscov2/insert_SV_python2.py consensus_\${segment_clean}.fasta output_manta_\${segment_clean}.fa  consensus_\${segment_clean}_SV.fasta
              # podmieniamy naglowek
              sed -i s"|_SV|\\|${sampleId}|"g consensus_\${segment_clean}_SV.fasta
              mv consensus_\${segment_clean}_SV.fasta output_\${segment_clean}.fasta
              
            else
              HEADER=`head -1 consensus_\${segment_clean}.fasta`
              cat consensus_\${segment_clean}.fasta | sed s"|\${HEADER}|\${HEADER}\\|${sampleId}|"g > output_\${segment_clean}.fasta
            fi # koniec if-a na brak SV

          else
            HEADER=`head -1 consensus_\${segment_clean}.fasta`
            cat consensus_\${segment_clean}.fasta | sed s"|\${HEADER}|\${HEADER}\\|${sampleId}|"g > output_\${segment_clean}.fasta
          fi # koniec if-a na brak outputu manty
        fi # koniec if-a na zly coverage
      done # koniec petli na teracje po segmentach

      # Polaczony plik z wszystkimi segmentami (czyste naglowki, dla uzytkownika)
      cat output_*.fasta > ${sampleId}.fasta

      # create json
      ls output*.fasta | tr " " "\\n" >> list_of_fasta.txt
      python3 /home/parse_make_consensus.py --status "tak" -o consensus.json --input_fastas list_of_fasta.txt --output_path "${params.results_dir}/${sampleId}" --genome_file_merged "${params.results_dir}/${sampleId}/${sampleId}.fasta"
      
      # merge all fasta into a single file
      cat output*.fasta >> consensus_masked_SV.fa
      # for now downstream modules REQUIRE _SV in header's

      sed -i s"|\\|${sampleId}|_SV|"g consensus_masked_SV.fa

      # Check QC of the genome at this stage, if some parameters are not met switch QC_status_exit to nie 
      
      # TEMPORAL PARAMETER TO  BE SET WITH TOMEK
      # Number of Ns in sequence is lower than 90% otherwies progam will stop. 
      NUMBER_OF_N=`cat consensus_masked_SV.fa  | grep -v ">" | fold -w1 | sort | uniq -c | grep N | cut -d " " -f3`
      SEQ_LENGTH=`cat consensus_masked_SV.fa  | grep -v ">" | fold -w1 | wc -l`
      
     if [ -z \${NUMBER_OF_N} ]; then 
        # No Ns in a sequence 
        QC_status_exit="tak"
        python3 /home/parse_make_consensus.py --status "\${QC_status_exit}" -o consensus.json --input_fastas list_of_fasta.txt --output_path "${params.results_dir}/${sampleId}" --genome_file_merged "${params.results_dir}/${sampleId}/${sampleId}.fasta"
      else
        if [ `awk -v n="\${NUMBER_OF_N}" -v total="\${SEQ_LENGTH}" 'BEGIN {wynik=n/total; if (wynik < 0.9) print "1"; else print "0"}'` -eq 1 ]; then
          # less than 90% of Ns in sequence we can continue ...
          QC_status_exit="tak"
          python3 /home/parse_make_consensus.py --status "\${QC_status_exit}" -o consensus.json --input_fastas list_of_fasta.txt --output_path "${params.results_dir}/${sampleId}" --genome_file_merged "${params.results_dir}/${sampleId}/${sampleId}.fasta"
        else
          QC_status_exit="nie"
          if [ "${params.lan}" == "pl" ]; then
            ERR_MSG="Sekwencja genomowa dla tej próbki zawiera ponad 90% znaków N. Kolejne moduły nie będą wykonywane."
          else
            ERR_MSG="The genomic sequence for this sample contains more than 90% of Ns. Downstream modules witll not execute."
          fi
          python3 /home/parse_make_consensus.py --status "nie" --error "\${ERR_MSG}" -o consensus.json --genome_file_merged "${params.results_dir}/${sampleId}/${sampleId}.fasta"
        fi
      fi

     fi # koniec if-a na QC

    # in consensus json remove _SV from segment name
    sed -i s'|\\|${sampleId}||'g consensus.json 
    # Fasta headears should follow specific naming scheme "{segment_name}|{sample_name}" but this BREAKS downstream modules
    # Thus to PUBDIR we will push different files than the one pushed to downstream modules 
    # TO DO
    """
}

