process consensus_illumina {
    // For illumina this module provides sample sequnce PRIOR to introduction of SVs
    // That is why for illumina valid sequence and valid json is provided by manta module 
    tag "consensus:${sampleId}"
    container  = params.main_image
    memory "20 GB"
    cpus 1
    input:
    tuple val(sampleId), path(masked_ref_genome_fa), path(varscan_fa), path(freebayes_fa), val(QC_status), path(lofreq_fa)

    output:
    tuple val(sampleId), path("consensus_*.fasta"), val(QC_status), emit: multiple_fastas

    script:
    """
    if [ ${QC_status} == "nie" ]; then
      touch consensus_dummy.fasta
    else
      make_consensus.py ${masked_ref_genome_fa} ${freebayes_fa} ${lofreq_fa} ${varscan_fa}
    fi
    """
}

process consensus_nanopore_one_segment {
    // For SARS-CoV-2 and RSV this module goes next to cuteSV step 
    tag "consensus:${sampleId}"
    memory "20 GB"
    container  = params.main_image
    cpus 1
    input:
    tuple val(sampleId), path(masked_ref_genome_fa), path(sample_genome), val(QC_status), path('genome.fasta')

    output:
    tuple val(sampleId), path("preSV_output_*.fasta"), path('genome.fasta'), val(QC_status), emit: multiple_fastas


    script:
    """
    if [ ${QC_status} == "nie" ]; then
      touch preSV_output_dummy.fasta
      touch ref_genome.fasta
      touch ref_genome.fasta.fai

    else
      if [ ${params.species} == "Influenza" ]; then
        cat ${sample_genome} | grep ">" | awk '{print substr(\$0,2), 0, 12}' | tr " " "\t" >> bed.bed
        bedtools maskfasta -fi ${sample_genome} -bed bed.bed -fo tmp.fasta
	      mv tmp.fasta ${sample_genome}
      fi

      make_consensus_nanopore.py ${masked_ref_genome_fa} ${sample_genome} ${sampleId}
      # add preSV_ prefix to all output*.fasta files
      for f in output*.fasta; do mv -- "\$f" "preSV_\$f"; done

    fi
    """
}


process consensus_nanopore {
    // For nanopore this module provides FINAL sequence for a sample
    // The output should be equivalent to manta module for illumina_path
    tag "consensus:${sampleId}"
    memory "20 GB"
    container  = params.main_image
    publishDir "${params.results_dir}/${sampleId}", mode: 'copy', pattern: "output_*.fasta"
    publishDir "${params.results_dir}/${sampleId}", mode: 'copy', pattern: "${sampleId}.fasta"
    cpus 1
    input:
    tuple val(sampleId), path(masked_ref_genome_fa), path(sample_genome), val(QC_status), path('genome.fasta')

    output:
    // tuple val(sampleId), path("consensus.fasta"), val(QC_status), emit: single_fasta
    tuple val(sampleId), path("output_*.fasta"), val(QC_status), emit: multiple_fastas
    tuple val(sampleId), path('output.fasta'), path('ref_genome.*'), val(QC_status), emit: fasta_refgenome_and_qc
    tuple val(sampleId), path("consensus.json"), emit: json
    tuple val(sampleId), path("${sampleId}.fasta"), emit: merged_fasta

    script:
    """
    if [ ${QC_status} == "nie" ]; then
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
      if [ ${params.species} == "Influenza" ]; then
        cat ${sample_genome} | grep ">" | awk '{print substr(\$0,2), 0, 12}' | tr " " "\t" >> bed.bed
        bedtools maskfasta -fi ${sample_genome} -bed bed.bed -fo tmp.fasta
	mv tmp.fasta ${sample_genome}
      fi

      make_consensus_nanopore.py ${masked_ref_genome_fa} ${sample_genome} ${sampleId}
      rm output.fasta
     
      # This step is required only for integration with downstream illumina modules
      cp genome.fasta ref_genome.fasta
      bwa index ref_genome.fasta

      # Polaczony plik z wszystkimi segmentami (czyste naglowki, dla uzytkownika).
      # Tworzony PRZED sed'em ktory dodaje _SV do output.fasta (uzywany downstream).
      cat output_*.fasta > ${sampleId}.fasta

      # prepare json for this step
      ls output_*.fasta | tr " " "\\n" >> list_of_fasta.txt
      parse_make_consensus.py --status "tak" -o consensus.json --input_fastas list_of_fasta.txt --output_path "${params.results_dir}/${sampleId}" --genome_file_merged "${params.results_dir}/${sampleId}/${sampleId}.fasta"
      cat  output_*.fasta >> output.fasta 
      sed -i s"|\\|${sampleId}|_SV|"g output.fasta
      sed -i s"|\\|${sampleId}||"g consensus.json
    fi
    """
}