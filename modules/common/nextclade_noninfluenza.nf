process nextclade {
    tag "nextclade:${sampleId}"
    container  = params.main_image
    cpus 1
    memory "20 GB"
    // publishDir "${params.results_dir}/${sampleId}", mode: 'copy', pattern: "nextstrain_lineage.csv"
    containerOptions "--volume ${params.external_databases_path}:/home/external_databases/"

    input:
    tuple val(sampleId), path('consensus_masked_SV.fa'), path(ref_genome_with_index), val(QC_status)

    output:
    tuple val(sampleId), path('nextstrain_lineage.json'), emit: json
    tuple val(sampleId), path('nextclade_lineages/nextclade.cds_translation.S.fasta'), env(QC_status_exit), emit:to_modeller

    script:
    def final_index = -1
    ref_genome_with_index.eachWithIndex { filename, index ->
        if (filename.toString().endsWith(".fasta")) {
         final_index = index
        }
    }
    """
    if [ ${QC_status} == "nie" ]; then
      mkdir nextclade_lineages
      touch nextclade_lineages/nextclade.cds_translation.S.fasta
      QC_status_exit="nie"
      if [ "${params.lan}" == "pl" ]; then
        ERR_MSG="Ten moduł został uruchomiony na próbce, która nie przeszła kontroli jakości."
      else
        ERR_MSG="This sample failed a QC analysis during an earlier phase of the analysis."
      fi 
      STATUS="nie"
      DATABASE_NAME="Nextclade"
      SEQ_SOURCE="full_genome"
      
      if [ ${params.species} == "RSV" ]; then
        RSV_TYPE="unk"
        echo -e "[{\\"status\\": \\"\${STATUS}\\", \
                    \\"sequence_source\\": \\"\${SEQ_SOURCE}\\", \
                    \\"database_name\\": \\"\${DATABASE_NAME}\\", \
                    \\"type_name\\": \\"\${RSV_TYPE}\\", \
                    \\"error_message\\": \\"\${ERR_MSG}\\"}]" >> nextstrain_lineage.json 

      elif [ ${params.species} == "SARS-CoV-2" ]; then

        echo -e "[{\\"status\\": \\"\${STATUS}\\", \
                    \\"sequence_source\\": \\"\${SEQ_SOURCE}\\", \
                    \\"database_name\\": \\"\${DATABASE_NAME}\\", \
                    \\"error_message\\": \\"\${ERR_MSG}\\"}]" >> nextstrain_lineage.json
      fi
    else
      # This module need to handel both RSV and SARS. Influenza has a separate module, due to its specific requirements
      # We guess the correct nextclade file by checking fasta header
      HEADER=`head -1 ${ref_genome_with_index[final_index]} | tr -d ">"`
      if [[ "\${HEADER}" == "MN"* ]]; then
        NEXCLADE_FILE="sars-cov-2.zip"
      elif [[ \${HEADER} == "PP525321"* ]]; then
        NEXCLADE_FILE="RSV_A.zip"
        RSV_TYPE="A"
      elif [[ \${HEADER} == "OR666591"* ]]; then
        NEXCLADE_FILE="RSV_B.zip"
        RSV_TYPE="B"
      fi
      
      nextclade run --input-dataset /home/external_databases/nextclade/\${NEXCLADE_FILE} \
                     --output-csv nextstrain_lineage.csv \
                     --output-all nextclade_lineages \
                     consensus_masked_SV.fa
      
      if [ -e nextclade_lineages/nextclade.auspice.json ]; then
        QC_status_exit="tak"
        parse_nextclade_output_csv2json.py --input nextstrain_lineage.csv \
                                           --input2 nextclade_lineages/nextclade.auspice.json \
                                           --output nextstrain_lineage_initial.json \
                                           --sequence_source full_genome \
                                           --lan ${params.lan}
      
       # RSV and SARS have slightly different json output 
        if [ ${params.species} == SARS-CoV-2 ]; then
          # This will put a dictionary in nextstrain_lineage_initial.json inside a list in json file
          jq -s "." nextstrain_lineage_initial.json > nextstrain_lineage.json
        elif [ ${params.species} == RSV ]; then
          echo -e "{\\"type_name\\": \\"\${RSV_TYPE}\\"}" > rsv_type.json
          # This will add rsv type to dictionary from nextclade and put that inside a list
          jq -s "[.[0] * .[1]]" nextstrain_lineage_initial.json rsv_type.json > nextstrain_lineage.json
          
          # file expected by modeller, not produced when analysizng RSV 
          touch nextclade_lineages/nextclade.cds_translation.S.fasta
        fi
      else
        QC_status_exit="nie"
        # create empty files for moddeler module
        mkdir nextclade_lineages || true
        touch nextclade_lineages/nextclade.cds_translation.S.fasta
        # create json for RSV json 
        echo -e "{\\"type_name\\": \\"unk\\"}" > rsv_type.json
        jq -s "." rsv_type.json > nextstrain_lineage.json
        
      fi

    fi
    """
}
