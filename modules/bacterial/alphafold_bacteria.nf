
process run_alphafold {
    tag "Alphafold for sample ${x}"
    cpus { params.threads > 20 ? 20 : params.threads }
    maxForks 8
    // Let us leave one GPU free just in case a "failed" process rebounce and quickly ask for an empy GPU
    container  = params.alphafold_image
    containerOptions "--volume ${params.db_absolute_path_on_host}/alphafold:/db --gpus all"
    publishDir "${params.results_dir}/${x}/", mode: 'copy', pattern: "${x}_gyrase_complex.pdb"
    input:
    tuple val(x), path(gff), path(faa), path(ffn), path(tsv), val(SPECIES), val(GENUS), val(QC_status), val(QC_status_contaminations)

    output:
    tuple val(x), path("${x}*.pdb"), emit: to_pubdir // return any number of pdbs produce by this module
    tuple val(x), path('alphafold.json'), emit: json

    script:
    """
    # This function can be used to model multimers with alphafold
    # It requires two additiona databases pdb_seqres_database_path and uniprot_database_path
    run_alpfafold_mer() {
      # --mgnify_database_path="/db/mgnify/mgy_clusters_2022_05.fa"
      # --small_bfd_database_path="/db/small_bfd/bfd-first_non_consensus_sequences.fasta"
      python /app/alphafold/run_alphafold.py  --fasta_paths="\$1" \
                                              --data_dir="/db/" \
                                              --db_preset="reduced_dbs" \
                                              --output_dir="\$2" \
                                              --uniref90_database_path="/db/uniref50/uniref50.fasta" \
                                              --mgnify_database_path="/db/uniref50/uniref50.fasta" \
                                              --small_bfd_database_path="/db/uniref50/uniref50.fasta" \
                                              --template_mmcif_dir="/db/pdb_mmcif/mmcif_files/" \
                                              --max_template_date="2024-01-01" \
                                              --obsolete_pdbs_path="/db/pdb_mmcif/obsolete.dat" \
                                              --use_gpu_relax=true \
                                              --models_to_relax=best \
                                              --model_preset=multimer \
                                              --pdb_seqres_database_path="/db/pdb_seqres/pdb_seqres.txt" \
                                              --uniprot_database_path="/db/uniprot/uniprot_sprot.fasta" \
                                              --num_multimer_predictions_per_model=1

    }
    # For all species as a proof of concept we predict gyrase structure

    # increase number of CPUs for jackhammer and hhblits for alignment
    sed -i s"|n_cpu: int = 8|n_cpu: int = ${task.cpus}|"g /app/alphafold/alphafold/data/tools/jackhmmer.py
    sed -i s"|n_cpu: int = 4|n_cpu: int = ${task.cpus}|"g /app/alphafold/alphafold/data/tools/hhblits.py

    # update alphafold config to run only "model_1"
    sed -i -E "s|'model_1',|['model_1']|g" /app/alphafold/alphafold/model/config.py
    sed -i -zE "s|'model_2',\\s*'model_3',\\s*'model_4',\\s*'model_5',\\s*||g" /app/alphafold/alphafold/model/config.py
    # and 'model_1_multimer_v3'
    sed -i -E "s|'model_1_multimer_v3',|['model_1_multimer_v3']|g" /app/alphafold/alphafold/model/config.py
    sed -i -zE "s|'model_2_multimer_v3',\\s*'model_3_multimer_v3',\\s*'model_4_multimer_v3',\\s*'model_5_multimer_v3',\\s*||g" /app/alphafold/alphafold/model/config.py


    # Determine which GPU is free
    nvidia-smi --query-gpu=memory.used --format=csv,noheader,nounits > tmp_smi.log
    ID=0
    while read L; do
      SMI_1=`echo \${L} | cut -d " " -f1`
      if [ \${SMI_1} -lt 5000 ]; then
        # given gpu uses less than 5Gb of memory
        break
      else
        ID=`echo "\${ID} + 1" | bc -l`
      fi
    done < tmp_smi.log

    # If no device is free than send exit code 1 and retry
    if [ \${ID} -gt 7 ]; then
       exit 1
    fi

    ## Steer alphafold to use available gpu
    export CUDA_VISIBLE_DEVICES=\${ID}

    mkdir wyniki
    if [ ! -e "${faa}" ]; then
       echo "File ${faa} does not exist" >> log
       exit 1
    fi


    if [[ ${QC_status} == "nie"  || ${QC_status_contaminations} == "nie" ]]; then
      # failed QC
      touch ${x}.pdb
      
      if [ "${params.lan}" == "pl" ]; then
        ERR_MSG="Ten moduł został uruchomiony na próbce, która nie przeszła kontroli jakości."
      else
        ERR_MSG="This module was eneterd with failed QC and poduced no valid output"
      fi
      echo -e "{\\"status\\":\\"nie\\",
                \\"error_message\\": \\"\${ERR_MSG}\\"}" >> alphafold.json

    else
      if [[ ${GENUS} == "Salmonella" || ${GENUS} == "Escherichia" || ${GENUS} == "Campylobacter" ]]; then

        grep "gyrase subunit" $faa | tr -d ">" >> names.txt
        seqtk subseq $faa names.txt >> gyrase_complex.fasta

        run_alpfafold_mer gyrase_complex.fasta wyniki
        protein_name="Gyrase"
        cp wyniki/gyrase_complex/relaxed_model_1_multimer_v3_pred_*pdb ${x}_gyrase_complex.pdb
        pdb_path="${params.results_dir}/${x}/${x}_gyrase_complex.pdb"
        echo -e "{\\"status\\":\\"tak\\",
                  \\"protein_structure_data\\":[{\\"protein_name\\":\\"\${protein_name}\\",
                                                 \\"pdb_file\\":\\"\${pdb_path}\\"
                                                }
                                                ]}" >> alphafold.json

      else
        touch ${x}.pdb
        
        if [ "${params.lan}" == "pl" ]; then
          ERR_MSG=`echo Ten modul jest przeznaczony do analizy bakterii z rodzajów: Salmonella, Escherichia oraz Campylobacter. W tej próbce wykryto: ${GENUS}`
        else
          ERR_MSG=`echo This module works with the following genera: Salmonella, Escherichia, or Campylobacter. Following genus was identified in this sample is: ${GENUS}`
        fi

        echo -e "{\\"status\\":\\"nie\\",
                  \\"error_message\\": \\"\${ERR_MSG}\\"}" >> alphafold.json

      fi # if na zly rodzaj
    fi # Error na zle QC
    """

}

process run_alphafold_slurm {
    tag "Alphafold for sample ${x}"
    cpus { params.threads > 20 ? 20 : params.threads }
    memory "250 GB"
    time "1h 30m" 
    clusterOptions "--gpus 1"
    container  = params.alphafold_image
    containerOptions "--volume ${params.db_absolute_path_on_host}/alphafold:/db --gpus=\"device=\${SLURM_JOB_GPUS}\""
    publishDir "${params.results_dir}/${x}/", mode: 'copy', pattern: "${x}_gyrase_complex.pdb"
    input:
    tuple val(x), path(gff), path(faa), path(ffn), path(tsv), val(SPECIES), val(GENUS), val(QC_status), val(QC_status_contaminations)

    output:
    tuple val(x), path("${x}*.pdb"), emit: to_pubdir // return any number of pdbs produce by this module
    tuple val(x), path('alphafold.json'), emit: json

    script:
    """
    # This function can be used to model multimers with alphafold
    # It requires two additiona databases pdb_seqres_database_path and uniprot_database_path
    run_alpfafold_mer() {
      # --mgnify_database_path="/db/mgnify/mgy_clusters_2022_05.fa" 
      # --small_bfd_database_path="/db/small_bfd/bfd-first_non_consensus_sequences.fasta" 
      python /app/alphafold/run_alphafold.py  --fasta_paths="\$1" \
                                              --data_dir="/db/" \
                                              --db_preset="reduced_dbs" \
                                              --output_dir="\$2" \
                                              --uniref90_database_path="/db/uniref50/uniref50.fasta" \
                                              --mgnify_database_path="/db/uniref50/uniref50.fasta" \
                                              --small_bfd_database_path="/db/uniref50/uniref50.fasta" \
                                              --template_mmcif_dir="/db/pdb_mmcif/mmcif_files/" \
                                              --max_template_date="2024-01-01" \
                                              --obsolete_pdbs_path="/db/pdb_mmcif/obsolete.dat" \
                                              --use_gpu_relax=true \
                                              --models_to_relax=best \
                                              --model_preset=multimer \
                                              --pdb_seqres_database_path="/db/pdb_seqres/pdb_seqres.txt" \
                                              --uniprot_database_path="/db/uniprot/uniprot_sprot.fasta" \
                                              --num_multimer_predictions_per_model=1

    } 
    # For all species as a proof of concept we predict gyrase structure

    # increase number of CPUs for jackhammer and hhblits for alignment
    sed -i s"|n_cpu: int = 8|n_cpu: int = ${task.cpus}|"g /app/alphafold/alphafold/data/tools/jackhmmer.py
    sed -i s"|n_cpu: int = 4|n_cpu: int = ${task.cpus}|"g /app/alphafold/alphafold/data/tools/hhblits.py

    # update alphafold config to run only "model_1"
    sed -i -E "s|'model_1',|['model_1']|g" /app/alphafold/alphafold/model/config.py
    sed -i -zE "s|'model_2',\\s*'model_3',\\s*'model_4',\\s*'model_5',\\s*||g" /app/alphafold/alphafold/model/config.py
    # and 'model_1_multimer_v3'
    sed -i -E "s|'model_1_multimer_v3',|['model_1_multimer_v3']|g" /app/alphafold/alphafold/model/config.py
    sed -i -zE "s|'model_2_multimer_v3',\\s*'model_3_multimer_v3',\\s*'model_4_multimer_v3',\\s*'model_5_multimer_v3',\\s*||g" /app/alphafold/alphafold/model/config.py

    mkdir wyniki
    if [ ! -e "${faa}" ]; then
       echo "File ${faa} does not exist" >> log
       exit 1
    fi

    if [[ ${QC_status} == "nie"  || ${QC_status_contaminations} == "nie" ]]; then
      # failed QC
      touch ${x}.pdb
      if [ "${params.lan}" == "pl" ]; then
        ERR_MSG="Ten moduł został uruchomiony na próbce, która nie przeszła kontroli jakości."
      else
        ERR_MSG="This module was eneterd with failed QC and poduced no valid output"
      fi
      
      echo -e "{\\"status\\":\\"nie\\",
                \\"error_message\\": \\"\${ERR_MSG}\\"}" >> alphafold.json

    else
      if [[ ${GENUS} == "Salmonella" || ${GENUS} == "Escherichia" || ${GENUS} == "Campylobacter" ]]; then

        grep "gyrase subunit" $faa | tr -d ">" >> names.txt
        seqtk subseq $faa names.txt >> gyrase_complex.fasta
        
        run_alpfafold_mer gyrase_complex.fasta wyniki
        protein_name="Gyrase"
        cp wyniki/gyrase_complex/relaxed_model_1_multimer_v3_pred_*pdb ${x}_gyrase_complex.pdb
        pdb_path="${params.results_dir}/${x}/${x}_gyrase_complex.pdb"
        echo -e "{\\"status\\":\\"tak\\",
                  \\"protein_structure_data\\":[{\\"protein_name\\":\\"\${protein_name}\\",
                                                 \\"pdb_file\\":\\"\${pdb_path}\\"
                                                }
                                                ]}" >> alphafold.json

      else
        touch ${x}.pdb
        if [ "${params.lan}" == "pl" ]; then
          ERR_MSG=`echo Ten modul jest przeznaczony do analizy bakterii z rodzajów: Salmonella, Escherichia oraz Campylobacter. W tej próbce wykryto: ${GENUS}`
        else
          ERR_MSG=`echo This module works with the following genera: Salmonella, Escherichia, or Campylobacter. Following genus was identified in this sample is: ${GENUS}`
        fi

        echo -e "{\\"status\\":\\"nie\\",
                  \\"error_message\\": \\"\${ERR_MSG}\\"}" >> alphafold.json

      fi # if na zly rodzaj
    fi # Error na zle QC
    """

}

process run_alphafold_dummy {
    tag "Alphafold for sample ${x}"
    cpus { params.threads > 1 ? 1 : params.threads }
    memory "100 MB"
    time "2m"
    container  = params.main_image
    publishDir "${params.results_dir}/${x}/", mode: 'copy', pattern: "${x}_gyrase_complex.pdb"
    input:
    tuple val(x), path(gff), path(faa), path(ffn), path(tsv), val(SPECIES), val(GENUS), val(QC_status), val(QC_status_contaminations)

    output:
    tuple val(x), path("${x}*.pdb"), emit: to_pubdir // return any number of pdbs produce by this module
    tuple val(x), path('alphafold.json'), emit: json


    script:
    """
    touch ${x}.pdb
    if [ "${params.lan}" == "pl" ]; then
        ERR_MSG="Pipeline zostal uruchomiony z flaga no-alphafold. Brak generacji modelu."
    else
        ERR_MSG="Pipeline was executed with no-alphafold flag. No results are created."
    fi

   echo -e "{\\"status\\":\\"nie\\",
             \\"error_message\\": \\"\${ERR_MSG}\\"}" >> alphafold.json

    """
 }