process alphafold_slurm {
    tag "alphafold:${sampleId}"
    cpus { params.threads > 15 ? 15 : params.threads }
    container  = params.alphafold_image
    memory "250 GB"
    time "40m"
    clusterOptions "--gpus 1"
    containerOptions "--volume ${params.external_databases_path}/alphafold:/db --gpus=\"device=\${SLURM_JOB_GPUS}\""
    publishDir "${params.results_dir}/${sampleId}", mode: 'copy', pattern: "*.pdb"

    input:
    tuple val(sampleId), path("*fasta"), val(QC_status)

    output:
    tuple val(sampleId), path("*.pdb"), emit: to_pubdir // return any number of pdbs produce by this module
    tuple val(sampleId), path('alphafold.json'), emit: json

    script:
    """
    # Original alphafold function, which we dont use
    
    check_fasta_length() {
      local file="\$1"
      local length=`grep -v ">" \${file} | fold -w1 | wc -l`
      echo \${length} 
    }

    run_alpfafold() {
      # Zmienna \$1 to plik z fasta
      # Zmienna \$2 to sciezka do katalogu z wynikami
      python /app/alphafold/run_alphafold.py  --fasta_paths="\$1" \
                                               --data_dir="/db/" \
                                               --db_preset="reduced_dbs" \
                                               --output_dir="\$2" \
                                               --uniref90_database_path="/db/uniref50/uniref50.fasta" \
                                               --mgnify_database_path="/db/mgnify/mgy_clusters_2022_05.fa" \
                                               --small_bfd_database_path="/db/small_bfd/bfd-first_non_consensus_sequences.fasta" \
                                               --template_mmcif_dir="/db/pdb_mmcif/mmcif_files/" \
                                               --max_template_date="2024-05-14" \
                                               --obsolete_pdbs_path="/db/pdb_mmcif/obsolete.dat" \
                                               --use_gpu_relax=true \
                                               --pdb70_database_path="/db/pdb70/pdb70" \
                                               --models_to_relax=best \
                                               --model_preset=monomer

     # Options that must be set when  db_preset is set to "full_dbs" , which is a default setting

        # --bfd_database_path="/db/bfd/bfd_metaclust_clu_complete_id30_c90_final_seq.sorted_opt"
        # --uniref30_database_path="/db/uniref30/UniRef30_2021_03" 
    }

    # This function is reimplementation of run_alphafold 
    # It uses only custom-made uniref50_viral to propose alignment, and replaces the BFD and mgnify databases all together
    run_custom_alpfafold() {
      python /app/alphafold/run_alphafold.py  --fasta_paths="\$1" \
                                               --data_dir="/db/" \
                                               --db_preset="reduced_dbs" \
                                               --output_dir="\$2" \
                                               --uniref90_database_path="/db/uniref50/uniref50_viral.fasta" \
                                               --mgnify_database_path="/db/uniref50/uniref50_viral.fasta" \
                                               --small_bfd_database_path="/db/uniref50/uniref50_viral.fasta" \
                                               --template_mmcif_dir="/db/pdb_mmcif/mmcif_files/" \
                                               --max_template_date="2024-05-14" \
                                               --obsolete_pdbs_path="/db/pdb_mmcif/obsolete.dat" \
                                               --use_gpu_relax=true \
                                               --pdb70_database_path="/db/pdb70/pdb70" \
                                               --models_to_relax=best \
                                               --model_preset=monomer
    }

   
    # Restore names of fasta files as produced by nextalign module
    for link in \$(find . -maxdepth 1 -type l); do
        target=\$(readlink "\$link")
        base_target=\$(basename "\$target")
        mv "\$link" "\$base_target"
    done

    # in case multiple processes are spawned by nextflow at the same time add random sleep...
    # sleep `python -c 'import random; print(random.randint(10, 60))'`

    # increase number of CPUs for jackhammer and hhblits for alignment
    sed -i s"|n_cpu: int = 8|n_cpu: int = ${task.cpus}|"g /app/alphafold/alphafold/data/tools/jackhmmer.py
    sed -i s"|n_cpu: int = 4|n_cpu: int = ${task.cpus}|"g /app/alphafold/alphafold/data/tools/hhblits.py

    # update alphafold config to run only "model_1"
    sed -i -E "s|'model_1',|['model_1']|g" /app/alphafold/alphafold/model/config.py
    sed -i -zE "s|'model_2',\\s*'model_3',\\s*'model_4',\\s*'model_5',\\s*||g" /app/alphafold/alphafold/model/config.py

  
    # directory for alphafold output
    mkdir wynik

    # declare variables 
    pdb_path_1=""
    pdb_path_2=""

    if [ ${QC_status} == "nie" ]; then
      # failed QC 
      touch ${sampleId}.pdb
      if [ "${params.lan}" == "pl" ]; then
        ERR_MSG="Ten moduł został uruchomiony na próbce, która nie przeszła kontroli jakości."
      else
        ERR_MSG="This module was eneterd with failed QC and poduced no valid output"
      fi
 
      echo -e "{\\"status\\":\\"nie\\", 
                \\"error_message\\": \\"\${ERR_MSG}\\"}" >> alphafold.json
    elif [ ${params.species} == "RSV" ]; then
      # For RSV we predict structures of G and F proteins

      cat nextalign_gene_F.translation.fasta | tr -d "-" | tr -d "X" | tr -d "*" >> tmp
      mv tmp nextalign_gene_F.translation.fasta
      target_fasta_F="nextalign_gene_F.translation.fasta"
      F_length=\$(check_fasta_length "\${target_fasta_F}")
 
      if [ \${F_length} -gt 10 ]; then
          run_custom_alpfafold "\${target_fasta_F}" wynik
          cp wynik/`basename \${target_fasta_F} ".fasta"`/ranked_0.pdb ${sampleId}_F.pdb
          pdb_path_1="${params.results_dir}/${sampleId}/${sampleId}_F.pdb"
      fi


      cat nextalign_gene_G.translation.fasta | tr -d "-" | tr -d "X" | tr -d "*" >> tmp
      mv tmp nextalign_gene_G.translation.fasta
      target_fasta_G="nextalign_gene_G.translation.fasta"
      G_length=\$(check_fasta_length "\${target_fasta_G}")
      if [  \${G_length} -gt 10 ]; then
          run_custom_alpfafold "\${target_fasta_G}" wynik
          cp wynik/`basename \${target_fasta_G} ".fasta"`/ranked_0.pdb ${sampleId}_G.pdb
          pdb_path_2="${params.results_dir}/${sampleId}/${sampleId}_G.pdb"
      fi
      
      # align structure to a common reference 
      if [[ -z "\${pdb_path_1}" && -z "\${pdb_path_2}" ]]; then
          # neither protein was analyzed dummy output 
          touch ${sampleId}.pdb
          
          if [ "${params.lan}" == "pl" ]; then
            ERR_MSG="Sekwencje bialek F i G byly zbyt aby zaproponowac ich modele"
          else
            ERR_MSG="Sequence of protein F or G was too short to produce a valid PDB file"
          fi

          echo -e "{\\"status\\":\\"blad\\",
                     \\"error_message\\": \\"\${ERR_MSG}\\"}" > alphafold.json       

      else
          echo -e "{\\"status\\":\\"tak\\",
                    \\"protein_structure_data\\":[{\\"protein_name\\":\\"F\\",
                                                   \\"pdb_file\\":\\"\${pdb_path_1}\\"
                                                  },
                                                  {
                                                  \\"protein_name\\":\\"G\\",
                                                  \\"pdb_file\\":\\"\${pdb_path_2}\\"
                                                 }
                                                 ]}" >> alphafold.json
      fi

    elif [ ${params.species} == "Influenza" ]; then
      cat nextalign_gene_HA.translation.fasta | tr -d "-" | tr -d "X" | tr -d "*" >> tmp
      mv tmp nextalign_gene_HA.translation.fasta
      target_fasta_HA="nextalign_gene_HA.translation.fasta"
      HA_length=\$(check_fasta_length "\${target_fasta_HA}")

      if [ \${HA_length} -gt 10 ]; then
          run_custom_alpfafold "\${target_fasta_HA}" wynik
          cp wynik/`basename \${target_fasta_HA} ".fasta"`/ranked_0.pdb ${sampleId}_HA.pdb 
          pdb_path_1="${params.results_dir}/${sampleId}/${sampleId}_HA.pdb"
      fi
     
      cat nextalign_gene_NA.translation.fasta | tr -d "-" | tr -d "X" | tr -d "*" >> tmp
      mv tmp nextalign_gene_NA.translation.fasta
      target_fasta_NA="nextalign_gene_NA.translation.fasta"
      NA_length=\$(check_fasta_length "\${target_fasta_NA}")
      if [ \${NA_length} -gt 10 ]; then
          run_custom_alpfafold "\${target_fasta_NA}" wynik
          cp wynik/`basename \${target_fasta_NA} ".fasta"`/ranked_0.pdb ${sampleId}_NA.pdb
          pdb_path_2="${params.results_dir}/${sampleId}/${sampleId}_NA.pdb"
      fi
      
      if [[ -z "\${pdb_path_1}" && -z "\${pdb_path_2}" ]]; then
          # neither protein was analyzed dummy output
          touch ${sampleId}.pdb

          if [ "${params.lan}" == "pl" ]; then
            ERR_MSG="Sekwencje bialek NA i HA byly zbyt aby zaproponowac ich modele"
          else
            ERR_MSG="Sequence of protein NA or HA was too short to produce a valid PDB file"
          fi

          echo -e "{\\"status\\":\\"blad\\",
                     \\"error_message\\": \\"\${ERR_MSG}\\"}" > alphafold.json
      else
          echo -e "{\\"status\\":\\"tak\\",
                      \\"protein_structure_data\\":[{\\"protein_name\\":\\"HA\\",
                                                   \\"pdb_file\\":\\"\${pdb_path_1}\\"
                                                   },
                                                   {
                                                    \\"protein_name\\":\\"NA\\",
                                                    \\"pdb_file\\":\\"\${pdb_path_2}\\"
                                                   }
                                                   ]}" >> alphafold.json

      fi
    elif [ ${params.species} == "SARS-CoV-2" ]; then
       if [ -e "nextalign_gene_S.translation.fasta" ]; then
       
         cat nextalign_gene_S.translation.fasta | tr -d "-" | tr -d "X" | tr -d "*" >> tmp
         mv tmp nextalign_gene_S.translation.fasta
         target_fasta_S="nextalign_gene_S.translation.fasta"
         S_length=\$(check_fasta_length "\${target_fasta_S}")
         if [ \${S_length} -gt 10 ]; then
             run_custom_alpfafold "\${target_fasta_S}" wynik
             cp wynik/`basename \${target_fasta_S} ".fasta"`/ranked_0.pdb ${sampleId}_spike.pdb
             pdb_path_1="${params.results_dir}/${sampleId}/${sampleId}_spike.pdb"
         fi
 
         # align all proteins to a common reference
         # TO DO

         if [ -z "\${pdb_path_1}" ]; then
             touch ${sampleId}.pdb

             if [ "${params.lan}" == "pl" ]; then
               ERR_MSG="Sekwencja bialka Spike byla zbyt aby zaproponowac jego modele"
             else
               ERR_MSG="Sequence of the Spike protein was too short to produce a valid PDB file"
             fi

             echo -e "{\\"status\\":\\"blad\\",
                       \\"error_message\\": \\"\${ERR_MSG}\\"}" >> alphafold.json
         else
             echo -e "{\\"status\\":\\"tak\\",
                       \\"protein_structure_data\\":[{\\"protein_name\\":\\"Spike\\",
                                                      \\"pdb_file\\":\\"\${pdb_path_1}\\"
                                                     }]}" >> alphafold.json
         fi
       else
         touch ${sampleId}.pdb

         if [ "${params.lan}" == "pl" ]; then
               ERR_MSG="Brak pliku fasta z sekwencja bialka Spike"
             else
               ERR_MSG="No fasta file with sequence of the Spike protein was provided"
         fi

         echo -e "{\\"status\\":\\"nie\\",
                   \\"error_message\\": \\"\${ERR_MSG}\\"}" >> alphafold.json

      fi
    fi
   """
}
