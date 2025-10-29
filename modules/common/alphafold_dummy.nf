process alphafold_dummy {
        tag "alphafold:${sampleId}"
    cpus { params.threads > 1 ? 1 : params.threads }
    container  = params.main_image
    memory "100 MB"
    time "2m"
    publishDir "${params.results_dir}/${sampleId}", mode: 'copy', pattern: "*.pdb"

    input:
    tuple val(sampleId), path("*fasta"), val(QC_status)

    output:
    tuple val(sampleId), path("*.pdb"), emit: to_pubdir // return any number of pdbs produce by this module
    tuple val(sampleId), path('alphafold.json'), emit: json

    script:
    """
    touch ${sampleId}.pdb
    if [ "${params.lan}" == "pl" ]; then
        ERR_MSG="Pipeline zostal uruchomiony z flaga no-alphafold. Brak generacji modelu."
    else
        ERR_MSG="Pipeline was executed with no-alphafold flag. No results are created."
    fi
 
   echo -e "{\\"status\\":\\"nie\\", 
             \\"error_message\\": \\"\${ERR_MSG}\\"}" >> alphafold.json
    
    """
 }