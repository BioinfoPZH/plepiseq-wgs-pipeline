process resistance_rsv {
    tag "rsv_resistance:${sampleId}"
    container  = params.main_image
    cpus params.threads
    memory "40 GB"
    containerOptions "--volume ${params.external_databases_path}:/home/external_databases/"

    input:
    tuple val(sampleId), path("sample_genome.fasta"), val(QC_status), val(SAMPLE_TYPE)

    output:
    tuple val(sampleId), path("drug_resistance.json"), emit: json

    script:
    """
    if [ ${QC_status} == "nie" ]; then
      if [ "${params.lan}" == "pl" ]; then
        ERR_MSG="Ten moduł został uruchomiony na próbce, która nie przeszła kontroli jakości."
      else
        ERR_MSG="This sample failed a QC analysis during an earlier phase of the analysis."
      fi
      echo '{"resistance_status": "nie", "resistance_error_message": "'\${ERR_MSG}'"}' > drug_resistance.json
    elif [ "${SAMPLE_TYPE}" == "unk" ]; then
      if [ "${params.lan}" == "pl" ]; then
        ERR_MSG="Nie udało się określić podtypu RSV dla tej próbki."
      else
        ERR_MSG="RSV subtype could not be determined for this sample."
      fi
      echo '{"resistance_status": "nie", "resistance_error_message": "'\${ERR_MSG}'"}' > drug_resistance.json
    else
      rsv_resistance_analysis.py sample_genome.fasta \
        /home/external_databases/nextclade \
        /home/data/rsv/fusion_protein \
        ${SAMPLE_TYPE} \
        drug_resistance.json \
        --lan ${params.lan}
    fi
    """
}
