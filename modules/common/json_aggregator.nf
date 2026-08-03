process json_aggregator_sars_illumina {
    tag "json_aggregator:${sampleId}"
    publishDir "${params.results_dir}/${sampleId}", mode: 'copy', pattern: "${sampleId}.json"
    container  = params.main_image
    containerOptions "--volume ${params.projectDir}:/home/projectDir:ro"
    cpus 1
    memory "20 GB"
    input:
    tuple val(sampleId), path(fastqc_pre_json_forward), path(fastqc_pre_json_reverse),
          path(kraken_contamination),
          path(fastqc_post_json_forward), path(fastqc_post_json_reverse),
          path(mapping_json),
          path(freyja),
          path(coinfection),
          path(dehumanized),
          path(wgsMetrics),
          path(consensus_json),
          path(pangolin_json), 
          path(nextclade_json),
          path(snpeff),
          path(alphafold)
    val(ExecutionDir)
    output:
    path("${sampleId}.json")

    script:
    ExecutionDir = ExecutionDir.replace(".", "")
    """
    if [ -e "/VERSION" ];then
      PIPELINE_VERSION=`cat /VERSION`
    else
      PIPELINE_VERSION="unknown"
    fi

    json_aggregator.py  --version \${PIPELINE_VERSION} \
                        --pathogen "${params.species}" \
                        --sampleId "${sampleId}" \
                        --fastqc_pre "${fastqc_pre_json_forward}" "${fastqc_pre_json_reverse}" \
                        --fastqc_post "${fastqc_post_json_forward}" "${fastqc_post_json_reverse}" \
                        --contamination "${kraken_contamination}" \
                        --freyja "${freyja}" \
                        --coinfection "${coinfection}" \
                        --dehumanized "${dehumanized}" \
                        --wgsMetrics "${wgsMetrics}"  \
                        --consensus "${consensus_json}" \
                        --pangolin "${pangolin_json}" \
                        --nextclade "${nextclade_json}" \
                        --alphafold "${alphafold}" \
                        --snpeff "${snpeff}" \
                        --mapping "${mapping_json}" \
                        --executiondir ${ExecutionDir}

    mv output.json ${sampleId}.json
    """
}

process json_aggregator_rsv_illumina {
    tag "json_aggregator:${sampleId}"
    publishDir "${params.results_dir}/${sampleId}", mode: 'copy', pattern: "${sampleId}.json"
    container  = params.main_image
    containerOptions "--volume ${params.projectDir}:/home/projectDir:ro"
    cpus 1
    memory "20 GB"
    input:
    tuple val(sampleId), path(fastqc_pre_json_forward), path(fastqc_pre_json_reverse),
          path(kraken_contamination),
          path(fastqc_post_json_forward), path(fastqc_post_json_reverse),
          path(mapping_json),
          path(freyja),
          path(dehumanized),
          path(wgsMetrics),
          path(consensus_json),
          path(pangolin_json),
          path(nextclade_json),
          path(snpeff),
          path(alphafold),
          path(resistance_json)
    val(ExecutionDir)

    output:
    path("${sampleId}.json")

    script:
    ExecutionDir = ExecutionDir.replace(".", "")
    """
    if [ -e "/VERSION" ];then
      PIPELINE_VERSION=`cat /VERSION`
    else
      PIPELINE_VERSION="unknown"
    fi

    json_aggregator.py  --version \${PIPELINE_VERSION} \
                        --pathogen "${params.species}" \
                        --sampleId "${sampleId}" \
                        --fastqc_pre "${fastqc_pre_json_forward}" "${fastqc_pre_json_reverse}" \
                        --fastqc_post "${fastqc_post_json_forward}" "${fastqc_post_json_reverse}" \
                        --contamination "${kraken_contamination}" \
                        --dehumanized "${dehumanized}" \
                        --wgsMetrics "${wgsMetrics}"  \
                        --consensus "${consensus_json}" \
                        --pangolin "${pangolin_json}" \
                        --nextclade "${nextclade_json}" \
                        --alphafold "${alphafold}" \
                        --freyja "${freyja}" \
                        --snpeff ${snpeff} \
                        --drug_resistance "${resistance_json}" \
                        --mapping "${mapping_json}" \
                        --executiondir ${ExecutionDir}

    mv output.json ${sampleId}.json
    """
}


process json_aggregator_influenza_illumina {
    tag "json_aggregator:${sampleId}"
    publishDir "${params.results_dir}/${sampleId}", mode: 'copy', pattern: "${sampleId}.json"
    container  = params.main_image
    containerOptions "--volume ${params.projectDir}:/home/projectDir:ro"
    cpus 1
    memory "20 GB"
    input:
    tuple val(sampleId), path(fastqc_pre_json_forward), path(fastqc_pre_json_reverse),
          path(kraken_contamination),
          path(fastqc_post_json_forward), path(fastqc_post_json_reverse),
          path(reassortment_json),
          path(mapping_json),
          path(freyja),
          path(dehumanized),
          path(wgsMetrics),
          path(consensus_json),
          path(pangolin_json),
          path(nextclade_json),
          path(snpeff),
          path(alphafold),
          path(resistance_json)
    val(ExecutionDir)

    output:
    path("${sampleId}.json")

    script:
    ExecutionDir = ExecutionDir.replace(".", "")
    """
    if [ -e "/VERSION" ];then
      PIPELINE_VERSION=`cat /VERSION`
    else
      PIPELINE_VERSION="unknown"
    fi

    json_aggregator.py  --version \${PIPELINE_VERSION} \
                        --pathogen "${params.species}" \
                        --sampleId "${sampleId}" \
                        --fastqc_pre "${fastqc_pre_json_forward}" "${fastqc_pre_json_reverse}" \
                        --fastqc_post "${fastqc_post_json_forward}" "${fastqc_post_json_reverse}" \
                        --contamination "${kraken_contamination}" \
                        --dehumanized "${dehumanized}" \
                        --wgsMetrics "${wgsMetrics}"  \
                        --consensus "${consensus_json}" \
                        --pangolin "${pangolin_json}" \
                        --nextclade "${nextclade_json}" \
                        --snpeff ${snpeff} \
                        --freyja "${freyja}" \
                        --alphafold ${alphafold} \
                        --reassortment ${reassortment_json} \
                        --drug_resistance ${resistance_json} \
                        --mapping "${mapping_json}" \
                        --executiondir ${ExecutionDir}


    mv output.json ${sampleId}.json
    """
}


process json_aggregator_sars_nanopore {
    tag "json_aggregator:${sampleId}"
    publishDir "${params.results_dir}/${sampleId}", mode: 'copy', pattern: "${sampleId}.json"
    container  = params.main_image
    containerOptions "--volume ${params.projectDir}:/home/projectDir:ro"
    cpus 1
    memory "20 GB"
    input:
    tuple val(sampleId), path(fastqc_pre_json_forward),
          path(kraken_contamination),
          path(mapping_json),
          path(freyja),
          path(coinfection),
          path(dehumanized),
          path(wgsMetrics),
          path(consensus_json),
          path(pangolin_json),
          path(nextclade_json),
          path(snpeff),
          path(alphafold)
    val(ExecutionDir)

    output:
    path("${sampleId}.json")

    script:
    ExecutionDir = ExecutionDir.replace(".", "")
    """
    if [ -e "/VERSION" ];then
      PIPELINE_VERSION=`cat /VERSION`
    else
      PIPELINE_VERSION="unknown"
    fi


    json_aggregator.py  --version \${PIPELINE_VERSION} \
                        --pathogen "${params.species}" \
                        --sampleId "${sampleId}" \
                        --fastqc_pre "${fastqc_pre_json_forward}" \
                        --contamination "${kraken_contamination}" \
                        --freyja "${freyja}" \
                        --coinfection "${coinfection}" \
                        --dehumanized "${dehumanized}" \
                        --wgsMetrics "${wgsMetrics}"  \
                        --consensus "${consensus_json}" \
                        --pangolin "${pangolin_json}" \
                        --nextclade "${nextclade_json}" \
                        --alphafold "${alphafold}" \
                        --snpeff ${snpeff} \
                        --mapping "${mapping_json}" \
                        --executiondir ${ExecutionDir}

    mv output.json ${sampleId}.json
    """
}

process json_aggregator_rsv_nanopore {
    tag "json_aggregator:${sampleId}"
    publishDir "${params.results_dir}/${sampleId}", mode: 'copy', pattern: "${sampleId}.json"
    container  = params.main_image
    containerOptions "--volume ${params.projectDir}:/home/projectDir:ro"
    cpus 1
    memory "20 GB"
    input:
    tuple val(sampleId), path(fastqc_pre_json_forward),
          path(kraken_contamination),
          path(mapping_json),
          path(freyja),
          path(dehumanized),
          path(wgsMetrics),
          path(consensus_json),
          path(pangolin_json),
          path(nextclade_json),
          path(snpeff),
          path(alphafold),
          path(resistance_json)
    val(ExecutionDir)

    output:
    path("${sampleId}.json")

    script:
    ExecutionDir = ExecutionDir.replace(".", "")
    """
    
    if [ -e "/VERSION" ];then
      PIPELINE_VERSION=`cat /VERSION`
    else
      PIPELINE_VERSION="unknown"
    fi

    json_aggregator.py  --version \${PIPELINE_VERSION} \
                        --pathogen "${params.species}" \
                        --sampleId "${sampleId}" \
                        --fastqc_pre "${fastqc_pre_json_forward}" \
                        --contamination "${kraken_contamination}" \
                        --dehumanized "${dehumanized}" \
                        --wgsMetrics "${wgsMetrics}"  \
                        --consensus "${consensus_json}" \
                        --pangolin "${pangolin_json}" \
                        --nextclade "${nextclade_json}" \
                        --alphafold "${alphafold}" \
                        --snpeff ${snpeff} \
                        --freyja "${freyja}" \
                        --drug_resistance "${resistance_json}" \
                        --mapping "${mapping_json}" \
                        --executiondir ${ExecutionDir}

    mv output.json ${sampleId}.json
    """
}

process json_aggregator_influenza_nanopore {
    tag "json_aggregator:${sampleId}"
    publishDir "${params.results_dir}/${sampleId}", mode: 'copy', pattern: "${sampleId}.json"
    container  = params.main_image
    containerOptions "--volume ${params.projectDir}:/home/projectDir:ro"
    cpus 1
    memory "20 GB"
    input:
    tuple val(sampleId), path(fastqc_pre_json_forward),
          path(kraken_contamination),
          path(reassortment_json),
          path(mapping_json),
          path(freyja),
          path(dehumanized),
          path(wgsMetrics),
          path(consensus_json),
          path(pangolin_json),
          path(nextclade_json),
          path(snpeff),
          path(alphafold),
          path(resistance_json)
    val(ExecutionDir)

    output:
    path("${sampleId}.json")

    script:
    ExecutionDir = ExecutionDir.replace(".", "")
    """
    
    if [ -e "/VERSION" ];then
      PIPELINE_VERSION=`cat /VERSION`
    else
      PIPELINE_VERSION="unknown"
    fi

    json_aggregator.py  --version \${PIPELINE_VERSION} \
                        --pathogen "${params.species}" \
                        --sampleId "${sampleId}" \
                        --fastqc_pre "${fastqc_pre_json_forward}" \
                        --contamination "${kraken_contamination}" \
                        --dehumanized "${dehumanized}" \
                        --wgsMetrics "${wgsMetrics}"  \
                        --consensus "${consensus_json}" \
                        --pangolin "${pangolin_json}" \
                        --nextclade "${nextclade_json}" \
                        --snpeff ${snpeff} \
                        --freyja "${freyja}" \
                        --alphafold ${alphafold} \
                        --reassortment ${reassortment_json} \
                        --drug_resistance ${resistance_json} \
                        --mapping "${mapping_json}" \
                        --executiondir ${ExecutionDir}

    mv output.json ${sampleId}.json
    """
}

