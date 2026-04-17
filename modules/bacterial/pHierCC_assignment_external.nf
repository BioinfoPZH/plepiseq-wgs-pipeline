process run_pHierCC_enterobase {
  // Queries the Enterobase API to assign hierCC clusters for a cgMLST ST.
  // Supports Salmonella and Escherichia; other genera produce a short-circuit
  // fallback (Campylobacter is not present in Enterobase).
  // Heavy lifting lives in bin/bacteria/phiercc_enterobase.py, which is
  // COPY'd into the container at /opt/docker/EToKi/externals/.
  container  = params.main_image
  tag "Predicting hierCC from enterobase for sample $x"
  cpus 1
  memory "5 GB"
  time "5m"
  input:
  tuple val(x), path('cgMLST_parsed_output.txt'), path('cgMLST_sample_full_list_of_allels.txt'), path('cgMLST_closest_ST_full_list_of_allels.txt'), val(SPECIES), val(GENUS), val(QC_status), val(QC_status_contaminations)
  output:
  tuple val(x), path('parsed_phiercc_enterobase.txt'), val(SPECIES), val(GENUS), val(QC_status), val(QC_status_contaminations)
  // when:
  // GENUS == 'Salmonella' || GENUS == 'Escherichia'
  script:
"""
python /opt/docker/EToKi/externals/phiercc_enterobase.py \\
    --cgmlst-parsed cgMLST_parsed_output.txt \\
    --species "${SPECIES}" \\
    --genus "${GENUS}" \\
    --qc-status "${QC_status}" \\
    --qc-contam-status "${QC_status_contaminations}" \\
    --api-token "${params.enterobase_api_token}" \\
    --output parsed_phiercc_enterobase.txt
"""
}

process run_pHierCC_pubmlst {
  // PubMLST does not expose hierCC via an API, so we query a locally
  // downloaded sts_table.npy mounted under /db/pubmlst.
  // Heavy lifting lives in bin/bacteria/phiercc_pubmlst.py.
  container  = params.main_image
  containerOptions "--volume ${params.db_absolute_path_on_host}:/db"
  cpus 1
  memory "1 GB"
  time "5m"
  tag "Predicting hierCC with local database for sample $x"
  input:
  tuple val(x), path('cgMLST_parsed_output.txt'), path('cgMLST_sample_full_list_of_allels.txt'), path('cgMLST_closest_ST_full_list_of_allels.txt'), val(SPECIES), val(GENUS),  val(QC_status), val(QC_status_contaminations)
  output:
  tuple val(x), path('parsed_phiercc_pubmlst.txt'), val(SPECIES), val(GENUS),  val(QC_status), val(QC_status_contaminations)
  //when:
  //SPECIES == 'jejuni'
  script:
"""
python /opt/docker/EToKi/externals/phiercc_pubmlst.py \\
    --cgmlst-parsed cgMLST_parsed_output.txt \\
    --species "${SPECIES}" \\
    --genus "${GENUS}" \\
    --qc-status "${QC_status}" \\
    --qc-contam-status "${QC_status_contaminations}" \\
    --db-dir /db/pubmlst \\
    --output parsed_phiercc_pubmlst.txt
"""
}
