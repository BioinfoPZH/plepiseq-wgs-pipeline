process run_pHierCC_local {
  // Looks up hierCC clusters for a cgMLST ST against two locally built
  // plepiseq-cluster profiles (single-linkage and complete-linkage), mounted
  // read-only at /db/phiercc_local. Supports Salmonella, Escherichia and
  // Campylobacter jejuni; other species produce a short-circuit fallback.
  // Heavy lifting lives in bin/bacteria/phiercc_local.py, which is COPY'd
  // into the container at /opt/docker/EToKi/externals/.
  container  = params.main_image
  containerOptions "--volume ${params.db_absolute_path_on_host}:/db"
  cpus 1
  memory "5 GB"
  time "5m"
  tag "Predicting hierCC with local database for sample $x"
  input:
  tuple val(x), path('cgMLST_parsed_output.txt'), path('cgMLST_sample_full_list_of_allels.txt'), path('cgMLST_closest_ST_full_list_of_allels.txt'), val(SPECIES), val(GENUS), val(QC_status), val(QC_status_contaminations)
  output:
  tuple val(x), path('parsed_phiercc_minimum_spanning_tree.txt'), path('parsed_phiercc_maximum_spanning_tree.txt'), emit: to_pubdir
  tuple val(x), path('cgMLST_json_phiercc_local.json'), emit: json
  script:
"""
python /opt/docker/EToKi/externals/phiercc_local.py \\
    --cgmlst-parsed cgMLST_parsed_output.txt \\
    --species "${SPECIES}" \\
    --genus "${GENUS}" \\
    --qc-status "${QC_status}" \\
    --qc-contam-status "${QC_status_contaminations}" \\
    --db-dir /db/phiercc_local \\
    --output-single parsed_phiercc_minimum_spanning_tree.txt \\
    --output-complete parsed_phiercc_maximum_spanning_tree.txt \\
    --output-json cgMLST_json_phiercc_local.json
"""
}
