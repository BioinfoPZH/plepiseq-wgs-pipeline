process run_fastqc_illumina {
  tag "fastqc for sample ${x}"
  container  = params.main_image
  publishDir "${params.results_dir}/${x}/QC", mode: 'copy'
  cpus { params.threads > 2 ? 2 : params.threads }
  memory "10 GB"
  time "15m"
  input:
  tuple val(x), path(reads), val(QC_STATUS)
  output:
  tuple val(x), path("*csv"), emit: publishdir // Wykresy, kopiujemy bo json wskazuje do publishdir
  tuple val(x), path("forward.json"),  path("reverse.json"), emit: json // Sam json do kopiowania w celu zlozenia "ostatecznego jsona"
  tuple val(x), env(QC_STATUS_EXIT), emit: qcstatus // Sam QC status
  tuple val(x), env(QC_STATUS_EXIT), env(TOTAL_BASES), emit: qcstatus_and_values // QC status + informccje o calkowitej liczbie zasad i ilosci odczytow
  script:
  if (QC_STATUS == null) { QC_STATUS="tak" } // Domyslna wartosc w przypadku gdy user nie poda QC status
  """
  # Set up QC_STATUS to "tak" if a given module does not provide this value via input
  # run_fastqc_and_generate_json.py will always produce output required by these module, even if no valid fastq file is provided, or fastq file
  # does not meet predeifned criteria. The script returns to values status (tak, nie, blad) and total numberof bases in fastqfile (0 if status is not tak)

  if [ ${QC_STATUS} == "nie" ]; then
    if [ "${params.lan}" == "pl" ]; then
      ERROR_MSG="Ten moduł został uruchomiony na próbce, która nie przeszła kontroli jakości."
    else
      ERROR_MSG="This sample failed a QC analysis during an earlier phase of the analysis."
    fi
    touch dummy.csv
    TOTAL_BASES=0
  else
    ERROR_MSG=""
  fi
  
  # If QC_STATUS is 'nie' then the script below will produce a json with ERROR_MSG
 
  DANE_FORWARD=(`python /opt/docker/EToKi/externals/run_fastqc_and_generate_json.py -i ${reads[0]} -m 8096 -c ${task.cpus} -x ${params.min_number_of_reads} -y ${params.min_median_quality} -s ${QC_STATUS} -r "\${ERROR_MSG}" -e pre-filtering -p "${params.results_dir}/${x}/QC" -o forward.json --lan ${params.lan} `)
  STATUS_FORWARD_ALL="\${DANE_FORWARD[0]}"
  BASES_FORWARD="\${DANE_FORWARD[1]}"

  DANE_REVERSE=(`python /opt/docker/EToKi/externals/run_fastqc_and_generate_json.py -i ${reads[1]} -m 8096 -c ${task.cpus} -x ${params.min_number_of_reads} -y ${params.min_median_quality} -s ${QC_STATUS} -r "\${ERROR_MSG}" -e pre-filtering -p "${params.results_dir}/${x}/QC" -o reverse.json --lan ${params.lan}`)
  STATUS_REVERSE_ALL="\${DANE_REVERSE[0]}"
  BASES_REVERSE="\${DANE_REVERSE[1]}"
 
  TOTAL_BASES=`echo "\${BASES_FORWARD} + \${BASES_REVERSE}" | bc -l`

  if [[ \${STATUS_FORWARD_ALL} == "nie"  || \${STATUS_REVERSE_ALL} == "nie"  || \${STATUS_FORWARD_ALL} == "blad"  || \${STATUS_REVERSE_ALL} == "blad" ]]; then
    QC_STATUS_EXIT="nie" # moduly "nizej" dostaja status nie
  else
    QC_STATUS_EXIT="tak"
  fi 
  """
}

process run_fastqc_nanopore {
  // Modification of run_fastqc_illumina 
  // That requires one fastq.gz file
  tag "fastqc for sample ${x}"
  container  = params.main_image
  publishDir "${params.results_dir}/${x}/QC", mode: 'copy'
  cpus { params.threads > 2 ? 2 : params.threads }
  memory "15 GB"
  time "15m"
  input:
  tuple val(x), path(reads), val(QC_STATUS)
  output:
  tuple val(x), path("*csv"), emit: publishdir
  tuple val(x), path("forward.json"), emit: json
  tuple val(x), env(QC_STATUS_EXIT), emit: qcstatus
  tuple val(x), env(QC_STATUS_EXIT), env(TOTAL_BASES), emit: qcstatus_and_values

  script:
  if (QC_STATUS == null) { QC_STATUS="tak" } // if this value was not set assume it is "tak"
  """
  if [ ${QC_STATUS} == "nie" ]; then
    if [ "${params.lan}" == "pl" ]; then
      ERROR_MSG="Ten moduł został uruchomiony na próbce, która nie przeszła kontroli jakości."
    else
      ERROR_MSG="This sample failed a QC analysis during an earlier step of the analysis."
    fi
    touch dummy.csv
    TOTAL_BASES=0
  else
    ERROR_MSG=""
  fi
 
  DANE_FORWARD=(`python /opt/docker/EToKi/externals/run_fastqc_and_generate_json.py -i ${reads} -m 10000 -c ${task.cpus} -x ${params.min_number_of_reads} -y ${params.min_median_quality} -s ${QC_STATUS} -r "\${ERROR_MSG}" -e pre-filtering -p "${params.results_dir}/${x}/QC" -o forward.json --lan ${params.lan}`)
  STATUS_FORWARD="\${DANE_FORWARD[0]}"
  TOTAL_BASES="\${DANE_FORWARD[1]}"

  if [ \${STATUS_FORWARD} != "tak" ]; then
    QC_STATUS_EXIT="nie"
    touch dummy.csv
    TOTAL_BASES=0
  else
    QC_STATUS_EXIT="tak"
  fi

  """
}