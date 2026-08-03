
process merge_all_subjsons_illumina {
  container  = params.main_image
  cpus 1
  memory "1 GB"
  time "2m"
  tag "Merging all subjsons for sample $x"
  publishDir "${params.results_dir}/${x}/", mode: 'copy', pattern: "${x}.json"
  input:
  tuple val(x), path("sistr.json"), path("seqsero.json"), path("spifinder.json"), path("ectyper.json"), path("virulencefinder.json"), path("prokka.json"), path("alphafold.json"), path("vfdb.json"), val(PATOTYP), path("plasmidfinder.json"), path("amrfinder.json"), path("resfinder.json"), path("cgMLST.json"), path("MLST.json"), path("forward.json"), path("reverse.json"), path("contaminations.json"), path('Genus_species.json'), path("initial_MLST.json"), path("genome_file.json"), path("bacterial_genome.json")
  val(ExecutionDir)
  output:
  path("${x}.json")
  script:
  ExecutionDir = ExecutionDir.replace(".", "")
  """

  if [ -e "/VERSION" ];then
    PIPELINE_VERSION=`cat /VERSION`
  else
    PIPELINE_VERSION="unknown"
  fi

  python /opt/docker/EToKi/externals/prepare_full_json.py --sistr_file sistr.json \
                                                          --seqsero_file seqsero.json \
                                                          --spifinder_file spifinder.json \
                                                          --ectyper_file ectyper.json \
                                                          --virulencefinder_file virulencefinder.json \
                                                          --vfdb_file vfdb.json \
                                                          --patotyp "${PATOTYP}" \
                                                          --plasmidfinder_file plasmidfinder.json \
                                                          --amrfinder_file amrfinder.json \
                                                          --resfinder_file resfinder.json \
                                                          --cgmlst_file cgMLST.json \
                                                          --mlst_file MLST.json \
                                                          --fastqc_forward_file forward.json \
                                                          --fastqc_reverse_file reverse.json \
                                                          --contaminations_file contaminations.json \
                                                          --initial_mlst_file initial_MLST.json \
                                                          --genome_statistics_file bacterial_genome.json \
                                                          --genome_file genome_file.json \
                                                          --genus_species_file Genus_species.json \
                                                          --repo_version "\${PIPELINE_VERSION}" \
                                                          --output ${x}.json \
                                                          --executiondir ${ExecutionDir} \
                                                          --alphafold_file alphafold.json \
                                                          --prokka_file prokka.json
  """

}



process merge_all_subjsons_nanopore {
  container  = params.main_image
  cpus 1
  memory "1 GB"
  time "2m"
  tag "Merging all subjsons for sample $x"
  publishDir "${params.results_dir}/${x}/", mode: 'copy', pattern: "${x}.json" 
  input:
  tuple val(x), path("sistr.json"), path("seqsero.json"), path("spifinder.json"), path("ectyper.json"), path("virulencefinder.json"), path("prokka.json"), path("alphafold.json"), path("vfdb.json"), val(PATOTYP), path("plasmidfinder.json"), path("amrfinder.json"), path("resfinder.json"), path("cgMLST.json"), path("MLST.json"), path("forward.json"), path("contaminations.json"), path('Genus_species.json'), path("initial_MLST.json"), path("genome_file.json"), path("bacterial_genome.json")
  val(ExecutionDir)
  output:
  path("${x}.json")
  script:
  ExecutionDir = ExecutionDir.replace(".", "")
  """
  if [ -e "/VERSION" ];then
    PIPELINE_VERSION=`cat /VERSION`
  else
    PIPELINE_VERSION="unknown"
  fi

  python /opt/docker/EToKi/externals/prepare_full_json.py --sistr_file sistr.json \
                                                          --seqsero_file seqsero.json \
                                                          --spifinder_file spifinder.json \
                                                          --ectyper_file ectyper.json \
                                                          --virulencefinder_file virulencefinder.json \
                                                          --vfdb_file vfdb.json \
                                                          --patotyp "${PATOTYP}" \
                                                          --plasmidfinder_file plasmidfinder.json \
                                                          --amrfinder_file amrfinder.json \
                                                          --resfinder_file resfinder.json \
                                                          --cgmlst_file cgMLST.json \
                                                          --mlst_file MLST.json \
                                                          --fastqc_forward_file forward.json \
                                                          --fastqc_reverse_file skip \
                                                          --contaminations_file contaminations.json \
                                                          --initial_mlst_file initial_MLST.json \
                                                          --genus_species_file Genus_species.json \
                                                          --genome_statistics_file bacterial_genome.json \
                                                          --genome_file genome_file.json \
                                                          --repo_version "\${PIPELINE_VERSION}" \
                                                          --output ${x}.json \
                                                          --executiondir ${ExecutionDir} \
                                                          --alphafold_file alphafold.json \
                                                          --prokka_file prokka.json
  """


}
