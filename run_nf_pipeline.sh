#!/bin/bash

# Unified script for running both BACTERIAL and VIRAL pipelines
# This script combines the functionality of run_nf_pipeline_bacterial.sh and run_nf_pipeline_viral.sh
# to reduce code duplication and maintain the DRY principle while preserving readability.

# ============================================================================
# COMMON PARAMETERS (shared between both pipelines)
# ============================================================================

# localization of the main file with the pipeline and databases
projectDir=""
external_databases_path="/mnt/raid/external_databases"
results_dir="./results" 

# Nextflow executor
profile="local"

# Run alphafold
run_alphafold="true"

# Parameters related to resources (max PER sample)
threads=40

# Required parameters without defaults
pipeline_type=""  # bacterial or viral
machine=""        # Nanopore or Illumina
reads=""          # Path to reads

# ============================================================================
# BACTERIAL-SPECIFIC PARAMETERS
# ============================================================================

# Docker images for bacterial pipeline
bacterial_main_image="pzh_pipeline_bacterial_main:latest"
bacterial_prokka_image="staphb/prokka:latest"

# Bacterial-specific parameters
genus="" 
quality=""
min_number_of_reads="" 
min_median_quality=""
main_genus_value=""
kmerfinder_coverage=""
main_species_coverage=""
min_genome_length=""
unique_loci=""
contig_number=""
N50=""
final_coverage=""
min_coverage_ratio=""
min_coverage_value=""
model_medaka=""

# ============================================================================
# VIRAL-SPECIFIC PARAMETERS
# ============================================================================

# Docker images for viral pipeline
viral_main_image="pzh_pipeline_viral_main:latest"
viral_manta_image="pzh_pipeline_viral_manta:latest"
viral_medaka_image="ontresearch/medaka:sha447c70a639b8bcf17dc49b51e74dfcde6474837b-amd64"

# Single source of truth for all supported primer schemes
SARSCOV2_PRIMERS=(EQA2023.SARS1 EQA2023.SARS2 EQA2024.V4_1 EQA2024.V4_1.nanopore EQA2024.V5_3 V1 V1200 V2 V3 V4 V4.1 V5.3.2 V5.4.2 VarSkip2 VarSkip2b VarSkip1a VarSkip_long_1a)
RSV_PRIMERS=(V0 V1)
ALL_PRIMERS=("${SARSCOV2_PRIMERS[@]}" "${RSV_PRIMERS[@]}")

# Viral-specific required parameters
primers_id=""
species=""
adapters_id="TruSeq3-PE-2"

# Viral-specific optional parameters
max_number_for_SV=""
min_median_for_SV=""
variant="" 
expected_genus_value="" 
quality_initial="" 
length="" 
max_depth="" 
min_cov="" 
mask="" 
quality_snp="" 
pval="" 
lower_ambig="" 
upper_ambig="" 
window_size="" 
min_mapq="" 
quality_for_coverage="" 
freyja_minq="" 
bed_offset=""
extra_bed_offset=""
medaka_model=""
medaka_chunk_len=""
medaka_chunk_overlap=""
first_round_pval=""
second_round_pval=""

# Common alphafold image
alphafold_image="alphafold2:latest"

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

get_primers_help_text() {
    local sarscov2_list="${SARSCOV2_PRIMERS[*]}"
    local rsv_list="${RSV_PRIMERS[*]}"
    echo "                                  Akceptowane wartosci to ${sarscov2_list} (dla SARS-CoV-2)"
    echo "                                  ${rsv_list} (dla RSV)"
}

show_common_usage() {
    echo "Required parameters/Parametry wymagane:"
    echo "  --pipeline_type VALUE           Pipeline type: bacterial or viral"
    echo "                                  Typ pipeline: bacterial (bakteryjny) lub viral (wirusowy)"
    echo "  --machine VALUE                 Sequencing platform: Nanopore or Illumina"
    echo "                                  Platforma sekwencjonujaca uzyta do analizy. Mozliwe wartosci to Nanopore albo Illumina"
    echo "  --reads PATH                    Path to sequencing data with naming pattern for sequencing files. Use single quotes for this argument"
    echo "                                  Scieżka do katalogu z wynikami sekwencjonowania wraz z wzorcem nazewnictwa plików"
    echo "                                  Format plikow: fastq.gz, Przyklad: '/some/directory/*_R{1,2}.fastq.gz'"
    echo "  --projectDir PATH               Sciezka do katalogu z pobranym repozytorium"
    echo "                                  Directory with projects repository"
    echo "  --external_databases_path PATH  Sciezka do katalogu z pobranymi zewnetrznymi bazami"
    echo "                                  Directory with all databases used by the program"
    echo ""
    echo "Optional parameters:"
    echo "  --results_dir PATH              Path to directory with program's output (default ./results)"
    echo "                                  Sciezka do katalogu z wynikami programu"
    echo "  --threads VALUE                 Thread count (default: $threads)"
    echo "                                  Maksymalna ilosci CPU uzywana do analizy pojedycznej probki"
    echo "  --profile VALUE                 Nazwa profile zdefiniowanego w pliku konfiguaracyjnym nextflow z informacja o executor"
    echo "                                  Name of the profile specified in the nextflow configuration file."
    echo "                                  Available values: \"local\" and \"slurm\". Default value \"local\"."
    echo "  --alphafold_image VALUE         Nazwa obrazu w formacie \"name:tag\" z obrazem zawierajacym program alphafold"
    echo "                                  Name of the docker image with alphafold program"
    echo "  --no-alphafold                  Skip calculations of 3D model with alphafold"
    echo "                                  Omin krok generowania modelu 3D z uzyciem programu alphafold"
    echo "  --all                           Display all parameters for advanced configuration"
    echo "                                  Wyswietl liste wszystkich parametrow modelu"
    echo "  -h, --help                      Show this help message"
}

usage() {
    echo "Usage/Wywolanie: $0 --pipeline_type [bacterial|viral] --machine [Nanopore|Illumina] --reads PATH [options]"
    echo ""
    show_common_usage
    echo ""
    echo "For bacterial pipeline, additional parameters:"
    echo "  --bacterial_main_image VALUE    Name of the docker image with main bacterial program"
    echo "  --bacterial_prokka_image VALUE  Name of the docker image with prokka program"
    echo "  --genus VALUE                   Genus of the bacteria (Salmonella, Escherichia, Campylobacter)"
    echo ""
    echo "For viral pipeline, additional required parameters:"
    echo "  --primers_id VALUE              Name of amplicon schema used"
    get_primers_help_text
    echo "  --species VALUE                 Name of the virus (SARS-CoV-2, Influenza, RSV)"
    echo "  --viral_main_image VALUE        Name of the docker image with main viral program"
    echo "  --viral_manta_image VALUE       Name of the docker image with manta program"
    echo "  --viral_medaka_image VALUE      Name of the docker image with medaka program"
}

show_all_parameters() {
    usage
    echo ""
    echo "===== BACTERIAL PIPELINE - Additional parameters ====="
    echo "  --quality VALUE                 Maximal quality of the base trimmed from 5' and 3' ends (default: 6 for illumina, 2 for nanopore)"
    echo "  --min_number_of_reads VALUE     Minimal number of paired-reads (for illumina) and reads (for nanopore) (default: 50000 for illumina, 10000 for nanopore)"
    echo "  --min_median_quality VALUE      Minimal median quality of bases (default: 10 for illumina, 5 for nanopore)"
    echo "  --main_genus_value VALUE        Minimal percentage of reads classified to main genus (default: 50)"
    echo "  --kmerfinder_coverage VALUE     Minimal coverage for kmerfinder (default: 20)"
    echo "  --main_species_coverage VALUE   Minimal theoretical coverage during species identification (default 20)"
    echo "  --min_genome_length VALUE       Minimal length of final assembly as fraction (default: 0.75)"
    echo "  --unique_loci VALUE             Minimal number of unique loci in MLST (default: 5 for illumina, 0 for nanopore)"
    echo "  --contig_number VALUE           Maximal number of contigs (default: 1000 for Illumina, 100 for Nanopore)"
    echo "  --N50 VALUE                     N50 value (default 30000)"
    echo "  --min_coverage_ratio VALUE      Minimal coverage for contig as fraction (default 0.1)"
    echo "  --final_coverage VALUE          Minimal required coverage value (default: 20)"
    echo "  --min_coverage_value VALUE      Minimal absolute coverage for contig (default: 20)"
    echo "  --model_medaka VALUE            Model used by medaka (default: r941_min_hac_g507)"
    echo ""
    echo "===== VIRAL PIPELINE - Additional parameters ====="
    echo "  --adapters_id VALUE             Adapters used during Illumina sequencing (default: $adapters_id)"
    echo "  --max_number_for_SV VALUE       Maximum number of reads for SV prediction (species-dependent)"
    echo "  --min_median_for_SV VALUE       Minimum median coverage for SVs calling (default 50)"
    echo "  --variant VALUE                 Expected influenza subtype (e.g. H5N1, or UNK for auto-detect)"
    echo "  --min_number_of_reads VALUE     Minimum number of reads (default: 1)"
    echo "  --expected_genus_value VALUE    Percentage of reads associated with genus (default 5%)"
    echo "  --min_median_quality VALUE      Minimum median quality (default: 0)"
    echo "  --quality_initial VALUE         Quality threshold for trimming (default: 5 for Illumina, 2 for Nanopore)"
    echo "  --length VALUE                  Minimal read length after trimming (default: 90 for Illumina, 0.49 for Nanopore)"
    echo "  --max_depth VALUE               Expected coverage after equalization (default: 600)"
    echo "  --min_cov VALUE                 Minimum coverage for SNPs/INDELs (default: 20 for Illumina, 50 for Nanopore)"
    echo "  --mask VALUE                    Coverage threshold for masking (default: 20 for Illumina, 50 for Nanopore)"
    echo "  --quality_snp VALUE             Minimum nucleotide quality for variant calling (default: 15 for Illumina, 5 for Nanopore)"
    echo "  --pval VALUE                    p-value threshold for variants (default: 0.05)"
    echo "  --lower_ambig VALUE             Minimum ratio for ambiguous symbol (default: 0.45)"
    echo "  --upper_ambig VALUE             Maximum ratio for ambiguous symbol (default: 0.55)"
    echo "  --window_size VALUE             Window size for coverage equalization (default: 50)"
    echo "  --min_mapq VALUE                Minimum mapping quality (default: 30)"
    echo "  --quality_for_coverage VALUE    Minimum quality for coverage determination (default: 10 for Illumina, 1 for Nanopore)"
    echo "  --freyja_minq VALUE             Minimum mapping quality for Freyja (default: 20 for Illumina, 2 for Nanopore)"
    echo "  --bed_offset VALUE              Primer boundary offset, Nanopore-specific (default: 10)"
    echo "  --extra_bed_offset VALUE        Extra primer boundary offset, Nanopore-specific (default: 10)"
    echo "  --medaka_model VALUE            Medaka model, Nanopore-specific (default: r941_min_sup_variant_g507)"
    echo "  --medaka_chunk_len VALUE        Medaka chunk length, Nanopore-specific (default: species-dependent)"
    echo "  --medaka_chunk_overlap VALUE    Medaka chunk overlap, Nanopore-specific (default: species-dependent)"
    echo "  --first_round_pval VALUE        p-value for initial genome prediction, Nanopore-specific (default: 0.05)"
    echo "  --second_round_pval VALUE       p-value for genome fine-tuning, Nanopore-specific (default: 0.05)"
}

# ============================================================================
# PARAMETER PARSING
# ============================================================================

# Build the getopt string dynamically based on common and pipeline-specific parameters
COMMON_OPTS="pipeline_type:,machine:,reads:,projectDir:,external_databases_path:,results_dir:,threads:,profile:,alphafold_image:,no-alphafold,all,help"
BACTERIAL_OPTS="bacterial_main_image:,bacterial_prokka_image:,genus:,quality:,min_number_of_reads:,min_median_quality:,main_genus_value:,kmerfinder_coverage:,main_species_coverage:,min_genome_length:,unique_loci:,contig_number:,N50:,final_coverage:,min_coverage_ratio:,min_coverage_value:,model_medaka:"
VIRAL_OPTS="viral_main_image:,viral_manta_image:,viral_medaka_image:,primers_id:,species:,adapters_id:,max_number_for_SV:,min_median_for_SV:,variant:,expected_genus_value:,quality_initial:,length:,max_depth:,min_cov:,mask:,quality_snp:,pval:,lower_ambig:,upper_ambig:,window_size:,min_mapq:,quality_for_coverage:,freyja_minq:,bed_offset:,extra_bed_offset:,medaka_model:,medaka_chunk_len:,medaka_chunk_overlap:,first_round_pval:,second_round_pval:"

OPTS=$(getopt -o h --long ${COMMON_OPTS},${BACTERIAL_OPTS},${VIRAL_OPTS} -- "$@")

eval set -- "$OPTS"

if [[ $# -eq 1 ]]; then
    echo "No parameters provided"
    usage
    exit 1
fi

while true; do
  case "$1" in
    # Common parameters
    --pipeline_type)
      pipeline_type="$2"
      shift 2
      ;;
    --machine)
      machine="$2"
      shift 2
      ;;
    --reads)
      reads="$2"
      shift 2
      ;;
    --projectDir)
      projectDir="$2"
      shift 2
      ;;
    --profile)
      profile="$2"
      shift 2
      ;;
    --external_databases_path)
      external_databases_path="$2"
      shift 2
      ;;
    --results_dir)
      results_dir="$2"
      shift 2
      ;;
    --threads)
      threads="$2"
      shift 2
      ;;
    --alphafold_image)
      alphafold_image="$2"
      shift 2
      ;;
    --no-alphafold)
      run_alphafold="false"
      shift 1
      ;;
    # Bacterial-specific parameters
    --bacterial_main_image)
      bacterial_main_image="$2"
      shift 2
      ;;
    --bacterial_prokka_image)
      bacterial_prokka_image="$2"
      shift 2
      ;;
    --genus)
      genus="$2"
      shift 2
      ;;
    --quality)
      quality="$2"
      shift 2
      ;;
    --min_number_of_reads)
      min_number_of_reads="$2"
      shift 2
      ;;
    --min_median_quality)
      min_median_quality="$2"
      shift 2
      ;;
    --main_genus_value)
      main_genus_value="$2"
      shift 2
      ;;
    --kmerfinder_coverage)
      kmerfinder_coverage="$2"
      shift 2
      ;;
    --main_species_coverage)
      main_species_coverage="$2"
      shift 2
      ;;
    --min_genome_length)
      min_genome_length="$2"
      shift 2
      ;;
    --unique_loci)
      unique_loci="$2"
      shift 2
      ;;
    --contig_number)
      contig_number="$2"
      shift 2
      ;;
    --N50)
      N50="$2"
      shift 2
      ;;
    --final_coverage)
      final_coverage="$2"
      shift 2
      ;;
    --min_coverage_ratio)
      min_coverage_ratio="$2"
      shift 2
      ;;
    --min_coverage_value)
      min_coverage_value="$2"
      shift 2
      ;;
    --model_medaka)
      model_medaka="$2"
      shift 2
      ;;
    # Viral-specific parameters
    --viral_main_image)
      viral_main_image="$2"
      shift 2
      ;;
    --viral_manta_image)
      viral_manta_image="$2"
      shift 2
      ;;
    --viral_medaka_image)
      viral_medaka_image="$2"
      shift 2
      ;;
    --primers_id)
      primers_id="$2"
      shift 2
      ;;
    --species)
      species="$2"
      shift 2
      ;;
    --adapters_id)
      adapters_id="$2"
      shift 2
      ;;
    --max_number_for_SV)
      max_number_for_SV="$2"
      shift 2
      ;;
    --min_median_for_SV)
      min_median_for_SV="$2"
      shift 2
      ;;
    --variant)
      variant="$2"
      shift 2
      ;;
    --expected_genus_value)
      expected_genus_value="$2"
      shift 2
      ;;
    --quality_initial)
      quality_initial="$2"
      shift 2
      ;;
    --length)
      length="$2"
      shift 2
      ;;
    --max_depth)
      max_depth="$2"
      shift 2
      ;;
    --min_cov)
      min_cov="$2"
      shift 2
      ;;
    --mask)
      mask="$2"
      shift 2
      ;;
    --quality_snp)
      quality_snp="$2"
      shift 2
      ;;
    --pval)
      pval="$2"
      shift 2
      ;;
    --lower_ambig)
      lower_ambig="$2"
      shift 2
      ;;
    --upper_ambig)
      upper_ambig="$2"
      shift 2
      ;;
    --window_size)
      window_size="$2"
      shift 2
      ;;
    --min_mapq)
      min_mapq="$2"
      shift 2
      ;;
    --quality_for_coverage)
      quality_for_coverage="$2"
      shift 2
      ;;
    --freyja_minq)
      freyja_minq="$2"
      shift 2
      ;;
    --bed_offset)
      bed_offset="$2"
      shift 2
      ;;
    --extra_bed_offset)
      extra_bed_offset="$2"
      shift 2
      ;;
    --medaka_model)
      medaka_model="$2"
      shift 2
      ;;
    --medaka_chunk_len)
      medaka_chunk_len="$2"
      shift 2
      ;;
    --medaka_chunk_overlap)
      medaka_chunk_overlap="$2"
      shift 2
      ;;
    --first_round_pval)
      first_round_pval="$2"
      shift 2
      ;;
    --second_round_pval)
      second_round_pval="$2"
      shift 2
      ;;
    --all)
      show_all_parameters
      exit 0
      ;;
    -h|--help)
       usage
       exit 0
      ;;
    --)
      shift
      break
      ;;
    *)
      echo "Unknown option: $1"
      usage
      exit 1
      ;;
  esac
done

# ============================================================================
# VALIDATION
# ============================================================================

# Validate pipeline type
if [[ "$pipeline_type" != "bacterial" && "$pipeline_type" != "viral" ]]; then
    echo "Error: --pipeline_type must be either 'bacterial' or 'viral'"
    usage
    exit 1
fi

# Validate common required parameters
if [[ -z "$machine" || -z "$reads" ]]; then
    echo "Error: Missing required parameters (--machine and --reads are mandatory)"
    usage
    exit 1
fi

# Validate profile
if [[ "$profile" != "slurm" && "$profile" != "local" ]]; then
    echo "Error: Profile must be either 'slurm' or 'local'"
    usage
    exit 1
fi

# Validate pipeline-specific required parameters
if [[ "$pipeline_type" == "viral" ]]; then
    if [[ -z "$primers_id" || -z "$species" ]]; then
        echo "Error: Viral pipeline requires --primers_id and --species parameters"
        usage
        exit 1
    fi
    
    # Validate primers
    CORRECT_ID=0
    for var in "${ALL_PRIMERS[@]}"; do
        if [ "${primers_id}" == "${var}" ]; then
            CORRECT_ID=1
            break
        fi
    done
    
    if [ ${CORRECT_ID} -eq 0 ]; then
        echo -e "Error: Invalid primer scheme. Available options: ${ALL_PRIMERS[@]}\n"
        exit 1
    fi
fi

# ============================================================================
# SET DEFAULTS BASED ON PIPELINE TYPE AND MACHINE
# ============================================================================

if [[ "$pipeline_type" == "bacterial" ]]; then
    # Machine-independent bacterial defaults
    [[ -z "${min_coverage_ratio}" ]] && min_coverage_ratio=0.1
    [[ -z "${min_coverage_value}" ]] && min_coverage_value=20
    [[ -z "${main_genus_value}" ]] && main_genus_value=50
    [[ -z "${kmerfinder_coverage}" ]] && kmerfinder_coverage=20
    [[ -z "${main_species_coverage}" ]] && main_species_coverage=20
    [[ -z "${min_genome_length}" ]] && min_genome_length=0.75
    [[ -z "${N50}" ]] && N50=30000
    [[ -z "${final_coverage}" ]] && final_coverage=20

    if [[ "$machine" == "Illumina" ]]; then
        [[ -z "${quality}" ]] && quality=6
        [[ -z "${min_number_of_reads}" ]] && min_number_of_reads=50000
        [[ -z "${min_median_quality}" ]] && min_median_quality=10
        [[ -z "${unique_loci}" ]] && unique_loci=5
        [[ -z "${contig_number}" ]] && contig_number=1000
    elif [[ "$machine" == "Nanopore" ]]; then
        [[ -z "${quality}" ]] && quality=2
        [[ -z "${min_number_of_reads}" ]] && min_number_of_reads=10000
        [[ -z "${min_median_quality}" ]] && min_median_quality=5
        [[ -z "${contig_number}" ]] && contig_number=100
        [[ -z "${model_medaka}" ]] && model_medaka="r941_min_hac_g507"
        [[ -z "${unique_loci}" ]] && unique_loci=0
    else
        echo "Error: Unsupported sequencing platform: $machine. Supported values: Nanopore, Illumina."
        exit 1
    fi

elif [[ "$pipeline_type" == "viral" ]]; then
    # Species-specific defaults
    if [[ "$species" == "SARS-CoV-2" ]]; then
        [[ -z "${max_number_for_SV}" ]] && max_number_for_SV=200000
        [[ -z "${min_median_for_SV}" ]] && min_median_for_SV=50
    elif [[ "$species" == "Influenza" ]]; then 
        [[ -z "${variant}" ]] && variant="UNK"
        [[ -z "${max_number_for_SV}" ]] && max_number_for_SV=10000
        [[ -z "${min_median_for_SV}" ]] && min_median_for_SV=50
    elif [[ "$species" == "RSV" ]]; then
        [[ -z "${max_number_for_SV}" ]] && max_number_for_SV=100000
        [[ -z "${min_median_for_SV}" ]] && min_median_for_SV=50
    else
        echo "Error: Unsupported species: $species. Supported values: SARS-CoV-2, Influenza, RSV."
        exit 1
    fi

    # Machine-specific viral defaults
    if [[ "$machine" == "Illumina" ]]; then
        [[ -z "${min_number_of_reads}" ]] && min_number_of_reads=1
        [[ -z "${expected_genus_value}" ]] && expected_genus_value=5
        [[ -z "${min_median_quality}" ]] && min_median_quality=0
        [[ -z "${quality_initial}" ]] && quality_initial=5
        [[ -z "${length}" ]] && length=90
        [[ -z "${max_depth}" ]] && max_depth=600
        [[ -z "${min_cov}" ]] && min_cov=20
        [[ -z "${mask}" ]] && mask=20
        [[ -z "${quality_snp}" ]] && quality_snp=15
        [[ -z "${pval}" ]] && pval=0.05
        [[ -z "${lower_ambig}" ]] && lower_ambig=0.45
        [[ -z "${upper_ambig}" ]] && upper_ambig=0.55
        [[ -z "${window_size}" ]] && window_size=50 
        [[ -z "${min_mapq}" ]] && min_mapq=30
        [[ -z "${quality_for_coverage}" ]] && quality_for_coverage=10
        [[ -z "${freyja_minq}" ]] && freyja_minq=20
    elif [[ "$machine" == "Nanopore" ]]; then
        [[ -z "${freyja_minq}" ]] && freyja_minq=2
        [[ -z "${bed_offset}" ]] && bed_offset=10
        [[ -z "${extra_bed_offset}" ]] && extra_bed_offset=10 
        [[ -z "${min_mapq}" ]] && min_mapq=30
        [[ -z "${window_size}" ]] && window_size=50
        [[ -z "${length}" ]] && length=0.49
        [[ -z "${medaka_model}" ]] && medaka_model="r941_min_sup_variant_g507"
        
        if [[ "${species}" == 'SARS-CoV-2' ]]; then
            [[ -z "${medaka_chunk_len}" ]] && medaka_chunk_len=5000  
            [[ -z "${medaka_chunk_overlap}" ]] && medaka_chunk_overlap=4000
        elif [[ "${species}" == 'Influenza' ]]; then
            [[ -z "${medaka_chunk_len}" ]] && medaka_chunk_len=1000  
            [[ -z "${medaka_chunk_overlap}" ]] && medaka_chunk_overlap=500
        elif [[ "${species}" == 'RSV' ]]; then
            [[ -z "${medaka_chunk_len}" ]] && medaka_chunk_len=5000
            [[ -z "${medaka_chunk_overlap}" ]] && medaka_chunk_overlap=4000
        fi
        
        [[ -z "${min_number_of_reads}" ]] && min_number_of_reads=1
        [[ -z "${expected_genus_value}" ]] && expected_genus_value=5
        [[ -z "${min_median_quality}" ]] && min_median_quality=0
        [[ -z "${quality_initial}" ]] && quality_initial=2
        [[ -z "${max_depth}" ]] && max_depth=600
        [[ -z "${min_cov}" ]] && min_cov=50
        [[ -z "${mask}" ]] && mask=50
        [[ -z "${quality_snp}" ]] && quality_snp=5
        [[ -z "${pval}" ]] && pval=0.05
        [[ -z "${first_round_pval}" ]] && first_round_pval=0.05
        [[ -z "${second_round_pval}" ]] && second_round_pval=0.05
        [[ -z "${lower_ambig}" ]] && lower_ambig=0.45
        [[ -z "${upper_ambig}" ]] && upper_ambig=0.55
        [[ -z "${window_size}" ]] && window_size=50
        [[ -z "${quality_for_coverage}" ]] && quality_for_coverage=1
    else
        echo "Error: Unsupported sequencing platform: $machine. Supported values: Nanopore, Illumina."
        exit 1
    fi
fi

# ============================================================================
# VALIDATE READS EXIST
# ============================================================================

expanded_reads=$(eval ls ${reads} 2> /dev/null)

if [ $(echo "${expanded_reads}" | wc -w) -lt 1 ]; then
    echo "Error: No reads found in: ${reads}"
    exit 1
fi

# ============================================================================
# RUN APPROPRIATE PIPELINE
# ============================================================================

if [[ "$pipeline_type" == "bacterial" ]]; then
    echo "Running the bacterial pipeline..."
    nextflow run ${projectDir}/nf_pipeline_bacterial.nf \
        --results_dir ${results_dir} \
        --genus ${genus} \
        --reads "${reads}" \
        --machine ${machine} \
        --main_image ${bacterial_main_image} \
        --prokka_image ${bacterial_prokka_image} \
        --alphafold_image ${alphafold_image} \
        --threads ${threads} \
        --db_absolute_path_on_host ${external_databases_path} \
        --min_coverage_ratio ${min_coverage_ratio} \
        --min_coverage_value ${min_coverage_value} \
        --quality ${quality} \
        --min_number_of_reads ${min_number_of_reads} \
        --min_median_quality ${min_median_quality} \
        --main_genus_value ${main_genus_value} \
        --kmerfinder_coverage ${kmerfinder_coverage} \
        --main_species_coverage ${main_species_coverage} \
        --min_genome_length ${min_genome_length} \
        --unique_loci ${unique_loci} \
        --contig_number ${contig_number} \
        --L50 ${N50} \
        --final_coverage ${final_coverage} \
        --model_medaka ${model_medaka} \
        --run_alphafold ${run_alphafold} \
        -profile ${profile} \
        -with-trace

elif [[ "$pipeline_type" == "viral" ]]; then
    echo "Running the viral pipeline..."
    nextflow run ${projectDir}/nf_pipeline_viral.nf \
        --projectDir ${projectDir} \
        --external_databases_path ${external_databases_path} \
        --reads "${reads}" \
        --primers_id ${primers_id} \
        --adapters_id ${adapters_id} \
        --machine ${machine} \
        --species ${species} \
        --main_image ${viral_main_image} \
        --manta_image ${viral_manta_image} \
        --medaka_image ${viral_medaka_image} \
        --alphafold_image ${alphafold_image} \
        --threads ${threads} \
        --variant ${variant} \
        --max_number_for_SV ${max_number_for_SV} \
        --results_dir ${results_dir} \
        --min_number_of_reads ${min_number_of_reads} \
        --expected_genus_value ${expected_genus_value} \
        --min_median_quality ${min_median_quality} \
        --quality_initial ${quality_initial} \
        --length ${length} \
        --max_depth ${max_depth} \
        --min_cov ${min_cov} \
        --mask ${mask} \
        --quality_snp ${quality_snp} \
        --pval ${pval} \
        --lower_ambig ${lower_ambig} \
        --upper_ambig ${upper_ambig} \
        --window_size ${window_size} \
        --min_mapq ${min_mapq} \
        --quality_for_coverage ${quality_for_coverage} \
        --freyja_minq ${freyja_minq} \
        --bed_offset ${bed_offset} \
        --extra_bed_offset ${extra_bed_offset} \
        --medaka_model ${medaka_model} \
        --medaka_chunk_len ${medaka_chunk_len} \
        --medaka_chunk_overlap ${medaka_chunk_overlap} \
        --first_round_pval ${first_round_pval} \
        --second_round_pval ${second_round_pval} \
        --min_median_for_SV ${min_median_for_SV} \
        --run_alphafold ${run_alphafold} \
        -profile ${profile} \
        -with-trace
fi
