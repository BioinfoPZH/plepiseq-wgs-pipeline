#!/bin/bash


# Simplified script for running the VIRAL pipeline
# The following parameters should be specified by a user:
# (I) name of the expected organism in a sample
# (II) directory with reads
# (III) sequencing platform
# (IV) amplicon scheme name / primers
# (V) docker images for main program, manta, alphafold2 and medaka
# (VI) directory with extrernal data
# (VII) path to the directory with main repository
# (VIII) nextflow executor
 
# fhave PREDEFINED values that, if one chooses to, can be still modified (for testing purpose)

# For simplicity we do NOT test arguments of options I-IV
# Options from V-VIII have predefined values based on default installation of the pipeline from the documentation

# Single source of truth for all supported primer schemes
# SARS-CoV-2 primers
SARSCOV2_PRIMERS=(EQA2023.SARS1 EQA2023.SARS2 EQA2024.V4_1 EQA2024.V4_1.nanopore EQA2024.V5_3 Artic_V1 Artic_V2 Artic_V3 Artic_V4 Artic_V4.1 Artic_V5.3.2 Artic_V5.4.2 Midnight_1200nt VarSkip_V1a VarSkip_V2 VarSkip_V2b VarSkip_V1a_long EQA2026.SARS1 EQA2026.SARS2)
# RSV primers  
RSV_PRIMERS=(RSV_WHO-2015 RSV_Artic_V1 RSV_EQA20206.illumina RSV_EQA20206.nanopore)
# Influenza primers (placeholder -- subtype-specific primers are selected automatically)
INFLUENZA_PRIMERS=(UniRef)
# Combined list for validation
ALL_PRIMERS=("${SARSCOV2_PRIMERS[@]}" "${RSV_PRIMERS[@]}" "${INFLUENZA_PRIMERS[@]}")

# Helper function to format primers list for help text
get_primers_help_text() {
    local sarscov2_list="${SARSCOV2_PRIMERS[*]}"
    local rsv_list="${RSV_PRIMERS[*]}"
    local influenza_list="${INFLUENZA_PRIMERS[*]}"
    echo "                                  Akceptowane wartosci to ${sarscov2_list} (dla SARS-CoV-2)"
    echo "                                  ${rsv_list} (dla RSV)"
    echo "                                  ${influenza_list} (dla Influenza)"
}

# Parameters I-IV without DEFAULTS that MUST be specified by a user (adapters_id) is an execpetion
machine="" # Only Nanopore or Illumina
reads="" # Existing path
primers_id="" # Existing path
adapters_id="TruSeq3-PE-2"
species="" # Only SARS-CoV-2, Influenza or RSV

# Parameteres V-VIII
## localization of the file with and databases required to execute this pipeline
## Existing directories, for testing purpose can be change. but for production invariable
projectDir=""
external_databases_path="/mnt/raid/external_databases"
results_dir="./results"

## docker images required to execute this pipeline
## Existing images, for testing purpose can be change, but for production invariable
main_image="plepiseq-wgs-pipeline-viral:latest"
manta_image="plepiseq-wgs-pipeline-manta:latest"
medaka_image="ontresearch/medaka:sha447c70a639b8bcf17dc49b51e74dfcde6474837b-amd64"
alphafold_image="plepiseq-wgs-pipeline-alphafold:latest"

## Nextflow executor
profile="local"

# Parmaters related to the resources available to the pipeline (max PER sample) 
# if N samples are analyzed the pipeline will use at most N times threads
# For testing purpose can be change, but for production invariable
threads=40

run_alphafold="true"

# Parameters with default values that depend on the orgasnism beeing processed

## For all species
max_number_for_SV=""
min_median_for_SV=""
## Only for influenza
variant="" 

# Parameters with default values that depend on the sequencing platform
## For both platforms
min_number_of_reads="" 
expected_genus_value="" 
min_median_quality="" 
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
## Nanopore specific
bed_offset=""
extra_bed_offset=""
medaka_model=""
medaka_chunk_len=""
medaka_chunk_overlap=""
first_round_pval=""
second_round_pval=""


# Helper function to display common parameters shared between usage() and show_all_parameters()
show_common_parameters() {
    echo "Required parameters/Parametry wymagane:"
    echo "  --machine VALUE                 Sequencing platform: Nanopore or Illumina"
    echo "                                  Platforma sekwencjonujaca uzyta do analizy. Mozliwe wartosci to Nanopore albo Illumina"
    echo "  --reads PATH                    Path to sequencing data with naming pattern for sequencing files. Use single quotes for this argument"
    echo "                                  Scieżka do katalogu z wynikami sekwencjonowania wraz z wzorcem nazewnictwa plików"
    echo "                                  Format plikow: fastq.gz, Przyklad: '/some/directory/*_R{1,2}.fastq.gz'"
    echo "  --primers_id VALUE              Name of amplicon schema used to amplify genetic materia"
    echo "                                  Nazwa schematu amplikonów użyta w eksperymencie"
    get_primers_help_text
    echo "  --species VALUE                 Name of the virus that underwent sequencing"
    echo "                                  Nazwa wirusa poddanego sekwencjonowaniu"
    echo "                                  Akceptowane wartosci to SARS-CoV-2 Influenza RSV"
    echo "  --projectDir PATH               Sciezka do katalogu z pobranym repozytorium"
    echo "                                  Directory with projects repository"
    echo "  --external_databases_path PATH  Sciezka do katalogu z pobranymi zewnetrznymi bazami"
    echo "                                  Directory with all databases used by the program"
    echo "  --main_image VALUE              Nazwa obrazu w formacie \"name:tag\" z obrazem zawierajacym programy uzywane przez pipeline"
    echo "                                  Name of the docker image with main program"
    echo "  --manta_image VALUE             Nazwa obrazu w formacie \"name:tag\" z obrazem zawierajacym program manta. Uzywana tylko w przypadku gdy parametr machine to Illumina"
    echo "                                  Name of the docker image with manta program, used only when processing Illumina data"
    echo "  --medaka_image VALUE            Nazwa obrazu w formacie \"name:tag\" z obrazem zawierajacym program medaka. Uzywana tylko w przypadku gdy parametr machine to Nanopore"
    echo "                                  Name of the docker image with medaka program required to process Nanopore data"
    echo "  --alphafold_image VALUE         Nazwa obrazu w formacie \"name:tag\" z obrazem zawierajacym program alphafold"
    echo "                                  Name of the docker image with alphafold program"
    echo "Optional parameters:"
    echo "  --adapters_id VALUE             Adapters used during Illumina-based sequencing (default: $adapters_id)"
    echo "                                  Nazwa adapterow stosowanych podczas sekwencjonowania z wykorzysyniem platformy Illumina"
    echo "                                  Akceptowane wartosci NexteraPE-PE TruSeq2-PE TruSeq2-SE TruSeq3-PE-2 TruSeq3-PE TruSeq3-SE"
    echo "  --threads VALUE                 Maximum number of CPUs allocated to a single module within Nextflow. (default: $threads)"
    echo "                                  Maksymalna ilosci CPU uzywana do analizy pojedycznej probki"
    echo "  --profile VALUE                 Nazwa profile zdefiniowanego w pliku konfiguaracyjnym nextflow z informacja o executor"
    echo "                                  Name of the profile specified in the nextflow configuration file."
    echo "                                  Available values: \"local\" and \"slurm\". Default value \"local\"."
    echo "  --all                           Display hidden parameters for advanced configuration"
    echo "                                  Wyswietl liste wszystkich parametrow modelu"
    echo "  -h, --help                      Show this help message"
}

# Usage function to display help
usage() {
    echo "Usage/Wywolanie: $0 --machine [Nanopore|Illumina] --reads PATH --primers_id VALUE --species [SARS-CoV-2|Influenza|RSV] [options]"
    show_common_parameters
}

# Full help
show_all_parameters() {
    echo "Usage/Wywolanie: $0 --machine [Nanopore|Illumina] --reads PATH --primers_id VALUE --species [SARS-CoV-2|Influenza|RSV] --projectDir PATH --external_databases_path PATH --main_image VALUE --manta_image VALUE --medaka_image VALUE --alphafold_image VALUE [options]"
    show_common_parameters
    echo ""
    echo "  --max_number_for_SV VALUE       Maximum number of reads used to predict SVs with Manta. Default value depends on species – 200_000 for SARS-CoV-2,"
    echo "                                  100_000 for RSV and 10_000 for Influenza"
    echo ""
    echo "  --min_median_for_SV VALUE       Minimum median coverage for a segment required for SVs calling with manta (default 50)"
    echo ""
    echo "  --variant VALUE                 Name of the expected influenza subtpye (e.g. H5N1). If unkown set up this value to UNK to auto-detect the subtype"
    echo ""
    echo "  --min_number_of_reads VALUE     Minimum number of reads present if fastq file for program to execute. Defualt: 1"
    echo ""
    echo "  --expected_genus_value VALUE    Percentage of reads associated with genus. Sample where percantage of reads fall below this threshold will not be analyzed. Defautlt 5%"
    echo ""
    echo "  --min_median_quality VALUE      Minimum median quality of bases for reads to be analyzed with fastqc. Default: 0"
    echo ""
    echo "  --quality_initial VALUE         Bases at 5' and 3' ends of reads are trimmed if their quality is below this value. Defaults: 5 for Illumina, 2 for Nanopore"
    echo ""
    echo "  --length VALUE                  Minimal length of the read after quality trimming to be used in the analysis. Default: 90 for Illumina, 49% of the initial read"
    echo "                                  length for Nanopore."
    echo ""
    echo "  --max_depth VALUE               Expected coverage after coverage equalization. Default: 600"
    echo ""
    echo "  --min_cov VALUE                 Minimum value for coverage at a position required to identify SNPs/INDELs. Defaults: 20 for Illumina, 50 for Nanopore"
    echo ""
    echo "  --mask VALUE                    Maximum value for coverage value to mask low coverage regions with 'N'. Defaults: 20 for Illumina, 50 for Nanopore"
    echo ""
    echo "  --quality_snp VALUE             Minimum value for nucleotide quality  (Phred + 33) used during SNPs/INDELs/SVs calling. Defaults: 15 for Illumina, 5 for Nanopore."
    echo ""
    echo "  --pval VALUE                    p-value threshold to determine if a variant is significant for Illumina data. Default: 0.05"
    echo ""
    echo "  --lower_ambig VALUE             Minimum ratio of reads carrying a minor allele at a given position above which ambiguous symbol can be introduced. Default: 0.45"
    echo ""
    echo "  --upper_ambig VALUE             Maximum ratio of reads carrying a minor allele at a given position below which ambiguous symbol can be introduced. Default: 0.55"
    echo ""
    echo "  --window_size VALUE             Window size used during coverage equalization. Default: 50"
    echo ""
    echo "  --min_mapq VALUE                Window size used during coverage equalization. Default: 30"
    echo ""
    echo "  --quality_for_coverage VALUE    Minimum value for nucleotide quality  (Phred + 33) considered during determination of low coverage regions. Defaults: 10 for"
    echo "                                  Illumina, 1 for Nanopore."
    echo ""
    echo "  --freyja_minq VALUE             Minimum mapping quality of a read to a reference genome for coinfection analysis with Freyja. Defaults: 20 for Illumina, 2 for Nanopore."
    echo ""
    echo "  --bed_offset VALUE              Number of bases by which a read can exceed the primer boundary. Nanopore-specific; default: 10."
    echo ""
    echo "  --extra_bed_offset VALUE        Number of bases by which a poor quality read can additionally exceed the primer boundary. Nanopore-specific; default: 10."
    echo ""
    echo "  --medaka_model VALUE            Medaka model. Nanopore-specific parameter. Default: 941_min_sup_variant_g507"
    echo ""
    echo "  --medaka_chunk_len VALUE        Medaka chunk length. Nanopore specific parameter. Default: 5_000 (for SARS-CoV_2 and RSV), 1_000 (for Influenza)"
    echo ""
    echo "  --medaka_chunk_overlap VALUE    Medaka chunk overlap. Nanopore specific parameter. Default: 4_000 (for SARS-CoV_2 and RSV), 500 (for Influenza)"
    echo ""
    echo "  --first_round_pval VALUE        p-value to determine if a variant is significant during initial genome prediction. Nanopore-specific parameter. Default: 0.05"
    echo ""
    echo "  --second_round_pval VALUE       p-value to determine if a variant is significant during genome fine-tuning. Nanopore-specific parameter. Default: 0.05"
    echo ""
    echo "  --results_dir PATH              Directory to store results. Default ./results"
    echo ""
    echo "  --no-alphafold                  Skip calculations of 3D model with alphafold"
    echo ""
}



# Parse arguments
OPTIONS=$(getopt -o h --long machine:,profile:,reads:,primers_id:,species:,adapters_id:,threads:,projectDir:,external_databases_path:,main_image:,manta_image:,medaka_image:,alphafold_image:,max_number_for_SV:,variant:,min_number_of_reads:,expected_genus_value:,min_median_quality:,quality_initial:,length:,max_depth:,min_cov:,mask:,quality_snp:,pval:,lower_ambig:,upper_ambig:,window_size:,min_mapq:,quality_for_coverage:,freyja_minq:,bed_offset:,extra_bed_offset:,medaka_model:,medaka_chunk_len:,medaka_chunk_overlap:,first_round_pval:,second_round_pval:,results_dir:,min_median_for_SV:,no-alphafold,all,help -- "$@")

eval set -- "$OPTIONS"

if [[ $# -eq 1 ]]; then  # getopt adds `--` to indicate end of options
    echo "No parameters provided"
    usage
    exit 1
fi

# Process options
while true; do
    case "$1" in
        --machine)
            machine="$2"
            shift 2
            ;;
        --reads)
            reads="$2"
            shift 2
            ;;
        --primers_id)
            primers_id="$2"
            shift 2
            ;;
        --profile)
            profile="$2"
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
        --threads)
            threads="$2"
            shift 2
            ;;
        --projectDir)
            projectDir="$2"
            shift 2
            ;;
        --external_databases_path)
            external_databases_path="$2"
            shift 2
            ;;
        --main_image)
            main_image="$2"
            shift 2
            ;;
        --manta_image)
            manta_image="$2"
            shift 2
            ;;
        --medaka_image)
            medaka_image="$2"
            shift 2
            ;;
        --alphafold_image)
            alphafold_image="$2"
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
        --min_number_of_reads)
            min_number_of_reads="$2"
            shift 2
            ;;
        --expected_genus_value)
            expected_genus_value="$2"
            shift 2
            ;;
        --min_median_quality)
            min_median_quality="$2"
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
	      --results_dir)
            results_dir="$2"
            shift 2
            ;;
        --no-alphafold)
            run_alphafold="false"
            shift 1
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


# Validate if all required parameters are set by a user
if [[ -z "$machine" || -z "$reads" || -z "$primers_id" || -z "$species" ]]; then
    echo "Error: Missing required parameters."
    usage
    exit 1
fi

# Check if users provided one of the profiles understood by the pipeline
if [[ "$profile" != "slurm" && "$profile" != "local" ]]; then
    echo "User must provide one the available profiles: slurm or local"
    usage
    exit 1
fi

# Check if user provided correct species and if so set defaults
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
    echo "Error: Unsupported species $species. Supported values are: SARS-CoV-2, Influenza, RSV."
    exit 1
fi

# Check if user provided correct sequencing platform and if so set default values for the main program
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
	[[ -z "${length}" ]] && length=0.49 # for nanopore nanopore min length is relative to the expected segment/amplikon length
	[[ -z "${medaka_model}" ]] && medaka_model="r941_min_sup_variant_g507" # Flow cell v9.4.1, for first round of medaka for the second round we use r941_min_sup_g507
	if [ ${species}  == 'SARS-CoV-2' ]; then
		[[ -z "${medaka_chunk_len}" ]] && medaka_chunk_len=5000  
		[[ -z "${medaka_chunk_overlap}" ]] && medaka_chunk_overlap=4000
	elif [ ${species}  == 'Influenza' ]; then
		[[ -z "${medaka_chunk_len}" ]] && medaka_chunk_len=1000  
                [[ -z "${medaka_chunk_overlap}" ]] && medaka_chunk_overlap=500
	elif [ ${species}  == 'RSV' ]; then
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
    echo "Error: Unsupported sequencing platform: $machine. Supported values are: Nanopore, Illumina."
    exit 1
fi


# In a directory there must be at least one file that meet provided pattern
expanded_reads=$(eval ls ${reads} 2> /dev/null)

if [ $(echo "${expanded_reads}" | wc -w) -lt 1 ]; then
	echo "Error: No reads found in: ${reads}"
	exit 1
fi


# Check primers -- validate that the provided primer scheme is correct for the selected species
if [[ "$species" == "SARS-CoV-2" ]]; then
    VALID_PRIMERS=("${SARSCOV2_PRIMERS[@]}")
elif [[ "$species" == "RSV" ]]; then
    VALID_PRIMERS=("${RSV_PRIMERS[@]}")
elif [[ "$species" == "Influenza" ]]; then
    VALID_PRIMERS=("${INFLUENZA_PRIMERS[@]}")
fi

CORRECT_ID=0
for var in "${VALID_PRIMERS[@]}"; do
    if [[ "${primers_id}" == "${var}" ]]; then
        CORRECT_ID=1
        break
    fi
done

if [[ ${CORRECT_ID} -eq 0 ]]; then
    echo -e "Error: Invalid primer scheme '${primers_id}' for species '${species}'."
    echo -e "Available options for ${species}: ${VALID_PRIMERS[*]}\n"
    exit 1
fi


echo "Running pipeline..."
nextflow run ${projectDir}/nf_pipeline_viral.nf \
    --projectDir ${projectDir} \
    --external_databases_path ${external_databases_path} \
    --reads "${reads}" \
    --primers_id ${primers_id} \
    --adapters_id ${adapters_id} \
    --machine ${machine} \
    --species ${species} \
    --main_image ${main_image} \
    --manta_image ${manta_image} \
    --medaka_image ${medaka_image} \
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

