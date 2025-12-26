#!/bin/bash


# Simplified script for running the BACTERIAL pipeline
# It is intended to be EXECUTED ON A100 machine
# The only parameters that do not have default values are:
# (I) directory with reads
# (II) sequencing platform
# all OTHER parameters ( localization od all the databases, images and modules) have PREDEFINED values that, if one chooses to, can be still modified (for testing purpose)

# localization of the main file with the pipeline and databases required to execute this pipeline
## Existing directories, for testing purpose can be change. but for production invariable
projectDir=""
external_databases_path="/mnt/raid/external_databases"
results_dir="./results" 

# docker images required to execute this pipeline
## Existing images, for testing purpose can be change, but for production invariable
main_image="plepiseq-wgs-pipeline-bacteria:$(cat ${projectDir}/VERSION)"
prokka_image="staphb/prokka:latest"
alphafold_image="alphafold2:latest"

## Nextflow executor
profile="local"

# Run alpfafold
run_alphafold="true"

# Parmaters related to resources available to the pipeline (max PER sample) if N samples are analyzed the pipeline will use at most N times more resuorces
# For testing purpose can be change, but for production invariable
threads=40

# Parameters without DEFAULTS that MUST be specified by a user
machine="" # Only Nanopore or Illumina
reads="" # Existing path


# Parameters with default values that depend on the sequencing platform
## For both platforms
genus="" # Salmonella Escherichia or Campylobacter can be empty
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
## Nanopore-specific
model_medaka=""


# Usage function to display help
usage() {
    echo "Usage/Wywolanie: $0 --machine [Nanopore|Illumina] --reads PATH --projectDir PATH --external_databases_path PATH --main_image VALUE --prokka_image --alphafold_image VALUE[options]"
    echo "Required parameters/Parametry wymagane:"
    echo "  --machine VALUE                 Sequencing platform: Nanopore or Illumina"
    echo "                                  Platforma sekwencjonujaca uzyta do analizy. Mozliwe wartosci to Nanopore albo Illumina"
    echo "  --reads PATH                    Path to sequencing data with naming pattern for sequencing files. Use single quotes for this argument"
    echo "                                  Scieżka do katalogu z wynikami sekwencjonowania wraz z wzorcem nazewnictwa plików"
    echo "                                  Format plikow: fastq.gz, Przyklad: '/some/directory/*_R{1,2}.fastq.gz'"
    echo "  --projectDir PATH               Sciezka do katalogu z pobranym repozytorium"
    echo "                                  Directory with projects repository"
    echo "  --external_databases_path PATH  Sciezka do katalogu z pobranymi zewnetrznymi bazami"
    echo "                                  Directory with all databases used by the program"
    echo "  --main_image VALUE              Nazwa obrazu w formacie \"name:tag\" z obrazem zawierajacym programy uzywane przez pipeline"
    echo "                                  Name of the docker image with main program"
    echo "  --prokka_image VALUE            Nazwa obrazu w formacie \"name:tag\" z obrazem zawierajacym program prokka."
    echo "                                  Name of the docker image with prokka program"
    echo "  --alphafold_image VALUE         Nazwa obrazu w formacie \"name:tag\" z obrazem zawierajacym program alphafold"
    echo "                                  Name of the docker image with alphafold program"
    echo "Optional parameters:"
    echo "  --genus VALUE                   Genus of the bacteria that underwent sequencing"
    echo "                                  Nazwa rodzajowa bakterii podelgajacej sekwencjonowaiu"
    echo "                                  Akceptowane wartosci to Salmonella Escherichia lub Campylobacter"
    echo "                                  parametru nie trzeba podawac, program sam wykrywa rodzaj na podstawie odczytow"
    echo "  --results_dir PATH              Path to directory with program's output (default ./results)"
    echo "                                  Sciezka do katalogu z wynikami programu"
    echo "  --threads VALUE                 Thread count (default: $threads)"
    echo "                                  Maksymalna ilosci CPU uzywana do analizy pojedycznej probki"
    echo "  --profile VALUE                 Nazwa profile zdefiniowanego w pliku konfiguaracyjnym nextflow z informacja o executor"
    echo "                                  Name of the profile specified in the nextflow configuration file."
    echo "                                  Available values: \"local\" and \"slurm\". Default value \"local\"."
    echo "  --all                           Display hidden parameters for advanced configuration"
    echo "                                  Wyswietl liste wszystkich parametrow modelu"
    echo "  -h, --help                      Show this help message"
}

# Full help
show_all_parameters() {
    echo "Usage/Wywolanie: $0 --machine [Nanopore|Illumina] --reads PATH --projectDir PATH --external_databases_path PATH --main_image VALUE --prokka_image --alphafold_image VALUE[options]"
    echo "Required parameters/Parametry wymagane:"
    echo "  --machine VALUE                 Sequencing platform: Nanopore or Illumina"
    echo "                                   Platforma sekwencjonujaca uzyta do analizy. Mozliwe wartosci to Nanopore albo Illumina"
    echo "  --reads PATH                    Path to sequencing data with naming pattern for sequencing files. Use single quotes for this argument"
    echo "                                  Scieżka do katalogu z wynikami sekwencjonowania wraz z wzorcem nazewnictwa plików"
    echo "                                  Format plikow: fastq.gz, Przyklad: '/some/directory/*_R{1,2}.fastq.gz'"
    echo "  --projectDir PATH               Sciezka do katalogu z pobranym repozytorium"
    echo "                                  Directory with projects repository"
    echo "  --external_databases_path PATH  Sciezka do katalogu z pobranymi zewnetrznymi bazami"
    echo "                                  Directory with all databases used by the program"
    echo "  --main_image VALUE              Nazwa obrazu w formacie \"name:tag\" z obrazem zawierajacym programy uzywane przez pipeline"
    echo "                                  Name of the docker image with main program"
    echo "  --prokka_image VALUE            Nazwa obrazu w formacie \"name:tag\" z obrazem zawierajacym program prokka."
    echo "                                  Name of the docker image with prokka program"
    echo "  --alphafold_image VALUE         Nazwa obrazu w formacie \"name:tag\" z obrazem zawierajacym program alphafold"
    echo "                                  Name of the docker image with alphafold program"
    echo "Optional parameters:"
    echo "  --genus VALUE                   Genus of the bacteria that underwent sequencing"
    echo "                                  Nazwa rodzajowa bakterii podelgajacej sekwencjonowaiu"
    echo "                                  Akceptowane wartosci to Salmonella Escherichia lub Campylobacter"
    echo "                                  parametru nie trzeba podawac, program sam wykrywa rodzaj na podstawie odczytow"
    echo "  --results_dir PATH              Path to directory with program's output (default ./results)"
    echo "                                  Sciezka do katalogu z wynikami programu"
    echo "  --threads VALUE                 Thread count (default: $threads)"
    echo "                                  Maksymalna ilosci CPU uzywana do analizy pojedycznej probki"
    echo "  --profile VALUE                 Nazwa profile zdefiniowanego w pliku konfiguaracyjnym nextflow z informacja o executor"
    echo "                                  Name of the profile specified in the nextflow configuration file."
    echo "                                  Available values: \"local\" and \"slurm\". Default value \"local\"."
    echo "  --quality VALUE                 Maximal quality of the base trimmed from 5' and 3' ends (default: 6 for illumina, 2 for nanopore)"
    echo "                                  Maksymalna jakosc nukleotydow w odczycie usuwanych z 3' i 5' koncow"
    echo "  --min_number_of_reads VALUE     Minimal number of paired-reads (fo illumina) and reads (for nanopore) required for sample"
    echo "                                  to be analyzed (default: 50000 for illumina, 10000 for nanopore)"
    echo "                                  Minimalna liczba odczytow w plikach fastq aby rozpoczac analize probki"
    echo "  --min_median_quality VALUE      Minimal median quality of bases required required to run the analysis (default: 10 for illumina, 5 for nanopore)"
    echo "                                  Minimalna mediana jakosci zasad wymagana dla probki."
    echo "  --main_genus_value VALUE        Minimal percantage of reads classified by kraken2 to main genus in the sample (default: 50)"
    echo "                                  Minimalny procent odczytow klasyfikowany do glownego rodzaju w probce"
    echo "  --kmerfinder_coverage VALUE     Minimal coverage claculated for the main species identified in a sample by kmerfinder (default: 20)"
    echo "                                  Minimalne teoretyczne pokrycie obliczone przez program kmerfinder"
    echo "  --main_species_coverage VALUE   Minimal theoretical coverage calculated during species identification (default 20)"
    echo "                                  Minimalne teoretyczne pokrycie liczone w trakcie identyfikacji gatunku"
    echo "  --min_genome_length VALUE       Minimal length of the final assembly required to process sample as a fraction of expected for identified species assmebly length"
    echo "                                  (default: 0.75)"
    echo "                                  Minimalna dlugosc genomu proponowanej probce w stosunku do oczekiwanej dlugosci genomu"
    echo "  --unique_loci VALUE             Minimal number of unique loci in initial MLST screen. Samples with lower number of unique loci are likely contaminated"
    echo "                                  and want be analyzed (default: 5 for illumina, 0 for nanopore)"
    echo "                                  Minimalna ilosc loci, w schemacie MLST, dla ktorych stwierdzono jedna, unikalna wersje allelu. Probki z mniejsza iloscia"
    echo "                                  unikalnych alleli uznawane sa za zanieczyszczone i nie sa analizowane"
    echo "  --contig_number VALUE           Maximal number of contigs (after covarage-based filtering) allowed for a sample."
    echo "                                  Samples with higher number of contigs want be analyzed (default: 1000 for Illumina and 100 for Nanopore data)"
    echo "                                  Maksymalna ilosc contigow proponowana dla probki."
    echo "  --N50 VALUE                     Sequence length of the shortest contig at 50% of the total assembly length (default 30000)"
    echo "                                  Minimalna dlugosc contigu ktory wraz z contig'ami dluzyszmi pozwala zaproponowac genom o dlugosci 50% ostatecznego gneomu"
    echo "  --min_coverage_ratio VALUE      Minimal coverage for a contig to pass filtering as a fraction of avarage coverage (default 0.1)"
    echo "                                  Minimalne pokrycie liczone dla contig'u wymagane do przejscia filtrow. Liczone"
    echo "                                  jako frakcja sredniego pokrycia w probce"
    echo "  --final_coverage VALUE          Minimal required coverage value calculated using length of the final assembly (default: 20)"
    echo "                                  Minimalne pokrycie dla probki liczone z wykorzystaniem ostatecznenie zapropnowanej sekwencji enomu"
    echo "  --min_coverage_value VALUE      Minimal absolute coverage for a contig to pass filtering (default: 20). NOT IMPLEMENTED."
    echo "                                  Minimalne bezwzgledne pokrycie wymagan dla contigu."
    echo "  --model_medaka VALUE            Model used by medaka to polish flye-proposed assebmly (default: r941_min_hac_g507, this model is still recommended even for R10 flow cell)"
    echo "                                  Model uzywany to identyfikacji SNP/SVs w genomie proponowanym przez program pilon"
    echo "  --no-alphafold                  Skip calculations of 3D model with alphafold"
    echo "                                  Omin krok generowania modelu 3D z uzyciem programu alphafold"
    echo "  --all                           Display this help meassage"
    echo "                                  Wyswietl liste wszystkich parametrow modelu"
    echo "  -h, --help                      Show this help message"
}


# Parse command-line options using GNU getopt
OPTS=$(getopt -o h --long projectDir:,profile:,external_databases_path:,results_dir:,main_image:,prokka_image:,alphafold_image:,threads:,machine:,reads:,genus:,quality:,min_number_of_reads:,min_median_quality:,main_genus_value:,kmerfinder_coverage:,main_species_coverage:,min_genome_length:,unique_loci:,contig_number:,N50:,final_coverage:,min_coverage_ratio:,min_coverage_value:,model_medaka:,no-alphafold,all,help -- "$@")

eval set -- "$OPTS"

if [[ $# -eq 1 ]]; then
    echo "No parameters provided"
    usage
    exit 1
fi

while true; do
  case "$1" in
    --projectDir )
      projectDir="$2";
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
    --main_image )
      main_image="$2"; 
      shift 2 
      ;;
    --prokka_image )
      prokka_image="$2"; 
      shift 2 
      ;;
    --alphafold_image )
      alphafold_image="$2"; 
      shift 2 
      ;;
    --threads )
      threads="$2"; 
      shift 2 
      ;;
    --machine )
      machine="$2"; 
      shift 2 
      ;;
    --reads )
      reads="$2"; 
      shift 2 
      ;;
    --genus )
      genus="$2"; 
      shift 2 
      ;;
    --quality )
      quality="$2"; 
      shift 2 
      ;;
    --min_number_of_reads )
      min_number_of_reads="$2"; 
      shift 2 
      ;;
    --min_median_quality )
      min_median_quality="$2"; 
      shift 2 
      ;;
    --main_genus_value )
      main_genus_value="$2"; 
      shift 2 
      ;;
    --kmerfinder_coverage )
      kmerfinder_coverage="$2"; 
      shift 2 
      ;;
    --main_species_coverage )
      main_species_coverage="$2"; 
      shift 2 
      ;;
    --min_genome_length )
      min_genome_length="$2"; 
      shift 2 
      ;;
    --unique_loci )
      unique_loci="$2"; 
      shift 2 
      ;;
    --contig_number )
      contig_number="$2"; 
      shift 2 
      ;;
    --N50 )
      N50="$2"; 
      shift 2 
      ;;
    --final_coverage )
      final_coverage="$2"; 
      shift 2 
      ;;
    --min_coverage_ratio )
      min_coverage_ratio="$2"; 
      shift 2 
      ;;
    --min_coverage_value )
      min_coverage_value="$2"; 
      shift 2 
      ;;
    --model_medaka )
      model_medaka="$2"; 
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
    -- )
      shift; 
      break 
      ;;
    * )
      echo "Unknown option: $1"
      usage
      exit 1
      ;;
  esac
done



# Validate if all required parameters are set by a user
if [[ -z "$machine" || -z "$reads" ]]; then
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

# Check if user provided correct sequencing platform and if so set default values for the main program
# Machine independent 
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
	[[ -z "${unique_loci}" ]] && unique_loci=5
elif [[ "$machine" == "Nanopore" ]]; then
        [[ -z "${quality}" ]] && quality=2
        [[ -z "${min_number_of_reads}" ]] && min_number_of_reads=10000
        [[ -z "${min_median_quality}" ]] && min_median_quality=5
        [[ -z "${contig_number}" ]] && contig_number=100
	[[ -z "${model_medaka}" ]] && model_medaka="r941_min_hac_g507"
	[[ -z "${unique_loci}" ]] && unique_loci=0
else
    echo "Error: Unsupported sequencinf platform: $machine. Supported values are: Nanopore, Illumina."
    exit 1
fi


# In a directory there must be at least one file that meet provided pattern
expanded_reads=$(eval ls ${reads} 2> /dev/null)

if [ $(echo "${expanded_reads}" | wc -w) -lt 1 ]; then
	echo "Error: No reads found in: ${reads}"
	exit 1
fi


# Tests for USER provided parameters 
# TO DO

echo "Running the bacterial pipeline..."
nextflow run ${projectDir}/nf_pipeline_bacterial.nf \
	     --results_dir ${results_dir} \
	     --genus ${genus} \
	     --reads "${reads}" \
	     --machine ${machine} \
	     --main_image ${main_image} \
	     --prokka_image ${prokka_image} \
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
