#!/usr/bin/env bash

# This script should be run periodically to update the external databases used by the pipeline.
#
# Example crontab entries:

# Update nextclade every Saturday at 3:00 AM
# Update pangolin every Saturday at 3:05 AM
# Update kraken2 every 3 months on the 1st day of the month at 3:10 AM

# 0 3 * * 6 cd /path/to/sars-illumina && bin/update_external_databases.sh nextclade
# 5 3 * * 6 cd /path/to/sars-illumina && bin/update_external_databases.sh pangolin
# 10 3 * * 6 cd /path/to/sars-illumina && bin/update_external_databases.sh freyja
# 15 3 1 */3 * cd /path/to/sars-illumina && bin/update_external_databases.sh kraken

database="all" #  name of the database to update, alternatively "all" can can be used 
kraken_type="standard" #  name of the kraken2 subdatabase, only valid if performning "kraken2" update
genus="all" #  name of the genus for which database is updated, only valid if mlsr or cgmlst databases are updated
output=""  #  top-level directory with databases, each database will be a subdirectory of it, hierarchy of databases in that directory is PREDEFINED
image_name="plepiseq-wgs-pipeline-updater:latest" #  name of the image within which all updates are performed
cpus=1
limit_first_n="" # optional: limit number of records downloaded (for enterobase and pubmlst testing)
pubmlst_oauth_file="" # optional path to PubMLST OAuth credentials file on host

# Function to display help message
function show_help() {
    echo "Usage: $0 --database <string> --output <path> --image_name <string> [--cpus <int> --kraken-type <type> --genus <Salmonella|Escherichia|Campylobacter|all> --limit_first_n <int> --pubmlst_oauth_file <path>]"
    echo
    echo "Options:"
    echo "  --database      Name of the database to download or update"
    echo "                  Nazwa bazy do pobrania/aktualizacji. Mozliwe opcje to:"
    echo "                  amrfinder_plus mlst cgmlst disinfinder resfinder vfdb enterobase"
    echo "                  kmerfinder metaphlan phiercc pubmlst pointfinder plasmidfinder virulencefinder"
    echo "                  spifinder speciesfinder mlstfinder pangolin nextclade kraken2 freyja alphafold all"
    echo "  --output        Path to save the downloaded database. Each database is placed in a separete subdirectory"
    echo "                  Sciezka do katalogu z bazami, dla kazdej bazy zostanie stworzony wlasny podkatalog "
    echo "  --image_name    Full name of the docker image (with tag) used for updates"
    echo "                  Nazwa obrazu docker uzywanego przez program do pobierania/aktualizacji baz"
    echo "Optional arguments:"
    echo "  --cpus          Number of threads"
    echo "                  Liczba procesorow (domyslna wartosc: 1)"
    echo "  --kraken_type   Type of Kraken database (valid if database is set to kraken2). See pipelines documentations section 5.4.3) If not provided 'standard' database is downloaded"
    echo "                  Nazwa predefiniowanej bazy wykorzystywanej przez program kraken. W przypadku gdy nie podano tego argumentu pobierana jest baza 'standard' "
    echo "  --genus         Name of a genus (valid only for mlst, cgmlst, and enterobase databases). If not provided database is downloaded for all three genuses"
    echo "                  Nazwa rodzaju bakterii dla ktorego pobierana jest baza (tylko w przypadku baz mlst, cgmls oraz enterobase). Mozliwe wartosci to:"
    echo "                  Salmonella Escherichia Campylobacter all"
    echo "  --limit_first_n  Optional: limit number of records downloaded per data source (valid for enterobase and pubmlst)."
    echo "                   Useful for testing without downloading the full dataset."
    echo "                   For pubmlst: timestamp is NOT advanced when limit truncates records, so remaining"
    echo "                   records will still be available on the next run. Remove the limit for a full sync."
    echo "                   For enterobase: only first N strains from remote list are considered; re-run without"
    echo "                   the limit to pick up the rest."
    echo "  --pubmlst_oauth_file  Optional path to PubMLST OAuth credentials file on host; will be mounted read-only into container"
}

OPTIONS=$(getopt -o h --long database:,output:,cpus:,image_name:,kraken_type:,genus:,limit_first_n:,pubmlst_oauth_file:,help -- "$@")

eval set -- "$OPTIONS"

if [[ $# -eq 1 ]]; then 
    echo "No parameters provided"
    show_help
    exit 1
fi


while true; do
    case "$1" in
        --database)
            database="$2"
            shift 2
            ;;
        --output)
            output="$2"
            shift 2
            ;;
	--image_name)
	    image_name="$2"
            shift 2
            ;;
	--kraken_type)
            kraken_type="$2"
            shift 2
            ;;
        --cpus)
            cpus="$2"
            shift 2
            ;;        
        --genus)
            genus="$2"
            shift 2
            ;;
        --limit_first_n)
            limit_first_n="$2"
            shift 2
            ;;
        --pubmlst_oauth_file)
            pubmlst_oauth_file="$2"
            shift 2
            ;;
        -h|--help)
            show_help
            exit 0
            ;;
        --)
            shift
            break
            ;;
        *)
            echo "Unknown option: $1"
            show_help
            exit 1
            ;;
    esac
done


# Check if a user provided a correct database to download/update
CORRECT_DB=0
ALL_DB=(amrfinder_plus mlst cgmlst cgmlstfinder, disinfinder resfinder vfdb enterobase kmerfinder metaphlan phiercc pubmlst pointfinder plasmidfinder virulencefinder spifinder speciesfinder mlstfinder pangolin nextclade kraken2 freyja alphafold all)
for var in ${ALL_DB[@]}; do
    if [ ${database} == ${var} ];then
           CORRECT_DB=1
           break
   fi
done

# Validate required arguments
## Databases is a valid
if [ ${CORRECT_DB} -eq 0 ]; then
    echo "Error: Provide incorrect argument to --database"
    show_help
    exit 1
fi

## output path is provided
if [[ -z "$output" ]]; then
    echo "Error: --output is required."
    show_help
    exit 1
fi

### Create output directory if does not exists, this directory will be mounted by docker
output=$(realpath ${output})
if [ ! -d "${output}" ]; then
    echo "Directory ${output} does not exist. Creating it..."
    mkdir -p "${output}"
fi

### Check if the user-provided path has write permissions 
if [ ! -w "$output" ]; then
    echo "Current user does not have write permissions to the directory $output"
    exit 1
fi

## image_name has a default, hence we only check if image name  is a valid docker image
tmp_name=`echo ${image_name} | cut -d ":" -f1`
tmp_tag=`echo ${image_name} | cut -d ":" -f2`

if [ $(docker images | grep "${tmp_name}" | grep "${tmp_tag}" | wc -l) -ne 1 ]; then
	echo "Provided docker image ${tmp_name}:${tmp_tag} does not exist. Provide valid image name"
	exit 1
fi

## Check optional parameters that are only valid for specific databases 

### Validate Kraken type if database is kraken2, 
### there is a default that we only check if user has not change it to something that is not supported
if [ "$database" == "kraken2" ]; then
	VALID_TYPES=("standard" "standard_08gb" "standard_16gb" "viral" "minusb" \
		"pluspf" "pluspf_08gb" "pluspf_16gb" "pluspfp" "pluspfp_08gb" "pluspfp_16gb" "nt" "eupathdb48")
        if [[ ! " ${VALID_TYPES[@]} " =~ " ${kraken_type} " ]]; then
             echo "Error: Invalid --kraken_type value."
             show_help
	     exit 1
       fi
fi

## Check genus only valid for 3 databases
if [[ "$database" == "mlst"  ||  "$database" == "cgmlst" || "$database" == "enterobase" ]]; then
        VALID_GENUS=(Salmonella Escherichia Campylobacter all)
	if [[ ! " ${VALID_GENUS[*]} " =~ " ${genus} " ]]; then
             echo "Error: Invalid --genus value."
             show_help
             exit 1
       fi
fi

## Check if user did provide betwee 0 to nproc of cpus
if [[ "$cpus" -le 0 || "$cpus" -gt "$(nproc)" ]]; then
       echo "Number of cpus cannot be lower than 1 and cannot exceed $(nproc)"
       exit 1
fi       

## Validate limit_first_n if provided (must be a positive integer, only valid for enterobase and pubmlst)
if [[ -n "$limit_first_n" ]]; then
    if ! [[ "$limit_first_n" =~ ^[0-9]+$ ]]; then
        echo "Error: --limit_first_n must be a positive integer."
        show_help
        exit 1
    fi
fi

# Output the parsed arguments
echo "Database: $database"
echo "Output Path: $output"
echo "Docker image: ${image_name}"
echo "Downloading all databases may take several hours"
[[ "$database" == "kraken2" ]] && echo "Kraken Type: ${kraken_type}"
[[ "$database" == "mlst"  ||  "$database" == "cgmlst" || "$database" == "enterobase" ]] && echo "Genus: ${genus}"
[[ -n "$limit_first_n" ]] && echo "Limit first N records: ${limit_first_n}"
[[ -n "$pubmlst_oauth_file" ]] && echo "PubMLST OAuth file: ${pubmlst_oauth_file}"

EXTRA_VOLUMES=""
if [[ -n "$pubmlst_oauth_file" ]]; then
    pubmlst_oauth_file=$(realpath "${pubmlst_oauth_file}")
    if [[ ! -f "$pubmlst_oauth_file" ]]; then
        echo "Error: --pubmlst_oauth_file does not exist: $pubmlst_oauth_file"
        exit 1
    fi
    if [[ ! -r "$pubmlst_oauth_file" ]]; then
        echo "Error: --pubmlst_oauth_file is not readable: $pubmlst_oauth_file"
        exit 1
    fi
    EXTRA_VOLUMES="--volume ${pubmlst_oauth_file}:/home/update/pubmlst_oauth.txt:ro"
fi


docker run --rm \
       --volume "${output}:/home/external_databases:rw" \
       ${EXTRA_VOLUMES} \
       --user $(id -u):$(id -g) \
       -e UPDATER_CONTAINER_IMAGE="${image_name}" \
       -e UPDATER_USER="$(id -un)" \
       -e UPDATER_HOST="$(hostname)" \
       -e UPDATER_WORKSPACE="${output}" \
       ${image_name} ${database} ${kraken_type} ${genus} ${cpus} ${limit_first_n}
