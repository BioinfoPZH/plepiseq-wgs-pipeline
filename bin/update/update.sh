#!/usr/bin/env bash

# Script is intended to run within a docker container, hence all paths are HARDCODED
# all subsripts intended to download/update a specific database are located in /home/update
# the "top-level" directory where is /home/external_databases
# data from each database will be put is a specific subdirectory e.g. //home/external_databases/cgmlst

# If updating/downloading a database requires few lines of code the function is inside this file
# more complex downloads/updates are kept in separate files in /home/updates

# We TRY to distinguish updating vs. downloading functionalities
# This is usually based on the presence or absence of specific version file in a given database subdirectory
# If updataing is not feasable the defult behaviour is to rmemove content of a specific subdirecroty and download
#  all the files from scratch

# This script is intended as an updater for both "viral" and "bacterial" pipelines

# This script understands 4 positional arguments (database name, type of kraken database, bacterial genus, number of cpus)
# This script should never be run directly but using a wrapper script update_external_databases.sh
# All values passed to this script are evaluated by update_external_databases.sh

# Updater execution context (provided by update_external_databases.sh, but keep safe defaults)
UPDATER_WORKSPACE="${UPDATER_WORKSPACE:-/home/external_databases}"
UPDATER_CONTAINER_IMAGE="${UPDATER_CONTAINER_IMAGE:-unknown}"
UPDATER_USER="${UPDATER_USER:-$(id -un)}"
UPDATER_HOST="${UPDATER_HOST:-$(hostname)}"


#############################################
# Function to update the nextclade database #
#############################################


## Nextclade
### No differenc in Updating/Downloading  if /home/external_databases/nextclade directory exist, everything inside it will be removed

update_nextclade() {
    python3 -u /home/update/download_nextclade.py --workspace "${UPDATER_WORKSPACE}" \
                                                  --container_image "${UPDATER_CONTAINER_IMAGE}" \
                                                  --user "${UPDATER_USER}" \
                                                  --host "${UPDATER_HOST}" \
                                                  --output_dir /home/external_databases/nextclade
    return $?
}

## Pangolin
## No differenc in Updating/Downloading  if /home/external_databases/pangolin directory exist, everything inside it will be removed
## --upgrade option in pip had no effect
update_pangolin() {
    python3 -u /home/update/download_pangolin_data.py --workspace "${UPDATER_WORKSPACE}" \
                                                      --container_image "${UPDATER_CONTAINER_IMAGE}" \
                                                      --user "${UPDATER_USER}" \
                                                      --host "${UPDATER_HOST}" \
                                                      --output_dir /home/external_databases/pangolin
    return $?
}

# Kraken2
update_kraken2() {
    local kraken2_type=$1
    if [ ! -d "/home/external_databases/kraken2" ]; then
	    mkdir /home/external_databases/kraken2
    fi

    python3 -u /home/update/download_kraken.py --local_path /home/external_databases/kraken2 \
                                               --db_name "${kraken2_type}" \
                                               --workspace "${UPDATER_WORKSPACE}" \
                                               --container_image "${UPDATER_CONTAINER_IMAGE}" \
                                               --user "${UPDATER_USER}" \
                                               --host "${UPDATER_HOST}"

    return $?
}

# Freyja
update_freyja() {
    python3 -u /home/update/download_freyja.py --workspace "${UPDATER_WORKSPACE}" \
                                               --container_image "${UPDATER_CONTAINER_IMAGE}" \
                                               --user "${UPDATER_USER}" \
                                               --host "${UPDATER_HOST}" \
                                               --output_dir /home/external_databases/freyja

    return $?
}


# AMRfinder_plus
## Database has an update mechanism based on the version.txt on ftp server
## if local and remote versions of the database are different the script will remove content of output_dir and download 
## all the files 
update_amrfinder() {
	if [ ! -d "/home/external_databases/amrfinder_plus" ]; then
		 mkdir /home/external_databases/amrfinder_plus
	fi
        python3 -u /home/update/download_amrfinder.py --workspace "${UPDATER_WORKSPACE}" \
                                                      --container_image "${UPDATER_CONTAINER_IMAGE}" \
                                                      --user "${UPDATER_USER}" \
                                                      --host "${UPDATER_HOST}" \
                                                      --output_dir /home/external_databases/amrfinder_plus

}

# Metaphlan
## Database has an update mechanism and dedicated script
update_metaphlan() {

        python3 -u /home/update/download_metaphlan.py --workspace "${UPDATER_WORKSPACE}" \
                                                      --container_image "${UPDATER_CONTAINER_IMAGE}" \
                                                      --user "${UPDATER_USER}" \
                                                      --host "${UPDATER_HOST}" \
                                                      --output_dir /home/external_databases/metaphlan

}

# Kmerfinder
## Update mechanism - timestamp file in /home/external_databases/kmerfinder/
## if dates in a timestamp file and a database version from https://cge.food.dtu.dk/services/KmerFinder/
## are identical no update is carried out
update_kmerfinder() {
    python3  /home/update/download_kmerfinder_db.py -o /home/external_databases/kmerfinder/
}

# SpeciesFinder DB (tarball download + extract; always rebuild)
update_speciesfinder() {
    python3 -u /home/update/download_speciesfinder_db.py --workspace "${UPDATER_WORKSPACE}" \
                                                         --container_image "${UPDATER_CONTAINER_IMAGE}" \
                                                         --user "${UPDATER_USER}" \
                                                         --host "${UPDATER_HOST}" \
                                                         --output_dir /home/external_databases/speciesfinder
    return $?
}

# CGE databases (genomicepidemiology) are updated via a milestone-based python client
# that uses remote HEAD commit id for update decisions.
update_cge_db() {
        local db=$1
        local kma_bin=$2

        if [ -n "${kma_bin}" ]; then
                python3 -u /home/update/download_cge_db.py --workspace "${UPDATER_WORKSPACE}" \
                                                           --container_image "${UPDATER_CONTAINER_IMAGE}" \
                                                           --user "${UPDATER_USER}" \
                                                           --host "${UPDATER_HOST}" \
                                                           --db "${db}" \
                                                           --output_dir "/home/external_databases/${db}" \
                                                           --kma_binary "${kma_bin}"
        else
                python3 -u /home/update/download_cge_db.py --workspace "${UPDATER_WORKSPACE}" \
                                                           --container_image "${UPDATER_CONTAINER_IMAGE}" \
                                                           --user "${UPDATER_USER}" \
                                                           --host "${UPDATER_HOST}" \
                                                           --db "${db}" \
                                                           --output_dir "/home/external_databases/${db}"
        fi
}
#VFDB
## No update
update_vfdb() {
	local cpus=$1
  python3 -u /home/update/download_vfdb.py --workspace "${UPDATER_WORKSPACE}" \
                                           --container_image "${UPDATER_CONTAINER_IMAGE}" \
                                           --user "${UPDATER_USER}" \
                                           --host "${UPDATER_HOST}" \
                                           --output_dir /home/external_databases/vfdb \
                                           --cpus ${cpus}

}

# MLST data
## No update mechanism
## Data for different genuses originate from eirther pubmlst or enterobase
## For Campylobacter for each species we need to call the script separetly
update_mlst_campylo() {
# subfunction for update_mlst
	local directory=${1}
	local spec=${2}
	local db=${3}
	python3 -u /home/update/download_cgmlst_pubmlst.py --database "${spec}" \
	                                                  --scheme_name "${db}" \
	                                                  --cpus ${cpus} \
	                                                  --output_dir "${directory}" \
	                                                  --workspace "${UPDATER_WORKSPACE}" \
	                                                  --container_image "${UPDATER_CONTAINER_IMAGE}" \
	                                                  --user "${UPDATER_USER}" \
	                                                  --host "${UPDATER_HOST}" \
	                                                  --oauth_credentials_file /home/update/pubmlst_oauth.txt \
	                                                  --download_workers 4
}

update_mlst() {
	local genus=${1}
	# Salmonella Escherichia Campylobacter
	if [ ${genus} == "Campylobacter" ]; then
		if [ ! -d "/home/external_databases/mlst/Campylobacter" ]; then
			mkdir -p /home/external_databases/mlst/Campylobacter
		fi
		#each species has its own seprate MLST
		SPEC="pubmlst_campylobacter_nonjejuni_seqdef"
		# fetus
		update_mlst_campylo "/home/external_databases/mlst/Campylobacter/fetus" "${SPEC}" "C. fetus MLST"
		# helveticus
		update_mlst_campylo "/home/external_databases/mlst/Campylobacter/helveticus" "${SPEC}" "C. helveticus MLST"
		# concisus
		update_mlst_campylo "/home/external_databases/mlst/Campylobacter/concisus" "${SPEC}" "C. concisus/curvus MLST"
		# hyointestinalis
		update_mlst_campylo "/home/external_databases/mlst/Campylobacter/hyointestinalis" "${SPEC}" "C. hyointestinalis MLST"
		# upsaliensis
		update_mlst_campylo "/home/external_databases/mlst/Campylobacter/upsaliensis" "${SPEC}" "C. upsaliensis MLST"
		# lari
		update_mlst_campylo "/home/external_databases/mlst/Campylobacter/lari" "${SPEC}" "C. lari MLST"
		# insulaenigrae
		update_mlst_campylo "/home/external_databases/mlst/Campylobacter/insulaenigrae" "${SPEC}" "C. insulaenigrae MLST"
		# lanienae
		update_mlst_campylo "/home/external_databases/mlst/Campylobacter/lanienae" "${SPEC}" "C. lanienae MLST"
		# sputorum
		update_mlst_campylo "/home/external_databases/mlst/Campylobacter/sputorum" "${SPEC}" "C. sputorum MLST"
		SPEC="pubmlst_campylobacter_seqdef"
		# jejuni
		update_mlst_campylo "/home/external_databases/mlst/Campylobacter/jejuni" "${SPEC}" "MLST"

	elif [ ${genus} == "Salmonella" ]; then
		# NOTE: do not wipe this directory – EnteroBase schema downloader uses a local checksum manifest
		# (enterobase_md5.json) to decide whether an update is required.

		DATABASE="senterica"
                scheme_name="MLST_Achtman"
                scheme_dir="Salmonella.Achtman7GeneMLST"
                python3 -u /home/update/download_schema_data_enterobase.py --database ${DATABASE} \
                                                                           --scheme_name "${scheme_name}" \
                                                                           --scheme_dir "${scheme_dir}" \
                                                                           --cpus ${cpus} \
                                                                           --output_dir /home/external_databases/mlst/Salmonella \
                                                                           --workspace "${UPDATER_WORKSPACE}" \
                                                                           --container_image "${UPDATER_CONTAINER_IMAGE}" \
                                                                           --user "${UPDATER_USER}" \
                                                                           --host "${UPDATER_HOST}"


	elif [ ${genus} == "Escherichia" ]; then
		# NOTE: do not wipe this directory – EnteroBase schema downloader uses a local checksum manifest
		# (enterobase_md5.json) to decide whether an update is required.
		DATABASE="ecoli"
                scheme_name="MLST_Achtman"
                scheme_dir="Escherichia.Achtman7GeneMLST"
		python3 -u /home/update/download_schema_data_enterobase.py --database ${DATABASE} \
                                                                           --scheme_name "${scheme_name}" \
                                                                           --scheme_dir "${scheme_dir}" \
                                                                           --cpus ${cpus} \
                                                                           --output_dir /home/external_databases/mlst/Escherichia \
                                                                           --workspace "${UPDATER_WORKSPACE}" \
                                                                           --container_image "${UPDATER_CONTAINER_IMAGE}" \
                                                                           --user "${UPDATER_USER}" \
                                                                           --host "${UPDATER_HOST}"

	elif [ ${genus} == "all" ]; then
                # NOTE: do not wipe – keep checksum manifest for incremental updates.

		DATABASE="senterica"
		scheme_name="MLST_Achtman"
		scheme_dir="Salmonella.Achtman7GeneMLST"
		python3 -u /home/update/download_schema_data_enterobase.py --database ${DATABASE} \
                                                                           --scheme_name "${scheme_name}" \
                                                                           --scheme_dir "${scheme_dir}" \
                                                                           --cpus ${cpus} \
                                                                           --output_dir /home/external_databases/mlst/Salmonella \
                                                                           --workspace "${UPDATER_WORKSPACE}" \
                                                                           --container_image "${UPDATER_CONTAINER_IMAGE}" \
                                                                           --user "${UPDATER_USER}" \
                                                                           --host "${UPDATER_HOST}"

		DATABASE="ecoli"
		scheme_name="MLST_Achtman" 
		scheme_dir="Escherichia.Achtman7GeneMLST"

                python3 -u /home/update/download_schema_data_enterobase.py --database ${DATABASE} \
                                                                           --scheme_name "${scheme_name}" \
                                                                           --scheme_dir "${scheme_dir}" \
                                                                           --cpus ${cpus} \
                                                                           --output_dir /home/external_databases/mlst/Escherichia \
                                                                           --workspace "${UPDATER_WORKSPACE}" \
                                                                           --container_image "${UPDATER_CONTAINER_IMAGE}" \
                                                                           --user "${UPDATER_USER}" \
                                                                           --host "${UPDATER_HOST}"

		if [ ! -d "/home/external_databases/mlst/Campylobacter" ]; then
                        mkdir -p /home/external_databases/mlst/Campylobacter
                fi
                #each species has its own seprate MLST
                SPEC="pubmlst_campylobacter_nonjejuni_seqdef"
                # fetus
                update_mlst_campylo "/home/external_databases/mlst/Campylobacter/fetus" "${SPEC}" "C. fetus MLST"
                # helveticus
                update_mlst_campylo "/home/external_databases/mlst/Campylobacter/helveticus" "${SPEC}" "C. helveticus MLST"
                # concisus
                update_mlst_campylo "/home/external_databases/mlst/Campylobacter/concisus" "${SPEC}" "C. concisus/curvus MLST"
                # hyointestinalis
                update_mlst_campylo "/home/external_databases/mlst/Campylobacter/hyointestinalis" "${SPEC}" "C. hyointestinalis MLST"
                # upsaliensis
                update_mlst_campylo "/home/external_databases/mlst/Campylobacter/upsaliensis" "${SPEC}" "C. upsaliensis MLST"
                # lari
                update_mlst_campylo "/home/external_databases/mlst/Campylobacter/lari" "${SPEC}" "C. lari MLST"
                # insulaenigrae
                update_mlst_campylo "/home/external_databases/mlst/Campylobacter/insulaenigrae" "${SPEC}" "C. insulaenigrae MLST"
                # lanienae
                update_mlst_campylo "/home/external_databases/mlst/Campylobacter/lanienae" "${SPEC}" "C. lanienae MLST"
                # sputorum
                update_mlst_campylo "/home/external_databases/mlst/Campylobacter/sputorum" "${SPEC}" "C. sputorum MLST"
                SPEC="pubmlst_campylobacter_seqdef"
                # jejuni
                update_mlst_campylo "/home/external_databases/mlst/Campylobacter/jejuni" "${SPEC}" "MLST"
	fi

}

# cgMLST related data
## like MLST data come from different sources
## for campylobacter only jejuni has cgMLST and all vrequired ariables are hardcoded within a scipt

update_cgmlst() {
	local genus=$1
	local cpus=$2 
	if [ ${genus} == "Campylobacter" ]; then
		DATABASE="pubmlst_campylobacter_seqdef"
		schema_name="C. jejuni / C. coli cgMLST v2"
		python3 -u /home/update/download_cgmlst_pubmlst.py --database "${DATABASE}" \
			                                           --scheme_name "${schema_name}" \
						                   --cpus ${cpus} \
						                   --output_dir /home/external_databases/cgmlst/Campylobacter/jejuni/ \
						                   --workspace "${UPDATER_WORKSPACE}" \
						                   --container_image "${UPDATER_CONTAINER_IMAGE}" \
						                   --user "${UPDATER_USER}" \
						                   --host "${UPDATER_HOST}" \
						                   --oauth_credentials_file /home/update/pubmlst_oauth.txt \
						                   --download_workers 4

	elif [ ${genus} == "Salmonella" ]; then
                # NOTE: do not wipe this directory – EnteroBase schema downloader uses a local checksum manifest
                # (enterobase_md5.json) to decide whether an update is required.
		DATABASE="senterica"
		scheme_name="cgMLST_v2"
		scheme_dir="Salmonella.cgMLSTv2"
		python3 -u /home/update/download_schema_data_enterobase.py --database ${DATABASE} \
			                                                   --scheme_name "${scheme_name}" \
								           --scheme_dir "${scheme_dir}" \
								           --cpus ${cpus} \
								           --output_dir /home/external_databases/cgmlst/Salmonella \
                                                                           --workspace "${UPDATER_WORKSPACE}" \
                                                                           --container_image "${UPDATER_CONTAINER_IMAGE}" \
                                                                           --user "${UPDATER_USER}" \
                                                                           --host "${UPDATER_HOST}"
        elif [ ${genus} == "Escherichia" ]; then
                # NOTE: do not wipe this directory – EnteroBase schema downloader uses a local checksum manifest
                # (enterobase_md5.json) to decide whether an update is required.
		DATABASE="ecoli"
		scheme_name="cgMLST" 
		scheme_dir="Escherichia.cgMLSTv1"
		python3 -u /home/update/download_schema_data_enterobase.py --database ${DATABASE} \
                                                                           --scheme_name "${scheme_name}" \
                                                                           --scheme_dir "${scheme_dir}" \
                                                                           --cpus ${cpus} \
								           --output_dir /home/external_databases/cgmlst/Escherichia \
                                                                           --workspace "${UPDATER_WORKSPACE}" \
                                                                           --container_image "${UPDATER_CONTAINER_IMAGE}" \
                                                                           --user "${UPDATER_USER}" \
                                                                           --host "${UPDATER_HOST}"
        elif [ ${genus} == "all" ]; then
		echo "Downloading data for Escherichia at: $(date +"%H:%M %d-%m-%Y")" >> log
		# NOTE: do not wipe – keep checksum manifest for incremental updates.
                DATABASE="ecoli"
                scheme_name="cgMLST"
                scheme_dir="Escherichia.cgMLSTv1"
	        python3 -u /home/update/download_schema_data_enterobase.py --database ${DATABASE} \
                                                                           --scheme_name "${scheme_name}" \
                                                                           --scheme_dir "${scheme_dir}" \
                                                                           --cpus ${cpus} \
                                                                           --output_dir /home/external_databases/cgmlst/Escherichia \
                                                                           --workspace "${UPDATER_WORKSPACE}" \
                                                                           --container_image "${UPDATER_CONTAINER_IMAGE}" \
                                                                           --user "${UPDATER_USER}" \
                                                                           --host "${UPDATER_HOST}"	
		echo "Downloading data for Salmonella at: $(date +"%H:%M %d-%m-%Y")" >> log
		# NOTE: do not wipe – keep checksum manifest for incremental updates.
                DATABASE="senterica"
                scheme_name="cgMLST_v2"
                scheme_dir="Salmonella.cgMLSTv2"
		python3 -u /home/update/download_schema_data_enterobase.py --database ${DATABASE} \
			                                                   --scheme_name "${scheme_name}" \
								           --scheme_dir "${scheme_dir}" \
								           --cpus ${cpus} \
								           --output_dir /home/external_databases/cgmlst/Salmonella \
                                                                           --workspace "${UPDATER_WORKSPACE}" \
                                                                           --container_image "${UPDATER_CONTAINER_IMAGE}" \
                                                                           --user "${UPDATER_USER}" \
                                                                           --host "${UPDATER_HOST}"

		echo "Downloading data for Campylobacter at: $(date +"%H:%M %d-%m-%Y")" >> log
		DATABASE="pubmlst_campylobacter_seqdef"
                schema_name="C. jejuni / C. coli cgMLST v2"
                python3 -u /home/update/download_cgmlst_pubmlst.py --database "${DATABASE}" \
                                                                   --scheme_name "${schema_name}" \
                                                                   --cpus ${cpus} \
                                                                   --output_dir /home/external_databases/cgmlst/Campylobacter/jejuni/ \
                                                                   --workspace "${UPDATER_WORKSPACE}" \
                                                                   --container_image "${UPDATER_CONTAINER_IMAGE}" \
                                                                   --user "${UPDATER_USER}" \
                                                                   --host "${UPDATER_HOST}" \
                                                                   --oauth_credentials_file /home/update/pubmlst_oauth.txt \
                                                                   --download_workers 4
	fi
}

# Downloading data regarding known strains from enterobase
## There is an update mechanism so we do not remove files
update_enterobase() {
	local genus=${1}

	if [ ${genus} == "Escherichia" ]; then
                if [ ! -d "/home/external_databases/enterobase/Escherichia" ]; then
			mkdir -p /home/external_databases/enterobase/Escherichia
                fi
		cd /home/external_databases/enterobase/Escherichia
		DATABASE="ecoli" 
		CGNAME="cgMLST" 
		python3 -u /home/update/download_enterobase_data.py "${DATABASE}" "${CGNAME}" >> log 2>&1
        elif [ ${genus} == "Salmonella" ]; then
		if [ ! -d "/home/external_databases/enterobase/Salmonella" ]; then
                        mkdir -p /home/external_databases/enterobase/Salmonella
                fi
                cd /home/external_databases/enterobase/Salmonella
		DATABASE="senterica" 
		CGNAME="cgMLST_v2" 
		python3 -u /home/update/download_enterobase_data.py "${DATABASE}" "${CGNAME}" >> log 2>&1
	elif [[ ${genus} == "all" || ${genus} == "Campylobacter" ]]; then
		if [ ! -d "/home/external_databases/enterobase/Escherichia" ]; then
                        mkdir -p /home/external_databases/enterobase/Escherichia
                fi
                cd /home/external_databases/enterobase/Escherichia
                DATABASE="ecoli"
                CGNAME="cgMLST"
                python3 -u /home/update/download_enterobase_data.py "${DATABASE}" "${CGNAME}" >> log 2>&1

		if [ ! -d "/home/external_databases/enterobase/Salmonella" ]; then
                        mkdir -p /home/external_databases/enterobase/Salmonella
                fi
                cd /home/external_databases/enterobase/Salmonella
                DATABASE="senterica"
                CGNAME="cgMLST_v2"
                python3 -u /home/update/download_enterobase_data.py "${DATABASE}" "${CGNAME}" >> log 2>&1
	fi

}

# Downloading data to get analogue of pHierCC for C.jejuni and for historical analysis
## There is an update mechanism

update_pubmlst() {
	local cpus=${1}
	if [ ! -d "/home/external_databases/pubmlst/Campylobacter/jejuni" ]; then
		mkdir -p /home/external_databases/pubmlst/Campylobacter/jejuni
	fi
	cd /home/external_databases/pubmlst/Campylobacter/jejuni
	python3 -u /home/update/download_pubmlst_data.py ${cpus} >> log 2>&1

}


# Data from custom clustering of cgMLST data
## No update mechanism so we donload again files
update_phiercc() {
	local genus=${1}
	python3 -u /home/update/download_phiercc.py \
		--workspace "${UPDATER_WORKSPACE}" \
		--container_image "${UPDATER_CONTAINER_IMAGE}" \
		--user "${UPDATER_USER}" \
		--host "${UPDATER_HOST}" \
		--genus "${genus}" \
		--output_dir "/home/external_databases/phiercc_local"
}

# Uniref50 and Uniref50 for Virual sequences used by alphafold
## Uses milestone-based python client (selective updates; does NOT wipe other AlphaFold assets)
update_alphafold() {
	python3 -u /home/update/download_alphafold.py \
		--workspace "${UPDATER_WORKSPACE}" \
		--container_image "${UPDATER_CONTAINER_IMAGE}" \
		--user "${UPDATER_USER}" \
		--host "${UPDATER_HOST}" \
		--output_dir "/home/external_databases/alphafold"
	return $?
}
#############
# Main code *
#############


db_name=$1
kraken_type=$2
genus=$3
cpus=$4

# -------------------------
# Metadata for Python clients
# -------------------------
# These values are used by click-based downloaders (VFDB/Kraken) for report metadata.
# Prefer values passed in from update_external_databases.sh (outer caller),
# otherwise fall back to container-derived defaults.
UPDATER_WORKSPACE="${UPDATER_WORKSPACE:-/home/update}"
UPDATER_CONTAINER_IMAGE="${UPDATER_CONTAINER_IMAGE:-plepiseq-wgs-pipeline-updater:latest}"
UPDATER_USER="${UPDATER_USER:-}"
UPDATER_HOST="${UPDATER_HOST:-}"

if [ -z "${UPDATER_USER}" ]; then
    _u="$(id -un 2>/dev/null || true)"
    if [ -z "${_u}" ] || [ "${_u}" == "unknown" ]; then
        UPDATER_USER="uid_$(id -u)"
    else
        UPDATER_USER="${_u}"
    fi
fi

if [ -z "${UPDATER_HOST}" ]; then
    UPDATER_HOST="$(hostname -f 2>/dev/null || hostname)"
fi

if [ ${db_name} == "all" ];then
        echo "Downloading data for kraken2 at: $(date +"%H:%M %d-%m-%Y")"
	update_kraken2 "$kraken_type" >> /dev/null 2>&1
	echo "Downloading data for pangolin at: $(date +"%H:%M %d-%m-%Y")"
        update_pangolin >> /dev/null 2>&1
	echo "Downloading data for freyja at: $(date +"%H:%M %d-%m-%Y")"
        update_freyja >> /dev/null 2>&1
	echo "Downloading data for nextclade at: $(date +"%H:%M %d-%m-%Y")"
        update_nextclade >> /dev/null 2>&1
	echo "Downloading data for AMRfinder_plus at: $(date +"%H:%M %d-%m-%Y")"
	update_amrfinder >> /dev/null 2>&1
	echo "Downloading data for kmerfinder at: $(date +"%H:%M %d-%m-%Y")"
	update_kmerfinder >> /dev/null 2>&1
	echo "Downloading data for metaphlan at: $(date +"%H:%M %d-%m-%Y")"
	update_metaphlan >> /dev/null 2>&1
	echo "Downloading data for pointfinder at: $(date +"%H:%M %d-%m-%Y")"
	update_cge_db pointfinder_db >> /dev/null 2>&1
	echo "Downloading data for disinfinder at: $(date +"%H:%M %d-%m-%Y")"
	update_cge_db disinfinder_db >> /dev/null 2>&1
	echo "Downloading data for mlst_db at: $(date +"%H:%M %d-%m-%Y")"
	update_cge_db mlst_db "/home/kma/kma" >> /dev/null 2>&1
	echo "Downloading data for plasmidfinder at: $(date +"%H:%M %d-%m-%Y")"
	update_cge_db plasmidfinder_db >> /dev/null 2>&1
	echo "Downloading data for resfinder at: $(date +"%H:%M %d-%m-%Y")"
	update_cge_db resfinder_db >> /dev/null 2>&1
	echo "Downloading data for spifinder at: $(date +"%H:%M %d-%m-%Y")"
	update_cge_db spifinder_db >> /dev/null 2>&1
	echo "Downloading data for speciesfinder at: $(date +"%H:%M %d-%m-%Y")"
	update_speciesfinder >> /dev/null 2>&1
	echo "Downloading data for virulencefinder at: $(date +"%H:%M %d-%m-%Y")"
	update_cge_db virulencefinder_db >> /dev/null 2>&1
	echo "Downloading data for vfcb at: $(date +"%H:%M %d-%m-%Y")"
	update_vfdb ${cpus}
	echo "Downloading MLST data at: $(date +"%H:%M %d-%m-%Y")"
	update_mlst ${genus}  >> /dev/null 2>&1
	echo "Downloading cgMLST data at: $(date +"%H:%M %d-%m-%Y")"
	update_cgmlst ${genus} ${cpus} >> /dev/null 2>&1
	echo "Downloading pubmlst data at: $(date +"%H:%M %d-%m-%Y")"
	update_pubmlst ${cpus} >> /dev/null 2>&1
	echo "Downloading enterobase data at: $(date +"%H:%M %d-%m-%Y")"
	update_enterobase ${genus} >> /dev/null 2>&1
	echo "Downloading hiercc data at: $(date +"%H:%M %d-%m-%Y")"
	update_phiercc ${genus} >> /dev/null 2>&1
	echo "Downloading swissprot and uniref50 for viral sequences at: $(date +"%H:%M %d-%m-%Y")"
	update_alphafold >> /dev/null 2>&1
elif [ ${db_name} == "kraken2" ]; then
	update_kraken2 "$kraken_type" >> /dev/null 2>&1
elif [ ${db_name} == "pangolin" ]; then
	update_pangolin >> /dev/null 2>&1
elif [ ${db_name} == "freyja" ]; then
	update_freyja >> /dev/null 2>&1
elif [ ${db_name} == "nextclade" ]; then
	update_nextclade >> /dev/null 2>&1
elif [ ${db_name} == "amrfinder_plus" ]; then
	update_amrfinder >> /dev/null 2>&1
elif [ ${db_name} == "kmerfinder" ]; then
        update_kmerfinder >> /dev/null 2>&1 
elif [ ${db_name} == "metaphlan" ]; then
        update_metaphlan >> /dev/null 2>&1 
elif [ ${db_name} == "pointfinder" ]; then
        update_cge_db pointfinder_db >> /dev/null 2>&1
elif [ ${db_name} == "disinfinder" ]; then
        update_cge_db disinfinder_db >> /dev/null 2>&1
elif [ ${db_name} == "mlstfinder" ]; then
        update_cge_db mlst_db "/home/kma/kma" >> /dev/null 2>&1
elif [ ${db_name} == "plasmidfinder" ]; then
        update_cge_db plasmidfinder_db >> /dev/null 2>&1
elif [ ${db_name} == "resfinder" ]; then
        update_cge_db resfinder_db >> /dev/null 2>&1
elif [ ${db_name} == "spifinder" ]; then
        update_cge_db spifinder_db >> /dev/null 2>&1
elif [ ${db_name} == "speciesfinder" ]; then
        update_speciesfinder >> /dev/null 2>&1
elif [ ${db_name} == "virulencefinder" ]; then
        update_cge_db virulencefinder_db >> /dev/null 2>&1
elif [ ${db_name} == "vfdb" ]; then
	update_vfdb ${cpus} >> /dev/null 2>&1 
elif [ ${db_name} == "mlst" ]; then
        update_mlst ${genus} >> /dev/null 2>&1 
elif [ ${db_name} == "cgmlst" ]; then
	update_cgmlst ${genus} ${cpus} >> /dev/null 2>&1
elif [ ${db_name} == "pubmlst" ]; then
	update_pubmlst ${cpus} >> /dev/null 2>&1
elif [ ${db_name} == "enterobase" ]; then
	update_enterobase ${genus}
elif [ ${db_name} == "phiercc" ]; then
	update_phiercc ${genus} >> /dev/null 2>&1
elif [ ${db_name} == "alphafold" ]; then
	update_alphafold >> /dev/null 2>&1
fi
