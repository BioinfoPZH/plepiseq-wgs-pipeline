import os
import sys
import re
import time
import json
import glob
import shutil
import hashlib
import logging
import subprocess
from datetime import datetime
from multiprocessing import Pool
from pathlib import Path
import click
import requests
from Bio import SeqIO


VFDB_BASE_URL = "https://www.mgc.ac.cn/VFs/Down" # url to Download (yes it is 'Down')
VFDB_HOME = "http://www.mgc.ac.cn/VFs/" # base url
FILES = ["VFDB_setB_nt.fas.gz", "VFs.xls.gz"] # relevant files from vfdb


# ---------------------------- Helper Functions ---------------------------- #

def setup_logging(output_dir: Path):

    log_file_path = os.path.join(output_dir, "log.log")
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s] %(levelname)s: %(message)s',
        handlers=[
            logging.FileHandler(log_file_path, mode='w'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    logging.info("Logging initialized. Output log: %s", log_file_path)


def execute_command(cmd: str):
    """Execute shell command and capture output."""
    process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()
    if stdout:
        logging.debug(stdout.decode())
    if stderr:
        logging.debug(stderr.decode())
    return process.returncode == 0


def download_file_with_retry(url, output_path, max_retries=3, wait_seconds=300):
    """Download file with retries."""
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(url, stream=True, timeout=60)
            if response.status_code == 200:
                logging.info("Downloading %s...", os.path.basename(output_path))
                with open(output_path, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                return True
            else:
                logging.warning("Attempt %d failed with status %d", attempt, response.status_code)
        except Exception as e:
            logging.warning("Attempt %d failed: %s", attempt, str(e))
        if attempt < max_retries:
            logging.info("Waiting %d seconds before retry...", wait_seconds)
            time.sleep(wait_seconds)
    logging.error("Failed to download %s after %d attempts.", url, max_retries)
    return False


def file_md5sum(path):
    """Compute md5 checksum of a file."""
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def vfdb_server_available():
    """Check VFDB server availability."""
    try:
        response = requests.head(VFDB_HOME, timeout=10)
        return response.status_code == 200
    except Exception:
        return False


def read_fasta(fasta_path):
    """Parse FASTA file into dict {header: sequence}."""
    return {f">{rec.description}": str(rec.seq) for rec in SeqIO.parse(fasta_path, "fasta")}


def extract_info_from_header(header, bis=0):
    # Pattern napisany przy pomocy strony https://regexr.com/7voga
    # dla stringa
    # >VFG030121(gb|WP_013988985) (kefB) cation:proton antiporter [Potassium/proton antiporter (VF0838) - Immune modulation (VFC0258)] [Mycobacterium africanum GM041182]
    # wyciaga 7 grup w kolejnosci
    # match.group(1)
    # 'gb|WP_013988985'
    # match.group(2)
    # 'kefB'
    # match.group(3)
    # 'Potassium/proton antiporter'
    # match.group(4)
    # 'VF0838'
    # match.group(5)
    # 'Immune modulation'
    # match.group(6)
    # 'VFC0258'
    # match.group(7)
    # 'Mycobacterium '
    # testowano tez na
    # >VFG021469(gb|WP_000062035) (stbA) type 1 fimbrial protein [Stb (VF0954) - Adherence (VFC0001)] [Salmonella enterica subsp. enterica serovar Heidelberg str. SL476]

    """Extract organism, VF, and VFC info from VFDB FASTA headers."""
    pattern = '^>\\w+\\((\\w+\\|\\w+|\\w+\\|\\w+\\.\\w+)\\)\\s\\((.*)\\)\\s.+\\s\\[(.+)\\s\\((.+)\\)\\s-\\s(.+)\\s\\((.+)\\)\\]\\s\\[(\\w+)\\s.*\\]'
    #  >VFG000371 (yadA) trimeric autotransporter adhesin YadA [YadA (VF0133) - Effector delivery system (VFC0086)] [Yersinia pestis CO92]
    if bis:
        pattern = '^>(\\w)+\\s\\((.*)\\)\\s.+\\s\\[(.+)\\s\\((.+)\\)\\s-\\s(.+)\\s\\((.+)\\)\\]\\s\\[(\\w+)\\s.*\\]'
    match = re.match(pattern, header)
    if not match:
        raise ValueError(f"Header pattern mismatch: {header}")
    return [match.group(i) for i in range(1, 8)]


def run_blast(lista_plikow, start, end):
    """Run makeblastdb on FASTA files in parallel."""
    for plik in lista_plikow[start:end]:
        execute_command(f"makeblastdb -in {plik} -dbtype nucl")
    return True

### Main function ###
@click.command()
@click.option("-o", "--output_dir", required=True, type=click.Path(), help="Output directoryfor vfdb")
@click.option("-c", "--cpus", default=4, show_default=True, help="Number of CPUs for BLAST indexing")
@click.option("-r", "--max_retries", default=3, show_default=True, help="Maximum retries for downloads")
@click.option("-w", "--wait_seconds", default=300, show_default=True, help="Seconds to wait between retries")
def main(output_dir, cpus, max_retries, wait_seconds):

    ### check if output dir for vfdb exists, if not create it
    output_dir = Path(output_dir)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    # Initialize log.log file in vfdb directory
    start_time = datetime.now()
    setup_logging(output_dir)
    logging.info("=== VFDB Downloader started ===")

    # Check VFDB availability
    # do nothing if server is unavailable
    if not vfdb_server_available():
        logging.warning("VFDB server unavailable. Skipping update for now.")
        sys.exit(0)

    # vfdb_md5.json stores md5sum of relevant files from a previous attempt to download data
    # if VFDB is available and relevant files are identical to what we have we will skip the update
    # if files have different md5sum we clean the directory and process the data
    md5_path = os.path.join(output_dir, "vfdb_md5.json")
    old_md5 = {}

    if os.path.exists(md5_path):
        with open(md5_path, "r") as f:
            old_md5 = json.load(f)



    # if 'old' source files are inside directory (they should be) rename them to old
    # if update is required they will be simply removed
    # if update is not required we will restore their original name
    for file_name in FILES:
        if os.path.exists(os.path.join(output_dir, file_name)):
            os.rename(os.path.join(output_dir, file_name), os.path.join(output_dir, file_name + '.old'))


    # Donload files from VFDB and compare md5sum to current version
    updated_files = []
    for file_name in FILES:
        url = f"{VFDB_BASE_URL}/{file_name}"
        local_path = os.path.join(output_dir, file_name)

        # Failed download abort everything
        if not download_file_with_retry(url, local_path, max_retries, wait_seconds):
            if os.path.exists(local_path + ".old"):
                os.rename(local_path + ".old", local_path)
            sys.exit(1)

        # Calculate md5sum of a new file
        new_md5 = file_md5sum(local_path)
        if old_md5.get(file_name) == new_md5:
            logging.info("%s unchanged (MD5 match). Skipping update.", file_name)
            # remove old file (identical to one just downloaded)
            if os.path.exists(local_path + ".old"):
                os.remove(local_path + ".old")
        else:
            # we need to update our database
            updated_files.append(file_name)
            old_md5[file_name] = new_md5
            # remove old file as well we only need new one
            if os.path.exists(local_path + ".old"):
                os.remove(local_path + ".old")

    # updated_files is empty no need to update
    if not updated_files:
        logging.info("All VFDB files are up to date. Nothing to do.")
        sys.exit(0)
    else:
        # clean all subdirectories in output_dir / vfdb we will store new results there
        _ = [shutil.rmtree(os.path.join(output_dir, d)) for d in os.listdir(output_dir) if os.path.isdir(os.path.join(output_dir, d))]


    # Save updated md5s
    with open(md5_path, "w") as f:
        json.dump(old_md5, f, indent=2)

    # Decompress all files not only one that is beeing updated
    for f in FILES:
        if f.endswith(".gz"):
            execute_command(f"gunzip -f {os.path.join(output_dir, f)}")

    fasta_path = os.path.join(output_dir, "VFDB_setB_nt.fas")
    if not os.path.exists(fasta_path):
        logging.error("Missing VFDB_setB_nt.fas after decompression.")
        sys.exit(1)

    # Parse and organize FASTA
    logging.info("Parsing FASTA and organizing directory structure...")
    seq_dict = read_fasta(fasta_path)
    vfdb_dict = {}

    for header, seq in seq_dict.items():
        try:
            seq_id, gene, VF_name, VF_id, VFC_name, VFC_id, org = extract_info_from_header(header)
        except Exception:
            try:
                seq_id, gene, VF_name, VF_id, VFC_name, VFC_id, org = extract_info_from_header(header, bis=1)
            except Exception:
                logging.warning("Skipping sequence: %s", header)
                continue

        gene = gene.replace("/", "-").replace("*", "-").replace("'", "-").replace(" ", "-").replace("<", "-").replace(">", "-")
        gene = re.sub(r"[()]", "", gene)
        key_path = f"{org}/{VFC_id}/{VF_id}"

        vfdb_dict.setdefault(key_path, {}).setdefault(gene, {})[header] = seq

    for dir_path, genes in vfdb_dict.items():
        full_dir = os.path.join(output_dir, dir_path)
        os.makedirs(full_dir, exist_ok=True)
        for gene, seqs in genes.items():
            with open(os.path.join(full_dir, f"{gene}.fa"), "w") as f:
                for h, s in seqs.items():
                    f.write(f"{h}\n{s}\n")

    # Run makeblastdb in parallel
    logging.info("Creating BLAST indices...")
    fasta_files = glob.glob(os.path.join(output_dir, "**/*.fa"), recursive=True)
    pool = Pool(cpus)
    step = len(fasta_files) // cpus or 1
    jobs = [pool.apply_async(run_blast, (fasta_files, i, min(i + step, len(fasta_files))))
            for i in range(0, len(fasta_files), step)]
    pool.close()
    pool.join()

    # gzip back files for next update
    for f in ['VFs.xls', 'VFDB_setB_nt.fas']:
        execute_command(f"gzip {os.path.join(output_dir, f)}")

    end_time = datetime.now()
    logging.info("=== VFDB Downloader finished successfully ===")
    logging.info("Total runtime: %s", str(end_time - start_time).split(".")[0])


if __name__ == "__main__":
    main()
