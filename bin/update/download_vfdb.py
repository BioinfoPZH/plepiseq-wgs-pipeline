from utils.net import check_url_available, StatusType
from utils.download_helpers import _download_file_with_retry
from utils.report import ReportBuilder, ALL_STEPS, SCHEMA_VERSION
from utils.run_id import generate_run_id
from utils.updates_helpers import file_md5sum
from utils.setup_logging import _setup_logging
from utils.generic_helpers import _dir_removal, _execute_command, get_timestamp
from utils.validation import verify_expected_files

import getpass
import socket
import os
import json
from pathlib import Path
from Bio import SeqIO
import re
from multiprocessing.dummy import Pool as ThreadPool
from functools import partial

import glob
import logging
from  typing import List, Dict, Optional, Any, Tuple
import click

### Database specific section ###
#################################
# Determines urls, expected files (downloaded, processed), updating mechanism etc.
# This is static and database-specific.
################################


DATABASE = {"name": "vfdb", "category": "Virulence factors database"}
SOURCE = {
    "source_type": "https",
    "reference": "https://www.mgc.ac.cn/VFs/Down",
    "expected_raw_files": ["VFDB_setB_nt.fas.gz", "VFs.xls.gz"],
    "expected_processed_files": ["Salmonella/VFC0001/VF0102/fimA.fa",
                                 "Salmonella/VFC0001/VF0102/fimA.fa.ndb",
                                 "Salmonella/VFC0001/VF0970/siiE.fa",
                                 "Salmonella/VFC0001/VF0970/siiE.fa.not",
                                 "Salmonella/VFC0325/VF0396/mig-5.fa",
                                 "Salmonella/VFC0325/VF0396/mig-5.fa.nsq",
                                 "Escherichia/VFC0235/VF1134/hlyE-clyA.fa",
                                 "Escherichia/VFC0235/VF1134/hlyE-clyA.fa.ntf",
                                 "Escherichia/VFC0083/VF1109/tia.fa",
                                 "Escherichia/VFC0346/VF0215/aatC.fa",
                                 "Escherichia/VFC0346/VF0215/aatC.fa.nto",
                                 "Campylobacter/VFC0001/VF0322/cadF.fa",
                                 "Campylobacter/VFC0001/VF0322/cadF.fa.ndb",
                                 "Campylobacter/VFC0001/VF0637/Cj1279c.fa",
                                 "Campylobacter/VFC0001/VF0637/Cj1279c.fa.ndb",
                                 "Campylobacter/VFC0272/VF0725/Cj0178.fa",
                                 "Campylobacter/VFC0272/VF0725/Cj0178.fa.nin"]
}

### End of Database specific section ###



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


def _makeblastdb_one(path: str, logger: logging.Logger) -> tuple[str, bool]:
    ok = _execute_command(["makeblastdb", "-in", path, "-dbtype", "nucl"], logger=logger)
    return path, ok

def download_vfdb_raw_files(
    output_dir: Path,
    reference_url: str,
    logger: logging.Logger,
    expected_raw_files: List[str],
    max_retries: int = 3,
    interval: int = 300,
) -> Dict[str, Any]:

    started_at = get_timestamp()

    # Backup existing files (only if they exist)
    for file_name in expected_raw_files:
        file_local_path = output_dir / file_name
        file_local_path_old = output_dir / f"{file_name}.old"

        if file_local_path.exists():
            # remove stale backup to avoid confusion
            if file_local_path_old.exists():
                file_local_path_old.unlink()
            os.replace(file_local_path, file_local_path_old)

    failed_file: Optional[str] = None
    attempts_used_max = 1

    for file_name in expected_raw_files:
        url = f"{reference_url.rstrip('/')}/{file_name}"
        file_local_path = output_dir / file_name

        ok, attempts_used = _download_file_with_retry(
            url=url,
            output_path=file_local_path,
            logger=logger,
            max_retries=max_retries,
            wait_seconds=interval,
        )
        attempts_used_max = max(attempts_used_max, attempts_used)

        if not ok:
            failed_file = file_name
            break

    finished_at = get_timestamp()

    if failed_file is not None:
        # Restore old files
        for file_name in expected_raw_files:
            local_path = output_dir / file_name
            old_path = output_dir / f"{file_name}.old"

            if local_path.exists():
                local_path.unlink()
            if old_path.exists():
                os.replace(old_path, local_path)

        return {
            "status": StatusType.FAILED.value,
            "message": f"Failed to download requested file: {failed_file}",
            "started_at": started_at,
            "finished_at": finished_at,
            "attempts": attempts_used_max,
            "retryable": True,
            "metrics": {
                "failed_file": failed_file,
                "files_total": len(expected_raw_files),
                "reference_url": reference_url,
            },
        }

    # Success: remove backups
    for file_name in expected_raw_files:
        old_path = output_dir / f"{file_name}.old"
        if old_path.exists():
            old_path.unlink()

    return {
        "status": StatusType.PASSED.value,
        "message": "All raw files downloaded successfully",
        "started_at": started_at,
        "finished_at": finished_at,
        "attempts": attempts_used_max,
        "retryable": True,
        "metrics": {
            "files_total": len(expected_raw_files),
            "files_downloaded": len(expected_raw_files),
            "reference_url": reference_url,
        },
    }



def determine_update_status_checksum_manifest(
    output_dir: Path,
    expected_raw_files: List[str],
    logger: logging.Logger,
    md5_filename: str = "vfdb_md5.json",
    keep_dirs: Tuple[str, ...] = ("logs",'reports',),
) -> Tuple[Dict[str, Any], Dict[str, Any], bool, Dict[str, str]]:
    """
    Compare MD5 checksums for expected raw files with previously stored manifest.
    Returns:
      milestone_dict,
      update_decision_kwargs (for ReportBuilder.set_update_decision),
      update_required,
      new_md5
    """

    started_at = get_timestamp()
    first_build = False

    md5_path = output_dir / md5_filename

    # Load old checksums (or init empty baseline)
    if md5_path.exists():
        with open(md5_path, "r", encoding="utf-8") as f:
            old_md5: Dict[str, str] = json.load(f)
    else:
        old_md5 = {}
        first_build = True

    new_md5: Dict[str, str] = {}
    changed_files: List[str] = []

    # Compute new checksums and determine if it's different from its predecessor
    for file_name in expected_raw_files:
        local_path = output_dir / file_name
        checksum = file_md5sum(str(local_path))
        new_md5[file_name] = checksum

        if old_md5.get(file_name, "") != checksum:
            changed_files.append(file_name)

    finished_at = get_timestamp()


    update_required = (not md5_path.exists()) or (len(changed_files) > 0)

    if update_required:
        msg = "Update required: checksum change detected." if md5_path.exists() else "No previous manifest: treating as first build."
        logger.info("%s Changed files: %s", msg, ", ".join(changed_files) if changed_files else "(baseline)")

        # WE clean up all the subdirecotirs that hold processed files (we only keep log subdirectory, and file in the output dir)

        _dir_removal(directory=output_dir,
                    keep_dirs=keep_dirs,
                    logger=logger)

        milestone = {
            "status": StatusType.PASSED.value,
            "message": msg,
            "started_at": started_at,
            "finished_at": finished_at,
            "attempts": 1,
            "retryable": False,
            "metrics": {
                "md5_manifest_path": str(md5_path),
                "manifest_present": md5_path.exists(),
                "update_required": True,
                "changed_files": changed_files,
            },
        }

        # Save the md5sums for future reference
        with open(md5_path, "w", encoding="utf-8") as f:
            json.dump(new_md5, f, indent=2)


        update_decision = {
            "mode": "checksum_manifest",
            "result": "updated",
            "message": msg,
            "checksums_before": [{"file_name": k, "checksum": v} for k, v in old_md5.items()],
            "checksums_after": [{"file_name": k, "checksum": v} for k, v in new_md5.items()],
            "first_build": first_build,
        }

        return milestone, update_decision, True, new_md5

    # No changes detected
    msg = "No update required: checksums match previous manifest."
    logger.info("%s", msg)

    milestone = {
        "status": StatusType.PASSED.value,
        "message": msg,
        "started_at": started_at,
        "finished_at": finished_at,
        "attempts": 1,
        "retryable": False,
        "metrics": {
            "md5_manifest_path": str(md5_path),
            "manifest_present": True,
            "update_required": False,
            "changed_files": [],
        },
    }

    update_decision = {
        "mode": "checksum_manifest",
        "result": "latest_version_present",
        "message": msg,
        "checksums_before": [{"file_name": k, "checksum": v} for k, v in old_md5.items()],
        "checksums_after": [{"file_name": k, "checksum": v} for k, v in new_md5.items()],
        "first_build": first_build,
    }

    return milestone, update_decision, False, new_md5



def process_vfdb(
    output_dir: Path,
    expected_raw_files: List[str],
    logger: logging.Logger,
    cpus: int = 8,
) -> Dict[str, Any]:

    started_at = get_timestamp()

    # 1) Decompress expected raw files that are gz
    for fname in expected_raw_files:
        p = output_dir / fname
        if p.name.endswith(".gz"):
            ok = _execute_command(cmd = f"gunzip -f {p}",
                                  logger=logger)
            if not ok:
                return {
                    "status": StatusType.FAILED.value,
                    "message": f"Failed to gunzip {p.name}",
                    "started_at": started_at,
                    "finished_at": get_timestamp(),
                    "attempts": 1,
                    "retryable": False,
                    "metrics": {"file": p.name},
                }

    fasta_path = output_dir / "VFDB_setB_nt.fas"

    if not fasta_path.exists():
        return {
            "status": StatusType.FAILED.value,
            "message": "Missing VFDB_setB_nt.fas after decompression.",
            "started_at": started_at,
            "finished_at": get_timestamp(),
            "attempts": 1,
            "retryable": False,
            "metrics": {"expected": str(fasta_path)},
        }

    # 2) Parse and organize FASTA
    logger.info("Parsing FASTA and organizing directory structure...")
    seq_dict = read_fasta(str(fasta_path))
    vfdb_dict: Dict[str, Dict[str, Dict[str, str]]] = {}

    skipped_headers = 0

    for header, seq in seq_dict.items():
        try:
            seq_id, gene, VF_name, VF_id, VFC_name, VFC_id, org = extract_info_from_header(header)
        except Exception:
            try:
                seq_id, gene, VF_name, VF_id, VFC_name, VFC_id, org = extract_info_from_header(header, bis=1)
            except Exception:
                skipped_headers += 1
                logger.warning("Skipping sequence header (unparsed): %s", header)
                continue

        gene = (
            gene.replace("/", "-")
                .replace("*", "-")
                .replace("'", "-")
                .replace(" ", "-")
                .replace("<", "-")
                .replace(">", "-")
        )
        gene = re.sub(r"[()]", "", gene)

        key_path = f"{org}/{VFC_id}/{VF_id}"
        vfdb_dict.setdefault(key_path, {}).setdefault(gene, {})[header] = seq

    files_written = 0

    for dir_path, genes in vfdb_dict.items():
        full_dir = output_dir / dir_path
        full_dir.mkdir(parents=True, exist_ok=True)
        for gene, seqs in genes.items():
            out_fa = full_dir / f"{gene}.fa"
            with open(out_fa, "w", encoding="utf-8") as f:
                for h, s in seqs.items():
                    f.write(f"{h}\n{s}\n")
            files_written += 1

    # 3) Build BLAST indices
    logger.info("Creating BLAST indices...")
    fasta_files = glob.glob(str(output_dir / "**" / "*.fa"), recursive=True)

    if not fasta_files:
        return {
            "status": StatusType.FAILED.value,
            "message": "No .fa files were produced for BLAST indexing.",
            "started_at": started_at,
            "finished_at": get_timestamp(),
            "attempts": 1,
            "retryable": False,
            "metrics": {"files_written": files_written},
        }


    # Run makeblastdb in parallel
    worker = partial(_makeblastdb_one, logger=logger)

    with ThreadPool(cpus) as pool:
        results = pool.map(worker, fasta_files)

    failed_files = [p for (p, ok) in results if not ok]
    failed = len(failed_files)



    if failed > 0:
        return {
            "status": StatusType.FAILED.value,
            "message": f"makeblastdb failed for {failed} files.",
            "started_at": started_at,
            "finished_at": get_timestamp(),
            "attempts": 1,
            "retryable": False,
            "metrics": {"fasta_files": len(fasta_files), "failed": failed},
        }

    # 4) gzip back specific raw files
    for fname in ("VFs.xls", "VFDB_setB_nt.fas"):
        p = output_dir / fname
        if p.exists():
            ok = _execute_command(f"gzip -f {p}", logger)
            if not ok:
                return {
                    "status": StatusType.FAILED.value,
                    "message": f"Failed to gzip {p.name}",
                    "started_at": started_at,
                    "finished_at": get_timestamp(),
                    "attempts": 1,
                    "retryable": False,
                    "metrics": {"file": p.name},
                }

    return {
        "status": StatusType.PASSED.value,
        "message": "Processing completed successfully.",
        "started_at": started_at,
        "finished_at": get_timestamp(),
        "attempts": 1,
        "retryable": False,
        "metrics": {
            "headers_total": len(seq_dict),
            "headers_skipped": skipped_headers,
            "fa_files_written": files_written,
            "blast_jobs": len(fasta_files),
        },
    }


@click.command()
@click.option("--workspace", type=str, help="Workspace path.", required=True)
@click.option("--run_id", type=str, default=None, help="Unique run ID.")
@click.option("--container_image", type=str, help="Container image name.", required=True)
@click.option("--report_file", type=str, default=None, help="Report file name.")
@click.option("--log_file", type=str, default="log.log", help="Log file name.")
@click.option("--user", type=str, help="User name.", required=True)
@click.option("--host", type=str, help="Host name.", required=True)
@click.option("--output_dir", type=str, default=str(Path.cwd() / "vfdb_data"), help="Output directory.")
@click.option("--cpus", type=int, default=40, help="Number of CPUs.")
def main(workspace: str,
         container_image: str,
         user: str,
         host: str,
         run_id: str | None = None,
         report_file: str | None = None,
         log_file: str = "log.log",
         output_dir: str = str(Path.cwd() / "vfdb_data"),
         cpus: int = 40,
         ) -> None:

    ### create a unique id for this process
    if not run_id:
        run_id = generate_run_id(DATABASE["name"])

    ### setup output dir
    if not output_dir:
        output_dir = str(Path.cwd() / DATABASE["name"])  # choose your preferred default
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # logging

    log_dir = output_dir / "logs"
    if run_id not in Path(log_file).stem:
        log_file = Path(log_file).stem + f"_{run_id}" + Path(log_file).suffix

    logger = _setup_logging(output_dir = log_dir, filename = log_file)

    ### execution context is dynamic
    execution_context = {
        "workspace": workspace,
        "user": user if user else getpass.getuser(),
        "host": host if host else socket.gethostname(),
        "container_image": container_image,
    }

    ### set up report file
    if report_file is None:
        report_file = f"{run_id}.json"
    else:
        if run_id not in report_file:
            report_file = f"{run_id}.json"

    report_dir = output_dir / "reports"
    Path(report_dir).mkdir(parents=True, exist_ok=True)

    # set up report json schema
    rb = ReportBuilder.start(
        schema_version=SCHEMA_VERSION,
        database=DATABASE,
        execution_context=execution_context,
        run_id=run_id,
        source=SOURCE,
        log_file=str(report_dir / report_file),
    )

    ### AutoFill remaining Milestones in the report
    def skip_remaining_steps(remaining_steps: list[str], reason: str) -> None:
        """
        Adds milestones with SKIPPED status, for 'downstream' milestones with an identical message
        """
        for step in remaining_steps:
            rb.add_skipped(step, reason)


    ### STEPS ###
    # -----------------------------
    # 1) PREFLIGHT_CONNECTIVITY
    # -----------------------------
    pre = check_url_available("https://www.google.com", retries=3, interval=10, logger=logger)
    rb.add_named_milestone("PREFLIGHT_CONNECTIVITY", pre)
    ALL_STEPS.remove("PREFLIGHT_CONNECTIVITY")

    if pre["status"] != StatusType.PASSED.value:
        skip_remaining_steps(ALL_STEPS, "Skipped due to failed preflight connectivity.")
        rb.fail(
            code="NO_INTERNET",
            message=f"Preflight connectivity failed: {pre.get('message', '')}",
            retry_recommended=True,
        )
        rb.finalize("FAIL")
        rb.write(str(report_dir / report_file))
        return

    # -----------------------------
    # 2) DATABASE_AVAILABILITY
    # -----------------------------

    db_access = check_url_available(SOURCE['reference'], retries=3, interval=30, logger=logger)
    rb.add_named_milestone("DATABASE_AVAILABILITY", db_access)
    ALL_STEPS.remove("DATABASE_AVAILABILITY")


    if db_access["status"] != StatusType.PASSED.value:
        skip_remaining_steps(ALL_STEPS, "Skipped due to failed database availability check.")
        rb.fail(
            code="DATABASE_UNAVAILABLE",
            message=f"VFDB endpoint not reachable: {db_access.get('message', '')}",
            retry_recommended=True,
        )
        rb.finalize("FAIL")
        rb.write(str(report_dir / report_file))
        return

    # -----------------------------
    # 3) Download database
    # -----------------------------

    STEP = 'REMOTE_FILES_DOWNLOAD_STATUS'
    downloading_report = download_vfdb_raw_files(
        output_dir=output_dir,
        reference_url=SOURCE["reference"],
        expected_raw_files=SOURCE["expected_raw_files"],
        logger=logger,
        max_retries=3,
        interval=300,
    )
    rb.add_named_milestone(STEP, downloading_report)
    ALL_STEPS.remove(STEP)
    if downloading_report["status"] != StatusType.PASSED.value:
        skip_remaining_steps(ALL_STEPS, "Skipped: failed to download raw files.")
        rb.finalize("FAIL")
        rb.write(str(report_dir / report_file))
        return

    # -----------------------------
    # Determine UPDATE_STATUS
    # -----------------------------
    STEP = "UPDATE_STATUS"

    milestone, update_decision, update_required, _new_md5 = determine_update_status_checksum_manifest(
        output_dir=output_dir,
        expected_raw_files=SOURCE["expected_raw_files"],
        logger=logger,
        md5_filename="vfdb_md5.json",
        keep_dirs=("logs", "reports"),
    )

    rb.add_named_milestone(STEP, milestone)
    ALL_STEPS.remove(STEP)

    rb.set_update_decision(**update_decision)

    if milestone["status"] != StatusType.PASSED.value:
        skip_remaining_steps(ALL_STEPS, "Skipped: Failed update step.")
        rb.fail(code="UPDATE_DECISION_FAILED", message=milestone["message"], retry_recommended=False)
        rb.finalize("FAIL")
        rb.write(str(report_dir / report_file))
        return

    if not update_required:
        skip_remaining_steps(ALL_STEPS, "Skipped: latest version already present.")
        rb.finalize("SKIPPED")
        rb.write(str(report_dir / report_file))
        return

    # ---------------------------------------------------------
    # Process files
    # ---------------------------------------------------------

    STEP = "PROCESSING_STATUS"
    proc = process_vfdb(
        output_dir=output_dir,
        expected_raw_files=SOURCE["expected_raw_files"],
        logger=logger,
        cpus=cpus,
    )
    rb.add_named_milestone(STEP, proc)
    ALL_STEPS.remove(STEP)

    if proc["status"] != StatusType.PASSED.value:
        skip_remaining_steps(ALL_STEPS, "Skipped: processing failed.")
        rb.fail(code="PROCESSING_FAILED", message=proc["message"], retry_recommended=False)
        rb.finalize("FAIL")
        rb.write(str(report_dir / report_file))
        return

    # ---------------------------------------------------------
    # Final check
    # ---------------------------------------------------------

    STEP = "FINAL_STATUS"
    final = verify_expected_files(
        base_dir=output_dir,
        expected_files=SOURCE["expected_processed_files"],
    )
    rb.add_named_milestone(STEP, final)
    ALL_STEPS.remove(STEP)

    if final["status"] != StatusType.PASSED.value:
        rb.fail(code="FINAL_STATUS_FAILED", message=final["message"], retry_recommended=False)
        rb.finalize("FAIL")
        rb.write(str(report_dir / report_file))
        return

    rb.finalize("PASS")
    rb.write(str(report_dir / report_file))
    
if __name__ == '__main__':
    main()