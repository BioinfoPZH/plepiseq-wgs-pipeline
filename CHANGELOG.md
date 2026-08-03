# Changelog
All notable changes to this project will be documented in this file.

## [1.8.0] - 2026-08-03
### Added
- Bacterial pHierCC / HierCC assignment via EnteroBase, PubMLST, and local `plepiseq-cluster` data, with contract-shaped fallbacks so Nextflow can continue on QC/API/DB failures.
- Local HierCC level assignment (`phiercc_local.py`) using indexed clustering assets from `plepiseq-cluster`.
- EQA2026 primer schemes for SARS-CoV-2 and RSV; updated RSV-A/RSV-B reference genomes (PP525321 / OR666591) and matching primer coordinates for RSV_Artic_V1 and RSV_WHO-2015.
- RSV F-protein resistance analysis (nirsevimab / palivizumab / clesrovimab) integrated into the viral JSON report.
- Nanopore structural-variant detection with cuteSV for amplicon-spanning deletions (SARS-CoV-2 and RSV), mirroring the Illumina Manta pattern where Medaka cannot call large SVs.
- Provisional Legionella support: MLST when the Legionella MLST DB is installed, and AMR analysis via AMRFinder.
- Merged filtered bacterial genome FASTA output and `genome_file_merged` / ambiguous-base metrics in consensus JSON.
- `--debug` flag on viral and bacterial shell wrappers for richer Nextflow reporting (`trace` / `dag` / `report`) and `-resume`.

### Changed
- Refactored the bacterial workflow into reusable `modules/bacterial/*.nf` processes; preserved expected outputs on failed-QC paths.
- Migrated pHierCC clustering downloads from the old git-tracked tree to **GitHub Releases** of `BioinfoPZH/plepiseq-cluster` (release-tag versioning).
- Bacterial Nanopore polishing now uses the external pinned Medaka image instead of building Medaka into the bacterial Docker image.
- Bumped FreeBayes to 1.3.10 and report SNPs/indels separately (`--max-complex-gap -1`) instead of complex multi-nucleotide events.
- Tuned viral wrapper defaults (Illumina `max_depth`, species-specific ambiguity / coverage / Nanopore medaka and window settings) for current EQA practice; Influenza/RSV use a wider ambiguity window than SARS-CoV-2.
- FreeBayes ambiguity path no longer applies the strict high-confidence QUAL threshold (ambiguous sites often have low QUAL); high-confidence calls still require QUAL.
- `prep_own_vcf.py` aligns with MUSCLE and hardens trailing-window / `tmp.fasta` handling.
- Raised bacterial Kraken2 memory 100→120 GB and MetaPhlAn Illumina 40→60 GB to avoid SLURM OOM on growing DBs / peak RSS.
- Docker maintenance: pin bacterial tool versions (e.g. SeqSero2, KMA); pin AlphaFold `torch` to 2.12 (avoid setuptools conflict); viral apt index refresh; bacterial MAFFT install restored once the upstream endpoint recovered.
- snpEff hRSV_A/B databases rebuilt for the new RSV references (including NS1 gene fix on RSV-A).

### Fixed
- pHierCC EnteroBase Basic-auth credential formatting; local HierCC lookup for STs below the first index checkpoint; leftover rename to `version_local` / `version_remote`.
- CGE MLST DB updater: fail early at DATABASE_AVAILABILITY when the git remote is unreachable (non-interactive), instead of mid-run after wiping the workspace.
- Bacterial Prokka/MLST dummy outputs and Legionella MLST DB path detection when modules run on failed QC or unsupported genera.
- cuteSV coverage-ratio / VAF tuning and Nanopore consensus wiring for the SV path.
- Trimmomatic forced to Phred33; RSV detect_type / Nextclade / snpEff accession updates; AlphaFold SLURM time limit extended slightly.

## [1.7.1] - 2026-06-21
### Fixed
- Fixed the Freyja database download after upstream replaced the LFS `usher_barcodes.csv` with a gzipped `usher_barcodes.csv.gz`; the client now fetches and decompresses it, staging downloads and restoring the previous database if a run fails.
- Fixed a VFDB crash on non-UTF-8 FASTA headers and hardened the run so a JSON report is always written, the output is backed up/restored on failure, and the checksum manifest is saved only after the final check passes.

## [1.7.0] - 2026-03-02
### Added
- Introduced `run_pipeline.sh` -- a unified dispatcher that routes to the correct sub-wrapper (viral or bacterial) based on `--organism`, providing a single entry point for both pipelines.

### Changed
- Renamed all SARS-CoV-2 primer schemes to descriptive names (e.g. `V1` -> `Artic_V1`, `V1200` -> `Midnight_1200nt`, `VarSkip2` -> `VarSkip_V2`).
- Renamed RSV primer schemes (`V0` -> `RSV_WHO-2015`, `V1` -> `RSV_Artic_V1`).
- Added `UniRef` as a default Influenza primer schema.
- Shell wrapper now validates that the provided primer scheme matches the selected species.
- removed hostname from clusterOptions directive in modules running alphafold


## [1.6.0] - 2026-02-28
### Changed
- Replaced all database update scripts in `bin/update/` with milestone-based Python clients. Each client verifies internet connectivity, database availability, and expected output files before and after every download, preventing silent data loss. Clients produce a structured JSON report (schema: `https://github.com/BioinfoPZH/plepiseq_json`) and a log file per run.
- Unified EnteroBase and PubMLST credentials into a single `key=value` file (`--credentials_file`) mounted read-only into the container, replacing the previously used token file that was copied to a docker image. See `sample_credentials.txt` for the expected format.

## [1.5.3] - 2026-02-23

### Changed
- Updated SpeciesFinder parsing to use a custom scoring function for species/genus selection based on filtered hits (`Template_length >= 1 Mbp`) and the mean z-score of `Depth` and `Template_Identity` columns from results.spa.
- Updated contamination JSON generators to support SpeciesFinder `results.txt` format in place of legacy KmerFinder parsing.

### Fixed
- Ensured contamination summary selection always reports two distinct species; when no different secondary species is available, the output now uses `None` with coverage `0`.

## [1.5.2] - 2026-02-20

### Fixed
- Fixed NumPy 2.0 compatibility in Python scripts by replacing deprecated `np.alltrue` with `np.all`.
- Fixed VCF generation in `bin/sarscov2/prep_own_vcf.py` for newer `iranges` releases by normalizing interval bounds consistently for Python slicing.
- Fixed bacterial Docker image setup for Medaka installation.

### Changed
- Updated viral Docker image dependency set to satisfy Pangolin requirements (including NumPy/scikit-learn alignment and Freyja dependency handling).
- Updated Python dependency constraints in `requirements.txt` (including `iranges` update and restoration of Scorpio/Constellation installation path).
- Updated bacterial image tables to version `3.8.0` to match current SISTR requirements.
- Applied maintenance updates across Dockerfiles (`Dockerfile-viral`, `Dockerfile-bacterial`, `Dockerfile-manta`, `Dockerfile-alphafold`), including pip argument handling improvements.

## [1.5.1] - 2026-02-11

### Fixed
- Fixed Python virtualenv `PATH` in viral Dockerfile (removed double-slash `//opt/venv` paths) to prevent pip install failures.

### Changed
- Viral image: pin Python packaging toolchain for build reproducibility (`setuptools<81`) and add `Cython` to support building Python extensions during `pip install`.
- Viral image: install Freyja without dependencies (`--no-deps`) to avoid upstream dependency resolution/build issues.
- Viral image: add `libdatrie1` to support Snakemake/`datrie` runtime requirements.


## [1.5.0] - 2026-02-11
### Changed
- Update `Dockerfile-bacterial` base to Ubuntu 22.04/CUDA 12.1 and Python 3.10
- Replace KmerFinder (obsolete since 12.2025) with SpeciesFinder
- Update MetaPhlAn version to 4.2.4 
- Update Spades version to 4.3.0
- Fixed versions for most programs and their dependencies
- Adjusted memory requirments of modules that use MetaPhlAn and SpeciesFinder


## [1.4.6] - 2026-02-10
### Fixed
- Update `Dockerfile-bacterial`: ensure EToKi uses a working BBMap download URL and has access to `usearch`.

## [1.4.5] - 2026-01-20
### Fixed
- Update Dockerfile-alphafold to accept Conda default channels' Terms of Service required for non-interactive installs.


## [1.4.4] - 2026-01-19
### Fixed
- Update AMRfinder from version 4.0 to 4.2.5. Version 4.2.5 requires database version at least 2025-12-03.1


## [1.4.3] - 2025-12-27
### Changed
- Enforced `plepiseq-wgs-pipeline` as the recommended prefix for Docker images created during pipeline installation.
- Updated default image names in `run_nf_pipeline_bacterial.sh`, `run_nf_pipeline_viral.sh`, and `update_external_databases.sh`.
- Updated `README.md` with the correct Docker image names, an improved AlphaFold2 installation section, and an updated requirements section.

### Fixed
- Restored the use of the `latest` tag for the most up-to-date version of each image in the shell wrappers.


## [1.4.2] - 2025-12-26
### Changed
- Updated the default name of the containers.


## [1.4.1] - 2025-12-23
### Changes
- restored first_round_pval default value to 0.05. 

## [1.4.0] - 2025-12-16
### Changed
- Removed default values from Nextflow files; the shell wrapper is now the main gateway to execute the pipeline.
- Updated documentation files to reflect changes introduced up to this version.
- Removed redundant Writerside documentation sources (auxiliary doc project files); kept `dokumentacja.docx` and `dokumentacja.pdf` as the main documentation.


## [1.3.1] - 2025-12-09
### Fixed
- Use the correct `uniref50` directory in the AlphaFold UniRef database update function.


## [1.3.0] - 2025-11-30
### Added
- Support for VarSkip primers for SARS-CoV-2.


## [1.2.0] - 2025-10-29
### Added
- Add `--no-alphafold` flag to shell wrappers to skip protein structure generation with AlphaFold.


## [1.1.1] - 2025-10-28
### Added
- Rewrite the update script that downloads the KmerFinder database.
- Rewrite the update script that downloads the VFDB database.


### Changed
- KmerFinder is now downloaded from <https://cge.food.dtu.dk/services/KmerFinder/>, and an update mechanism for that database was introduced.
- The VFDB script now checks if the database is available, downloads the data, and verifies that the expected files are present. This process is repeated up to three times if any of the checks fail.


## [1.1.0] - 2025-09-26
### Changed
- Update bacterial FASTQC parsing script to handle nanopore data.
- Fix installation of the MLST database from CGE.
- Introduce a QC switch for modules merging BAMs after subsampling. If subsampling returns fewer than 10 valid reads, the QC flag is set to `nie`.
- Increase the maximum execution time for the SPAdes module from 20 to 30 minutes.
- Create a single script that downloads cgMLST and MLST data from EnteroBase and remove old organism- and database-specific scripts. The script now checks internet connectivity and verifies the existence of files. The process automatically resumes up to three times if any of the checks fail.


## [1.0.0] - 2025-07-17
### Changed
- Introduce the `VERSION` file.
- Mark this as the first production-ready version of the program.

