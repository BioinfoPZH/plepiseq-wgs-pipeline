# Changelog
All notable changes to this project will be documented in this file.

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

