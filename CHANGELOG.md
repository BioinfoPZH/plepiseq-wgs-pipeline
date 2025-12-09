# Changelog
All notable changes to this project will be documented in this file.

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

