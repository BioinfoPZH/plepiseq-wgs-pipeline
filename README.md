# PlEpiSeq – High‑throughput viral & bacterial NGS pipelines (Nextflow + Docker)

PlEpiSeq is a production‑grade collection of Nextflow workflows that turn raw FASTQ files into QC'd, annotated, and lineage‑assigned genomes for

* **Viruses:** SARS‑CoV‑2, Influenza A/B, human RSV (types A & B)
* **Bacteria:** *Salmonella*, *Escherichia*, and *Campylobacter* genera

It supports both **Illumina (paired‑end)** and **Oxford Nanopore (single‑end)** reads and can run on aHPC cluster (SLURM profile provided), or in the cloud.

This project is part of [PleEpiSeq](https://www.pzh.gov.pl/projekty-i-programy/plepiseq/) project, co-funded by the European Union.

---

## Table of contents
1. [Features](#features)
2. [Quick start](#quick-start)
3. [Requirements](#requirements)
4. [Installation](#installation)
5. [Running the pipelines](#running-the-pipelines)
6. [Key parameters](#key-parameters)
7. [Results & output structure](#results--output-structure)
8. [Updating external databases](#updating-external-databases)
9. [Hardware guidelines](#hardware-guidelines)
10. [Contributing](#contributing) • [License](#license)

---

## Features
* **End‑to‑end automation** – QC → trimming → mapping → variant calling → genome assembly → lineage/clade assignment (Nextclade / Pangolin / Freyja) → (optional) 3‑D modeling of key proteins via AlphaFold2
* **Both viral & bacterial workflows** with identical CLI style
* **Docker‑based reproducibility** – one `docker build…` per image and you're done
* **Modular Nextflow design** – each logical step is its own module (43 modules total)
* **GPU acceleration** for AlphaFold2 (automatic GPU selection & retry logic)
* **Built‑in QC "switches"** – low‑quality samples halt downstream steps early

---

## Quick start


### 1. Clone the main repository
```
git clone --depth 1 https://github.com/BioinfoPZH/plepiseq-wgs-pipeline.git
cd plepiseq-wgs-pipeline
```
### 2. Build docker images used by the pipeline 
```
docker build --target main    -f docker/Dockerfile-viral     -t plepiseq-wgs-pipeline-viral:$(cat VERSION)       -t plepiseq-wgs-pipeline-viral:latest    .
docker build --target manta   -f docker/Dockerfile-manta     -t plepiseq-wgs-pipeline-manta:$(cat VERSION)       -t plepiseq-wgs-pipeline-manta:latest    .
docker build --target updater -f docker/Dockerfile-viral     -t plepiseq-wgs-pipeline-updater:$(cat VERSION)     -t plepiseq-wgs-pipeline-updater:latest  .
docker build                  -f docker/Dockerfile-bacterial -t plepiseq-wgs-pipeline-bacterial:$(cat VERSION)   -t plepiseq-wgs-pipeline-bacterial:latest .
```

Note: For each Dockerfile two images are created: one tagged `latest` and one tagged with the value from the `VERSION` file. Images tagged with `latest` are used as **defaults** in most helper scripts.


### 3. download external reference databases (≈230 GB)
```
./update_external_databases.sh --database all --output /mnt/raid/external_databases --credentials_file /path/to/credentials
```

Note: Provide the script with an existing path and prepare valid crentail file based on `sample_credentials.txt`. Credentials are required to fetch data from enterobase and pubmlst services.

---

## Requirements

| Category              | Minimum                                 | Recommended                           |
|-----------------------|-----------------------------------------|---------------------------------------|
| **OS**                | modern x86‑64 GNU/Linux                 | Ubuntu 20/22 LTS or Debian 12         |
| **Container runtime** | Docker ≥ 24.0                           | Docker 27.x                           |
| **Workflow engine**   | Nextflow ≥ 24.04                        | Nextflow 24.10 (binary in `$PATH`)    |
| **GPU** (optional)    | 1× CUDA‑capable card                    | 1–8× NVIDIA A100 80 GB for AlphaFold2 |
| **RAM**               | 96 GB (Kraken2 std.)                    | ≥ 128 GB per running sample           |
| **Disk**              | 4 TB (images + <br/>external databases) | 4 TB NVMe SSD                         |

---

## Installation

1. **Clone** this repo (see *Quick start*).
2. **Build Docker images** as shown above.
3. **AlphaFold2** – clone DeepMind repo, copy **our** custom dockerfile from ```docker/Dockerfile``` 
    to AlphaFold2 repository and build an image:
   ```bash
   git clone https://github.com/google-deepmind/alphafold.git
   cd alphafold && git checkout 6350ddd63b3e3f993c7f23b5ce89eb4726fa49e8
   ```
   copy updated Alphafold image from our repo to alphafold repo 
   
   ```
   cp ${PATH_TO_PLEPISEQ_REPO}/docker/Dockerfile-alphafold docker/Dockerfile
   ```
   build an image
   ```
   docker build -f docker/Dockerfile -t plepiseq-wgs-pipeline-alphafold:latest .
   ```
   
4. **Medaka & Prokka** – pull public images (exact tags are pinned):
   ```bash
   docker pull ontresearch/medaka:sha447c70a639b8bcf17dc49b51e74dfcde6474837b-amd64
   docker pull staphb/prokka:latest
   ```
5. **External DBs** – already downloaded in *Quick start*; update regularly (see below).
6. Create nextflow config (consult documentation §2.1.6)
---

## Running the pipelines

### Unified entry point (`run_pipeline.sh`)

The recommended way to run either pipeline is via the unified dispatcher script. It routes
to the correct sub-wrapper based on the `--organism` value:

```bash
# Viral example (SARS-CoV-2, Illumina)
./run_pipeline.sh --organism "SARS-CoV-2" --reads "/data/fastq/*_R{1,2}.fastq.gz" --machine "Illumina" --primers_id "Artic_V4.1"

# Viral example (Influenza, Nanopore)
./run_pipeline.sh --organism "Influenza" --reads "/data/fastq/*fastq.gz" --machine "Nanopore" --primers_id "UniRef"

# Bacterial example (Salmonella, Illumina)
./run_pipeline.sh --organism "Salmonella" --reads "/data/fastq/*_R{1,2}.fastq.gz" --machine "Illumina"
```

All remaining arguments are forwarded verbatim to the appropriate sub-wrapper.
Run `./run_pipeline.sh` without arguments to see available organisms, or add `-h` to see
pipeline-specific options.

### Direct wrapper scripts

You can also call the pipeline-specific wrappers directly.

**Viral samples** (`run_nf_pipeline_viral.sh`):

```bash
./run_nf_pipeline_viral.sh --reads "/data/fastq/*_R{1,2}.fastq.gz" --machine "Illumina" --species "SARS-CoV-2" --primers_id "Artic_V4.1"
```

**Bacterial samples** (`run_nf_pipeline_bacterial.sh`):


```bash
./run_nf_pipeline_bacterial.sh --reads "/data/fastq/*fastq.gz" --machine "Nanopore"
```

If you renamed the default images, add `--main_image`, `--prokka_image`, `--alphafold_image`.

---

## Key parameters

| Flag                        | Purpose                     | Typical value / default          |
|-----------------------------|-----------------------------|----------------------------------|
| `--reads`                   | Glob pattern to FASTQ files | `*_R{1,2}.fastq.gz` (Illumina)   |
| `--machine`                 | Sequencer platform          | `Illumina` or `Nanopore`         |
| `--species`                 | Target organism             | `SARS-CoV-2`, `Influenza`, `RSV` |
| `--primers_id`              | Amplicon scheme (viral)     | see table below                  |
| `--external_databases_path` | Path to DBs                 | `/mnt/raid/external_databases`   |
| `--threads`                 | Max CPUs per process        | defaults to all available        |

Supported primer sets:

| Virus          | Scheme(s)                                                                         |
|----------------|-----------------------------------------------------------------------------------|
| **SARS‑CoV‑2** | `Artic_V1`–`Artic_V5.4.2`, `Midnight_1200nt`, `VarSkip_V1a`, `VarSkip_V2`, `VarSkip_V2b`, `VarSkip_V1a_long`, `EQA2023.*`, `EQA2024.*` |
| **RSV**        | `RSV_WHO-2015`, `RSV_Artic_V1`                                                    |
| **Influenza**  | `UniRef`                                                                          |

For a full list of explicit & implicit options, see §6 of the documentation.

---

## Results & output structure

```
results/
 └─ <sample_id>/
    ├─ QC/                    # FastQC reports (pre/post‑trim)
    ├─ mapping/               # BAM + index
    ├─ genome/                # consensus FASTA, VCF, masks
    ├─ classification/        # Nextclade / Pangolin / Freyja tables
    ├─ structure/             # PDB files (AlphaFold2) – viral only
    └─ sample.json            # unified machine‑readable summary
```

The `sample.json` aggregate combines module‑level outputs such as `mapping_data`, `viral_classification_data`, and `structural_data`.

---

## Updating external databases

Run weekly (cron/SLURM job example is shown in §5.2 of the docs):

```bash
./update_external_databases.sh --database all --output /mnt/raid/external_databases
```

Individual DBs can be refreshed with `--database pangolin`, `--database kraken2`, etc.

---

## Hardware guidelines

* **CPU** – pipeline scales horizontally; more cores shorten multi‑sample runs.
* **GPU** – AlphaFold2 is the only step requiring GPU; a GPU with 80 GB RAM is required. 
* **RAM** – Kraken2 standard DB needs ≈ 80 GB RAM per concurrent sample.
* **Disk I/O** – fast NVMe or tmpfs for `work/` and DB path strongly advised.

---

## Contributing

Pull requests and issue reports are welcome. Please open an issue first if you plan large changes to modules or Dockerfiles.

## Documentation (English)

Documentation is available in the [doc](doc/) directory:

```
doc/dokumentacja.docx
```

This file contains detailed instructions and descriptions of the pipeline components, tailored for local institutional use.

## License

This repository is released under the **MIT License** (see `LICENSE` file).

## VERSION

The current version of this pipeline is stored in the `VERSION` file at the root of this repository.


## Skipping AlphaFold (no GPU mode)

Both the bacterial and viral pipelines include an AlphaFold2-based protein structure prediction step. By default, this step runs and requires a GPU and the docker image with AlphaFold2 program.

You can disable this step by running the wrapper scripts with the `--no-alphafold` flag.
