# PlEpiSeq – High‑throughput viral & bacterial NGS pipelines (Nextflow + Docker)

PlEpiSeq is a production‑grade collection of Nextflow workflows that turn raw FASTQ files into QC’d, annotated, and lineage‑assigned genomes for

* **Viruses:** SARS‑CoV‑2, Influenza A/B, human RSV (types A & B)
* **Bacteria:** *Salmonella*, *Escherichia*, and *Campylobacter* genera

It supports both **Illumina (paired‑end)** and **Oxford Nanopore (single‑end)** reads and can run on aHPC cluster (SLURM profile provided), or in the cloud.

This project is part of [PleEpiSeq](https://www.pzh.gov.pl/projekty-i-programy/plepiseq/) project, co-funded by the European Union.

---

## Table of contents
1. [Features](#features)
2. [Quick start](#quick-start)
3. [Requirements](#requirements)
4. [Installation](#installation)
5. [Running the pipelines](#running-the-pipelines)
6. [Key parameters](#key-parameters)
7. [Results & output structure](#results--output-structure)
8. [Updating external databases](#updating-external-databases)
9. [Hardware guidelines](#hardware-guidelines)
10. [Contributing](#contributing) • [License](#license)

---

## Features
* **End‑to‑end automation** – QC → trimming → mapping → variant calling → genome assembly → lineage/clade assignment (Nextclade / Pangolin / Freyja) → (optional) 3‑D modeling of key proteins via AlphaFold2
* **Both viral & bacterial workflows** with identical CLI style
* **Docker‑based reproducibility** – one `docker build…` per image and you’re done
* **Modular Nextflow design** – each logical step is its own module (43 modules total)
* **GPU acceleration** for AlphaFold2 (automatic GPU selection & retry logic)
* **Built‑in QC “switches”** – low‑quality samples halt downstream steps early

---

## Quick start

```bash
# 1. clone the main repository
git clone --depth 1 https://github.com/mkadlof/pzh_pipeline_viral
cd pzh_pipeline_viral

# 2. build four all docker images
docker build --target main    -f docker/Dockerfile-viral     -t plepiseq-wgs-pipeline-viral:$(cat VERSION) .
docker build --target manta   -f docker/Dockerfile-manta     -t plepiseq-wgs-pipeline-manta:$(cat VERSION) .
docker build --target updater -f docker/Dockerfile-main      -t plepiseq-wgs-pipeline-updater:$(cat VERSION) .
docker build                  -f docker/Dockerfile-bacteria  -t plepiseq-wgs-pipeline-bacteria:$(cat VERSION) .
docker build                  -f docker/Dockerfile-alphafold -t plepiseq-wgs-pipeline-alphafold:$(cat VERSION) .

# 3. download external reference databases (≈230 GB)
./update_external_databases.sh --database all --output /mnt/raid/external_databases
```

---

## Requirements

| Category              | Minimum                 | Recommended                           |
|-----------------------|-------------------------|---------------------------------------|
| **OS**                | modern x86‑64 GNU/Linux | Ubuntu 20/22 LTS or Debian 12         |
| **Container runtime** | Docker ≥ 24.0           | Docker 27.x                           |
| **Workflow engine**   | Nextflow ≥ 24.04        | Nextflow 24.10 (binary in `$PATH`)    |
| **GPU** (optional)    | 1× CUDA‑capable card    | 1–8× NVIDIA A100 80 GB for AlphaFold2 |
| **RAM**               | 96 GB (Kraken2 std.)    | ≥ 128 GB per running sample           |
| **Disk**              | 300 GB (images + DB)    | 1 TB NVMe SSD for temp/work           |

---

## Installation

1. **Clone** this repo (see *Quick start*).
2. **Build Docker images** as shown above.
3. **Medaka & Prokka** – pull public images (exact tags are pinned):
   ```bash
   docker pull ontresearch/medaka:sha447c70a639b8bcf17dc49b51e74dfcde6474837b-amd64
   docker pull staphb/prokka:latest
   ```
4. **External DBs** – already downloaded in *Quick start*; update regularly (see below).
5. Create nextflow config (consult documentation §2.1.6)
---

## Running the pipelines

### Viral samples (`run_nf_pipeline_viral.sh`)

```bash
./run_nf_pipeline_viral.sh   --reads "/data/fastq/*_R{1,2}.fastq.gz"   --machine "Illumina"   --species "SARS-CoV-2"   --primers_id "EQA2023.SARS2"
```

*Same wrapper works for Influenza (`--species Influenza`) and RSV (`--species RSV`) and also for Nanopore reads (`--machine Nanopore`).*
Run the script without arguments to see a bilingual (EN | PL) help message.

### Bacterial samples (`run_nf_pipeline_bacterial.sh`)

```bash
./run_nf_pipeline_bacterial.sh   --reads "/data/fastq/*fastq.gz"   --machine "Nanopore"
```

If you renamed the default images, add `--main_image …`, `--prokka_image …`, `--alphafold_image …`.

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
| **SARS‑CoV‑2** | `V1`, `V3`, `V4`, `V4.1`, `V5.3.2`, `V1200`, `VarSkip2`, `EQA2023.*`, `EQA2024.*` |
| **RSV**        | `V0`, `V1`                                                                        |
| **Influenza**  | *primers_id unused (segment specific)*                                            |

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
* **GPU** – AlphaFold2 is the only step requiring GPU; a GPU with 80 GB RAM is required. 
* **RAM** – Kraken2 standard DB needs ≈ 80 GB RAM per concurrent sample.
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
