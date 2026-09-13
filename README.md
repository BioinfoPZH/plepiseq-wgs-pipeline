# PlEpiSeq – High‑throughput viral & bacterial NGS pipelines (Nextflow + Docker)

PlEpiSeq is a production‑grade collection of Nextflow workflows that turn raw FASTQ files into QC'd, annotated, and lineage‑assigned genomes for

* **Viruses:** SARS‑CoV‑2, Influenza A/B, human RSV (types A & B)
* **Bacteria:** *Salmonella*, *Escherichia*, and *Campylobacter* genera

It supports both **Illumina (paired‑end)** and **Oxford Nanopore (single‑end)** reads, and runs either on a single Linux host or on an HPC cluster via SLURM.

This project is part of the [PlEpiSeq](https://www.pzh.gov.pl/projekty-i-programy/plepiseq/) project, co-funded by the European Union.

Current version: see the [`VERSION`](VERSION) file (**1.8.2**). Release history is in [`CHANGELOG.md`](CHANGELOG.md).

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
* **End‑to‑end automation** – QC → contamination screening → trimming → reference/subtype selection → mapping → dehumanisation → co‑infection analysis → primer‑aware filtering and masking → variant calling → consensus → lineage/clade assignment (Nextclade / Pangolin / Freyja) → resistance analysis → (optional) 3‑D protein modelling with AlphaFold2
* **Both viral & bacterial workflows** with a shared CLI style and a single dispatcher
* **Docker‑based reproducibility** – one `docker build` per image
* **Modular Nextflow design** – 85 module files under `modules/`, of which 78 are wired into the two workflows
* **GPU acceleration** for AlphaFold2, with automatic GPU selection and retry logic
* **Built‑in QC "switches"** – low‑quality samples stop consuming compute early, but still produce a JSON report
* **One machine‑readable result per sample** – `<sample_id>.json`, validated against the schemas in [BioinfoPZH/plepiseq_json](https://github.com/BioinfoPZH/plepiseq_json)

---

## Quick start

### 1. Clone the main repository
```bash
git clone --depth 1 https://github.com/BioinfoPZH/plepiseq-wgs-pipeline.git
cd plepiseq-wgs-pipeline
```

### 2. Build the docker images used by the pipeline
```bash
docker build --target main    -f docker/Dockerfile-viral     -t plepiseq-wgs-pipeline-viral:$(cat VERSION)      -t plepiseq-wgs-pipeline-viral:latest     .
docker build --target manta   -f docker/Dockerfile-manta     -t plepiseq-wgs-pipeline-manta:$(cat VERSION)      -t plepiseq-wgs-pipeline-manta:latest     .
docker build --target updater -f docker/Dockerfile-viral     -t plepiseq-wgs-pipeline-updater:$(cat VERSION)    -t plepiseq-wgs-pipeline-updater:latest   .
docker build                  -f docker/Dockerfile-bacterial -t plepiseq-wgs-pipeline-bacterial:$(cat VERSION)  -t plepiseq-wgs-pipeline-bacterial:latest .
```

Note: For each Dockerfile two tags are created, `latest` and the value from the `VERSION` file. The `latest` tags are the **defaults** used by the helper scripts.

### 3. Download the external reference databases (≈230 GB)
```bash
./update_external_databases.sh --database all --output /mnt/raid/external_databases --credentials_file /path/to/credentials.txt
```

Note: the output path must already exist. Prepare the credentials file from [`sample_credentials.txt`](sample_credentials.txt) — credentials are required to fetch data from the EnteroBase and PubMLST services. This file is git‑ignored; never commit it.

### 4. Create a Nextflow config
Copy [`sample_nextflow.config`](sample_nextflow.config) to `nextflow.config` and adjust it. It must define the `local` and `slurm` profiles and, for bacterial analyses, a valid `params.enterobase_api_token`. See §2.1.6 of the [documentation](doc/dokumentacja.md).

> Nextflow config is Groovy: comments must use `//`, **not** `#`. A `#` comment causes a parse error.

---

## Requirements

| Category              | Minimum                                 | Recommended                           |
|-----------------------|-----------------------------------------|---------------------------------------|
| **OS**                | modern x86‑64 GNU/Linux                 | Ubuntu 20/22 LTS or Debian 12         |
| **Container runtime** | Docker ≥ 24.0                           | Docker 27.x                           |
| **Workflow engine**   | Nextflow ≥ 24.04                        | Nextflow 24.10 (binary in `$PATH`)    |
| **GPU** (optional)    | 1× CUDA‑capable card, ≥ 16 GB VRAM      | 1–8× NVIDIA A100 for AlphaFold2       |
| **RAM**               | 128 GB                                  | ≥ 256 GB, or SLURM with a 250 GB node |
| **Disk**              | 4 TB (images + external databases)      | 4 TB NVMe SSD                         |

The RAM minimum is set by the contamination screening step: `modules/common/kraken2.nf` requests **120 GB**, so a host with less will never schedule it under the `local` profile. The single largest request in the pipeline is 250 GB, by `alphafold_slurm`; under the `local` profile AlphaFold requests far less.

---

## Installation

1. **Clone** this repo (see *Quick start*).
2. **Build the Docker images** as shown above.
3. **AlphaFold2** – clone the DeepMind repo, replace its Dockerfile with ours, and build:
   ```bash
   git clone https://github.com/google-deepmind/alphafold.git
   cd alphafold && git checkout 9b2ed02ff6ecf5c6dd566c765661e9c5d04701e7
   cp ${PATH_TO_PLEPISEQ_REPO}/docker/Dockerfile-alphafold docker/Dockerfile
   docker build -f docker/Dockerfile -t plepiseq-wgs-pipeline-alphafold:latest .
   ```
   Only a subset of the AlphaFold databases is needed — see [Updating external databases](#updating-external-databases).
4. **Medaka & Prokka** – pull the public images (tags are pinned):
   ```bash
   docker pull ontresearch/medaka:sha447c70a639b8bcf17dc49b51e74dfcde6474837b-amd64
   docker pull staphb/prokka:latest
   ```
5. **External DBs** – downloaded in *Quick start*; refresh regularly (see below).
6. **Nextflow config** – see *Quick start* step 4.

If you have no GPU, skip step 3 and pass `--no-alphafold` (see [Skipping AlphaFold](#skipping-alphafold-no-gpu-mode)).

---

## Running the pipelines

### Unified entry point (`run_pipeline.sh`)

The recommended way to run either pipeline is the dispatcher, which routes to the correct sub-wrapper based on `--organism` and forwards every other argument verbatim:

```bash
# Viral example (SARS-CoV-2, Illumina)
./run_pipeline.sh --organism "SARS-CoV-2" --projectDir "$PWD" --reads "/data/fastq/*_R{1,2}.fastq.gz" --machine "Illumina" --primers_id "Artic_V4.1"

# Viral example (Influenza, Nanopore)
./run_pipeline.sh --organism "Influenza" --projectDir "$PWD" --reads "/data/fastq/*fastq.gz" --machine "Nanopore" --primers_id "UniRef"

# Bacterial example (Salmonella, Illumina)
./run_pipeline.sh --organism "Salmonella" --projectDir "$PWD" --reads "/data/fastq/*_R{1,2}.fastq.gz" --machine "Illumina"
```

Accepted `--organism` values are `SARS-CoV-2`, `Influenza`, `RSV` (routed to the viral wrapper as `--species`) and `Salmonella`, `Escherichia`, `Campylobacter` (routed to the bacterial wrapper as `--genus`).

Run `./run_pipeline.sh` without arguments to list the organisms, or add `-h` after `--organism` for pipeline-specific options.

### Direct wrapper scripts

```bash
# Viral
./run_nf_pipeline_viral.sh --projectDir "$PWD" --reads "/data/fastq/*_R{1,2}.fastq.gz" --machine "Illumina" --species "SARS-CoV-2" --primers_id "Artic_V4.1"

# Bacterial
./run_nf_pipeline_bacterial.sh --projectDir "$PWD" --reads "/data/fastq/*fastq.gz" --machine "Nanopore"
```

Both wrappers print a short help with `-h` and the full parameter list, including the tuning parameters, with `--all`.

If you renamed the default images, override them: the viral wrapper takes `--main_image`, `--manta_image`, `--medaka_image` and `--alphafold_image`; the bacterial wrapper takes `--main_image`, `--prokka_image`, `--medaka_image` and `--alphafold_image`.

---

## Key parameters

| Flag                        | Purpose                                          | Typical value / default            |
|-----------------------------|--------------------------------------------------|------------------------------------|
| `--reads`                   | Glob pattern for FASTQ files — **quote it**      | `'/data/*_R{1,2}.fastq.gz'`        |
| `--machine`                 | Sequencer platform                               | `Illumina` or `Nanopore`           |
| `--species`                 | Target organism (viral wrapper)                  | `SARS-CoV-2`, `Influenza`, `RSV`   |
| `--genus`                   | Target genus (bacterial wrapper, optional)       | auto-detected from reads if unset  |
| `--primers_id`              | Amplicon scheme (viral only)                     | see table below                    |
| `--projectDir`              | Path to this repository                          | no default — always pass it        |
| `--external_databases_path` | Path to the external databases                   | `/mnt/raid/external_databases`     |
| `--results_dir`             | Output directory                                 | `./results`                        |
| `--threads`                 | Max CPUs **per sample**                          | `40`                               |
| `--profile`                 | Nextflow executor profile                        | `local` (or `slurm`)               |
| `--adapters_id`             | Trimmomatic adapter set (viral only)             | `TruSeq3-PE-2`                     |
| `--no-alphafold`            | Skip 3‑D protein modelling                       | off; AlphaFold runs by default     |
| `--debug`                   | Add Nextflow trace/DAG/report and `-resume`      | off                                |
| `--all`                     | Print every parameter, including tuning ones     | —                                  |

`--threads` is a **per-sample** cap, not a global one: N concurrent samples may use up to N × 40 CPUs.

### Quality and variant-calling defaults depend on species and platform

Leaving a threshold unset does not give you one value — the wrappers pick an organism- and platform-specific default, tuned empirically to match EQA expectations. The most significant ones:

| Parameter                       | SARS‑CoV‑2   | Influenza    | RSV          |
|---------------------------------|--------------|--------------|--------------|
| `lower_ambig` / `upper_ambig`   | 0.45 / 0.55  | 0.4 / 0.6    | 0.4 / 0.6    |
| `min_cov` / `mask`              | 20 / 20      | 10 / 10      | 10 / 10      |
| `max_number_for_SV`             | 200000       | 10000        | 100000       |
| Nanopore `medaka_chunk_len`     | 5000         | 800          | 10000        |
| Nanopore `medaka_chunk_overlap` | 4000         | 400          | 8000         |
| Nanopore `first_round_pval`     | 0.05         | 0.25         | 0.05         |
| Nanopore `max_depth`            | 3000         | 2000         | 3000         |
| Nanopore `window_size`          | 50           | 50           | 100          |

SARS‑CoV‑2 uses RIVM thresholds; Influenza and RSV use the Pasteur thresholds proposed for EQA.

Platform-dependent defaults, identical across species: Illumina uses `max_depth=1000`, `quality_initial=5`, `length=90`, `quality_snp=15`, `freyja_minq=20`; Nanopore uses `quality_initial=2`, `quality_snp=5`, `freyja_minq=2`, `quality_for_coverage=1`, and `medaka_model=r941_min_sup_variant_g507`. Note that for Nanopore `length` is a **fraction** of the expected amplicon/segment length (`0.49`), not a number of bases.

Run either wrapper with `--all` for the complete list.

### Supported primer schemes

| Virus          | Scheme(s)                                                                                                                                                                                                 |
|----------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **SARS‑CoV‑2** | `Artic_V1`, `Artic_V2`, `Artic_V3`, `Artic_V4`, `Artic_V4.1`, `Artic_V5.3.2`, `Artic_V5.4.2`, `Midnight_1200nt`, `VarSkip_V1a`, `VarSkip_V1a_long`, `VarSkip_V2`, `VarSkip_V2b`, `EQA2023.SARS1`, `EQA2023.SARS2`, `EQA2024.V4_1`, `EQA2024.V4_1.nanopore`, `EQA2024.V5_3`, `EQA2026.SARS1`, `EQA2026.SARS2` |
| **RSV**        | `RSV_WHO-2015`, `RSV_Artic_V1`, `RSV_EQA20206.illumina`, `RSV_EQA20206.nanopore`                                                                                                                           |
| **Influenza**  | `UniRef` (a placeholder — subtype-specific primers are selected automatically)                                                                                                                             |

The wrapper validates `--primers_id` against the list for the selected species and rejects anything else. The authoritative lists live at the top of `run_nf_pipeline_viral.sh`.

---

## Results & output structure

Results are written per sample, flat, with a single `QC/` subdirectory:

```
results/
 └─ <sample_id>/
    ├─ QC/
    │   ├─ *_reads_quality_histogram.csv
    │   ├─ *_reads_length_histogram.csv
    │   └─ *_position_quality_plot.csv
    ├─ <sample_id>.json                    # unified machine-readable summary
    ├─ <sample_id>.fasta                   # consensus genome
    ├─ output_*.fasta                      # per-segment / variant consensus
    ├─ <sample_id>*nohuman.fastq.gz        # dehumanised reads
    ├─ <sample_id>_coverage_histogram.csv
    ├─ <sample_id>_coverage_barplot_*
    ├─ *.pdb                               # AlphaFold2 models, if enabled
    ├─ *allele_usage_histogram.txt         # SARS-CoV-2 co-infection analysis only
    └─ hybrid_genome.fasta                 # Influenza reassortment only
```

Bacterial runs produce a different set: `QC/`, `fastas/`, `<sample_id>.json`, `<sample_id>.fasta`, `<sample_id>_prokka*`, `<sample_id>_gyrase_complex.pdb`, and `enterobase_historical_data.txt` / `pubmlst_historical_data.txt`.

**`<sample_id>.json` is the primary product.** Alignments (BAM), variant calls (VCF), and the raw Nextclade / Pangolin / Freyja tables are deliberately **not** published — they exist only inside Nextflow's `work/` directory and are lost when it is cleaned. Their content is summarised in the JSON, under keys such as `viral_mapping_data`, `viral_classification_data`, `viral_mutation_data`, `viral_genome_data`, `freyja_data`, and `structural_data`. If you need the intermediates, keep `work/` or enable the relevant `publishDir` directives.

The JSON structure follows the schemas published in [BioinfoPZH/plepiseq_json](https://github.com/BioinfoPZH/plepiseq_json).

---

## Updating external databases

```bash
./update_external_databases.sh --database all --output /mnt/raid/external_databases --credentials_file /path/to/credentials.txt
```

`--credentials_file` is required whenever `--database` is one of `enterobase`, `pubmlst`, `cgmlst`, `mlst` or `all`. At NIH Poland these run weekly; a cron/SLURM example is in §5.2 of the [documentation](doc/dokumentacja.md).

Individual databases can be refreshed on their own. Accepted `--database` values:

`all`, `alphafold`, `amrfinder_plus`, `cgmlst`, `disinfinder`, `enterobase`, `freyja`, `kmerfinder`, `kraken2`, `metaphlan`, `mlst`, `mlstfinder`, `nextclade`, `pangolin`, `phiercc`, `plasmidfinder`, `pointfinder`, `pubmlst`, `resfinder`, `speciesfinder`, `spifinder`, `vfdb`, `virulencefinder`

With `--database kraken2` you may also pass `--kraken_type`, which defaults to `standard` and accepts `standard`, `standard_08gb`, `standard_16gb`, `viral`, `minusb`, `pluspf`, `pluspf_08gb`, `pluspf_16gb`, `pluspfp`, `pluspfp_08gb`, `pluspfp_16gb`, `nt` and `eupathdb48`. With `mlst`, `cgmlst` or `enterobase` you may pass `--genus` (`Salmonella`, `Escherichia`, `Campylobacter`, or `all`).

AlphaFold is the exception worth knowing about. Upstream ships nearly 3 TB, but this pipeline models only a small set of well-known proteins, so the large sequence databases (`bfd` and friends) can be skipped. Only `uniref50/`, `pdb_mmcif/`, `pdb_seqres/`, `uniprot/`, `pdb70/` and `params/` are required, and only `uniref50` and `uniprot` are refreshed by the update script.

---

## Hardware guidelines

* **CPU** – the pipeline scales horizontally; more cores shorten multi-sample runs. Budget `--threads` per concurrent sample.
* **RAM** – the binding constraint is contamination screening: Kraken2 requests **120 GB**. Bacterial contamination analysis requests the same. Plan for ≥ 128 GB, and note that concurrent samples multiply this.
* **GPU** – AlphaFold2 is the only step that needs one. No minimum VRAM is enforced by the modules; a 16 GB card such as a T4 completes a SARS-CoV-2 Spike model in roughly 30 minutes, and larger cards are faster rather than mandatory. Under SLURM, `alphafold_slurm` requests 250 GB of host RAM and one GPU.
* **Disk I/O** – fast NVMe or tmpfs for `work/` and for the database path is strongly advised.

---

## Skipping AlphaFold (no GPU mode)

Both pipelines include an AlphaFold2 protein-structure step, which runs by default and needs a GPU plus the AlphaFold image. Disable it with `--no-alphafold` on either wrapper (or on `run_pipeline.sh`, which forwards it). The rest of the workflow is unaffected and the JSON report is still produced, without `structural_data`.

---

## Contributing

Pull requests and issue reports are welcome. Please open an issue first if you plan large changes to modules or Dockerfiles.

Two conventions worth knowing: module files prefixed with an underscore (e.g. `modules/sarscov2/_modeller.nf`) are retired and wired into nothing, and known defects are tracked in [`TODO.md`](TODO.md).

## Documentation

Documentation lives in the [`doc`](doc/) directory:

* [`doc/dokumentacja.md`](doc/dokumentacja.md) – markdown, the working copy
* `doc/dokumentacja.docx` / `doc/dokumentacja.pdf` – the original, February 2026

> The detailed documentation predates release 1.8.0 and is being brought up to date chapter by chapter. Where it disagrees with this README or with the wrapper `--help` output, the code is authoritative.

## License

This repository is released under the **MIT License**.

## Version

The current version is stored in the [`VERSION`](VERSION) file; see [`CHANGELOG.md`](CHANGELOG.md) for the release history.
