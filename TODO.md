# TODO — known code defects

Findings from the documentation audit of 2026-09-13, recorded against version
`1.8.0` (commit `9901e9e`). These are **code** problems, not documentation gaps:
they should be fixed in the source rather than described in the docs. The
documentation audit itself is tracked separately.

Ordered roughly by severity.

---

## 1. `--projectDir` is unvalidated but has no default

**Files:** `run_nf_pipeline_viral.sh` (50, 432, 577), `run_nf_pipeline_bacterial.sh` (13, 326, 391)

Both wrappers initialise `projectDir=""` and both use it to locate the workflow
file, but neither includes it in the required-parameter check:

```bash
# run_nf_pipeline_viral.sh:432
if [[ -z "$machine" || -z "$reads" || -z "$primers_id" || -z "$species" ]]; then

# run_nf_pipeline_bacterial.sh:326
if [[ -z "$machine" || -z "$reads" ]]; then
```

Omitting `--projectDir` therefore passes validation and reaches:

```bash
# run_nf_pipeline_viral.sh:577
nextflow run ${projectDir}/nf_pipeline_viral.nf \
    --projectDir ${projectDir} \
```

which expands to `nextflow run /nf_pipeline_viral.nf` plus a bare `--projectDir`
flag. The failure is an obscure "file not found" on an absolute path the user
never typed.

**Fix:** add `projectDir` to the `-z` check in both wrappers. Optionally default
it to the script's own location, which is already computed in `run_pipeline.sh`
as `SCRIPT_DIR`, making the flag unnecessary in the common case.

---

## 2. Unset variables are passed as bare flags and become boolean `true`

**Files:** `run_nf_pipeline_viral.sh` (577-620)

Every parameter is interpolated into the `nextflow run` command unconditionally,
including the ones left empty because they do not apply to the current platform
or species. With `--machine Illumina`, seven Nanopore-only variables are still
empty and still interpolated, so Nextflow receives:

```
--bed_offset --extra_bed_offset --medaka_model --medaka_chunk_len \
--medaka_chunk_overlap --first_round_pval --second_round_pval
```

Nextflow treats a long option with no value as a boolean and sets each of these
params to `true`. Verified by probe, not by reading.

Today this is latent, because nothing reads those params on the Illumina path.
`--variant` is the same pattern and is closer to biting: it is empty for
SARS-CoV-2 and RSV (only the Influenza branch sets `variant="UNK"`), so
`params.variant` becomes `true`, and it is read unquoted by
`modules/infl/detect_subtype.nf:61` and
`modules/infl/detect_subtype_nanopore.nf:64`:

```bash
if [ ${params.variant} == 'UNK' ] ; then
```

This is safe only by accident — those two modules run for Influenza alone. Any
future use of a Nanopore-only param on the Illumina path, or of `params.variant`
outside `modules/infl/`, will silently compare against the string `true`.

**Fix:** build the argument list conditionally, appending each flag only when its
variable is non-empty, e.g.

```bash
args=()
[[ -n "${medaka_model}" ]] && args+=( --medaka_model "${medaka_model}" )
```

Quoting the interpolations in the module scripts (`"${params.variant}"`) is a
worthwhile hardening step regardless.

---

## 3. `params.variant` is used but never declared

**Files:** `nf_pipeline_viral.nf`, `modules/infl/detect_subtype.nf:61`, `modules/infl/detect_subtype_nanopore.nf:64`

`nf_pipeline_viral.nf` declares defaults for every other parameter (lines 5-90)
but not `params.variant`. The pipeline works only because the wrapper always
supplies the flag. Invoking `nextflow run nf_pipeline_viral.nf` directly for
Influenza — as the documentation's own advanced-usage examples suggest — fails
with an unresolved-property error.

**Fix:** add `params.variant = "UNK"` alongside the other declarations.

---

## 4. `params.lan` is hardcoded and cannot be selected

**Files:** `nf_pipeline_viral.nf:88`, `nf_pipeline_bacterial.nf:55`, 38 module files

Both workflows set `params.lan = "pl"` and 38 modules branch on it to choose the
language of messages written into the JSON report. Neither wrapper exposes a
flag for it, so the value can only be changed by editing the workflow file or by
overriding it in `nextflow.config`. The bilingual message infrastructure is
therefore effectively Polish-only for anyone using the documented entry points.

**Fix:** add `--lan` to both wrappers, validated against the values the modules
actually branch on, and forward it like any other parameter.

---

## 5. The Enterobase API token is written to the work directory in plaintext

**Files:** `modules/bacterial/pHierCC_assignment_external.nf:26`

The token correctly stays out of the wrapper's command line — `nf_pipeline_bacterial.nf:9`
documents that it must come from `nextflow.config`, and the wrapper does not pass
it. However, it is interpolated directly into the process script:

```
--api-token "${params.enterobase_api_token}" \
```

Nextflow writes the interpolated script to `work/<hash>/.command.sh`, so the
token ends up on disk in cleartext for every run, and survives as long as the
work directory does. It also appears in `.command.run` staging output.

**Fix:** pass the token through the environment (`env` directive, or `secret`
support in newer Nextflow) so the value is not baked into the task script, and
have `download_*`-style clients read it from the environment as the credentials
file mechanism already does elsewhere.

---

## 6. "Provisional Legionella support" is unreachable from any entry point

**Files:** `run_pipeline.sh:10`, `run_nf_pipeline_bacterial.sh:45`, `modules/bacterial/mlst_7genes_initial.nf`, `modules/bacterial/AMR_analysis_bacteria.nf`

`CHANGELOG.md` for 1.8.0 advertises provisional Legionella support, and two
bacterial modules do handle the genus. But `run_pipeline.sh` validates
`--organism` against `Salmonella Escherichia Campylobacter` and rejects anything
else, and the bacterial wrapper's `--genus` help text lists the same three. The
Legionella path can only be reached when kmerfinder auto-detects the genus and
`--genus` was left unset.

**Fix:** either add `Legionella` to `BACTERIAL_ORGANISMS` in `run_pipeline.sh`
and to the bacterial wrapper's accepted values, or reword the changelog entry to
say the support is auto-detection-only.

---

## 7. Seven orphaned modules remain on disk

**Files:**

```
modules/common/_cutadapt.nf
modules/common/_fastqc_nanopore.nf
modules/rsv/_filtering.nf
modules/rsv/_snpEff.nf
modules/sarscov2/_detect_type.nf
modules/sarscov2/_modeller.nf
modules/sarscov2/_simpleStats.nf
```

Of 85 module files, 78 are `include`d by a workflow. These seven are referenced
nowhere. The leading underscore appears to be the retirement convention, but it
is not documented, and their presence caused the existing documentation to
describe `_modeller.nf` and `_simpleStats.nf` as active steps and to promise
output files (`primers_poor*`, `<sampleId>_spike.pdb`) that no run produces.

**Fix:** delete them, or state the underscore convention in `CONTRIBUTING`/README
so the next reader is not misled. `modules/common/_fastqc_nanopore.nf:8` also has
`memory "4 Gb"` where every other module uses `GB` — worth normalising if the
module is ever revived.

---

## 8. `publishDir` for most viral results is commented out

Eighteen `publishDir` directives across sixteen viral modules are commented out,
covering nearly every intermediate and classification result:

| Module | Line | Would publish |
| --- | --- | --- |
| `modules/common/indelQual.nf` | 6 | `forvariants.bam*` — the alignment BAM + index |
| `modules/common/vcf_from_fasta.nf` | 6 | `consensus_vcf/*vcf*` |
| `modules/common/freeBayes.nf` | 6 | `freebayes/` — whole directory |
| `modules/common/lofreq.nf` | 4 | `lofreq/` — whole directory |
| `modules/common/varscan.nf` | 6 | `varscan/` — whole directory (`symlink` mode) |
| `modules/common/snpEff.nf` | 6 | `${sampleId}_detected_variants_consensus_annotated.txt` |
| `modules/common/pangolin.nf` | 6 | `pangolin_lineage.csv` |
| `modules/common/nextclade_noninfluenza.nf` | 6 | `nextstrain_lineage.csv` |
| `modules/infl/nextclade.nf` | 3 | `nextstrain_lineage.csv` |
| `modules/sarscov2/freyja.nf` | 6 | `coinfections.tsv` |
| `modules/common/nextalign.nf` | 6 | `*.fasta` |
| `modules/common/kraken2.nf` | 13, 14, 121, 122 | `Summary_kraken*`, `json_output/contaminations.json` |
| `modules/common/fastqc.nf` | 8, 66 | `json_output/*.json` |
| `modules/common/_fastqc_nanopore.nf` | 12 | `json_output/*.json` (orphaned module, see item 7) |

Those results therefore exist only inside `work/` and as fields in
`<sampleId>.json`, and are lost whenever the work directory is cleaned. Note in
particular that **no BAM and no VCF is ever published**, which is what the
existing `README.md` output tree claims.

This may well be deliberate — the JSON is the product, and publishing BAMs is
expensive. Flagging it as a decision to confirm rather than a defect: if it is
intentional, the commented-out blocks should be deleted so they stop reading like
a temporary measure.

---

## 9. Two `.gitignore` patterns never matched — FIXED

**File:** `.gitignore`

`bin/eqa_utils/ # temporary directory for eqa utils` carried a trailing comment.
Gitignore only honours `#` at the *start* of a line, so the pattern was the
literal string including the comment and matched nothing, leaving 94 MB
untracked and one `git add .` away from entering history. `bin/bacteria/__pycache__/`
was not listed at all — only the `bin/update/` pycache directories were.

Fixed in the same commit as the documentation conversion: the comment moved to
its own line and the missing pycache entry added. Verified with
`git check-ignore -v`. Recorded here because the failure mode is worth
remembering: it is the same class of defect as the `#`-in-Nextflow-config bug
below, and a linter for neither file would have caught it.

---

## Documentation-only, but breaks on copy-paste

Not code defects, listed here so they are not lost when the docs are rewritten.

- ~~The sample `nextflow.config` in §2.1.5 uses `#` for comments.~~ **Fixed in
  `doc/dokumentacja.md`**; confirmed by running `nextflow config`, which failed
  with `Unexpected character: '#' @ line 1, column 1` before the fix and parses
  cleanly after. Still wrong in `doc/dokumentacja.docx`, which is not edited.
- ~~§2.2 instructs the reader to create `bin/update/enterobase_api.txt`.~~
  **Fixed in `doc/dokumentacja.md`**; replaced with the `--credentials_file`
  flow. Still wrong in the `.docx`.
- **There is no `LICENSE` file.** `README.md` states MIT; the dangling reference
  to a `LICENSE` file has been removed, but the file itself should be added.
- `doc/dokumentacja.docx` and `.pdf` are now out of sync with
  `doc/dokumentacja.md`, which is the working copy. Decide whether to regenerate
  them at release time or retire them in favour of the markdown.

---

## Remaining documentation work (not defects)

Tracked here so the audit's findings are not lost:

- Chapter 4 (pipeline overview) and chapter 9 (primers appendix) are in Polish
  in an otherwise English document. Chapter 9's BED-format content is still
  technically accurate, so it needs translation rather than correction.
- Chapter 6.2 (bacterial parameters) has not been checked against
  `run_nf_pipeline_bacterial.sh`.
- Chapter 7 documents 43 modules out of 85 on disk. The whole
  `modules/bacterial/` tree (30 files) is undocumented, and the chapter still
  describes bacterial processes as inline in `nf_pipeline_bacterial.nf`, which
  1.8.0 refactored into separate modules.
- Features shipped in 1.7.x–1.8.0 and absent from the document: cuteSV
  structural-variant calling, RSV F-protein resistance analysis, pHierCC/HierCC
  assignment, the EQA2026 primer schemes and new RSV references, and the
  `run_pipeline.sh` dispatcher.
