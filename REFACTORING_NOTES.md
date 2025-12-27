# Refactoring Notes: Pipeline Scripts Unification

## Overview

The startup scripts `run_nf_pipeline_bacterial.sh` and `run_nf_pipeline_viral.sh` have been refactored to reduce code duplication and follow the DRY (Don't Repeat Yourself) principle.

## Changes Made

### New Unified Script: `run_nf_pipeline.sh`

A new unified script that combines the functionality of both bacterial and viral pipelines:
- Contains all shared logic and parameter handling
- Uses `--pipeline_type` parameter to determine which pipeline to run
- Maintains all original functionality and parameter validation
- Clearly organized with sections for common, bacterial-specific, and viral-specific parameters

### Backward Compatibility Wrappers

The original scripts (`run_nf_pipeline_bacterial.sh` and `run_nf_pipeline_viral.sh`) have been converted to lightweight wrappers that:
- Maintain backward compatibility with existing workflows
- Transform old parameter names to unified script format (e.g., `--main_image` → `--bacterial_main_image` or `--viral_main_image`)
- Automatically set the `--pipeline_type` parameter
- Redirect all calls to the unified script

## Usage

### Direct Usage of Unified Script

**Bacterial pipeline:**
```bash
./run_nf_pipeline.sh --pipeline_type bacterial --machine Illumina --reads 'path/to/reads/*_R{1,2}.fastq.gz' --projectDir . --external_databases_path /path/to/db
```

**Viral pipeline:**
```bash
./run_nf_pipeline.sh --pipeline_type viral --machine Illumina --reads 'path/to/reads/*_R{1,2}.fastq.gz' --primers_id V4 --species "SARS-CoV-2" --projectDir . --external_databases_path /path/to/db
```

### Backward Compatible Usage

**Bacterial pipeline (unchanged):**
```bash
./run_nf_pipeline_bacterial.sh --machine Illumina --reads 'path/to/reads/*_R{1,2}.fastq.gz' --projectDir . --external_databases_path /path/to/db
```

**Viral pipeline (unchanged):**
```bash
./run_nf_pipeline_viral.sh --machine Illumina --reads 'path/to/reads/*_R{1,2}.fastq.gz' --primers_id V4 --species "SARS-CoV-2" --projectDir . --external_databases_path /path/to/db
```

## Benefits

1. **Reduced Code Duplication**: Common logic is now in one place (~900 lines of duplicated code removed)
2. **Easier Maintenance**: Changes to common functionality only need to be made once
3. **Consistent Behavior**: Both pipelines now share identical validation and default value logic
4. **Backward Compatibility**: Existing scripts and workflows continue to work without modification
5. **Clear Structure**: The unified script has well-organized sections with clear comments

## Technical Details

- **Code Reduction**: Total code reduced from ~43 KB to ~31 KB
- **Wrapper Scripts**: Each wrapper is ~1.4-1.5 KB (down from ~19-24 KB)
- **Unified Script**: 27.8 KB containing all logic
- **Parameter Transformation**: Wrappers automatically map pipeline-specific image parameters

## Testing

All scripts have been tested for:
- Syntax correctness (`bash -n`)
- Help message display (`--help`, `--all`)
- Parameter validation
- Error handling for missing or invalid parameters
- Backward compatibility with original script interfaces
