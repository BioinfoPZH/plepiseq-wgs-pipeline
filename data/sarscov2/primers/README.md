In this directory, we place our own versions of primer schemes. The instructions are described in 
the documentation, point B.3. (check the new documentation!)

The schemes downloaded from the internet have been cleaned by me. That is, the primer name always
starts with "nCoV-2019_". Occasionally, a missing column 6 has been added. The file with the scheme
in the given subdirectory is always named `nCoV-2019.scheme.bed`. Missing `pairs.tsv` files
necessary for `ivar` have been added.

### Directories:

#### ARTIC

`Artic_V1` to `Artic_V5.4.2` - schemes downloaded from the repository
[https://github.com/artic-network/primer-schemes/tree/master](https://github.com/artic-network/primer-schemes/tree/master)
standardly used in the artic protocol.

#### Midnight

`Midnight_1200nt` - the Midnight 1200bp amplicon scheme.

#### EQA

Directories:
- EQA2023.SARS1
- EQA2023.SARS2
- EQA2024.V4_1 (practically identical, the only difference is the range of primer 64_LEFT)
- EQA2024.V5_3 (identical to the directory Artic_V5.3.2 because Artic_V5.3.2 is its copy)

contain primers used in EQA tests according to the names they had there.

# VarSkip

The VarSkip primer sets are alternative amplicon schemes developed by New England Biolabs (NEB) for SARS-CoV-2 
whole genome sequencing. They offer an alternative to the ARTIC primers and are optimized for different use cases.

## Source Repository

Two "versions" of primers can be found in the [NEB VarSkip repository](https://github.com/nebiolabs/VarSkip/tree/main):

1. **Root directory version** - original BED files with primer coordinates
2. **schemes/NEB_VarSkip subdirectory** - reformatted for ARTIC pipeline compatibility (no "alt" suffix in primer names, standardized reference name)

Primer coordinates for a given VarSkip version are identical between both formats (except VarSkip_V1a_long).

## Source Files

- **VarSkip_V2** (also known as VarSkip2a): https://github.com/nebiolabs/VarSkip/blob/main/neb_vss2a.primer.bed
- **VarSkip_V1a_long**: https://github.com/nebiolabs/VarSkip/blob/main/schemes/NEB_VarSkip/V1a-long/NEB_VarSkip.scheme.bed 
  (NOTE: this scheme differs from https://github.com/nebiolabs/VarSkip/blob/main/neb_vsl1a.primer.bed - e.g., no "MISPRIME" entries)
- **VarSkip_V1a**: https://github.com/nebiolabs/VarSkip/blob/main/neb_vss1a.primer.bed
- **VarSkip_V2b**: https://github.com/nebiolabs/VarSkip/blob/main/neb_vss2b.primer.bed

## Conversion Steps

All primers in this directory have been adjusted to work with our pipeline. For details on the standard conversion 
process (extending amplicons by 1bp, standardizing primer names, reference names, pool names, etc.), see the 
[Pipeline Customization documentation](../../../doc/topics/Pipeline-customization.md) - specifically the "Primers" section 
which describes BED file format requirements and naming conventions.


## Other Primer Sets

**obserco_extra** - directory with primers used in PZH as part of the obserco grant. It is based on 
Midnight_1200nt with additional primers, but most of these additions have different sequences yet map
to the same genomic regions as the original primers.

## Important Notes

- ALL primers in this directory (except EQA2024.V4_1.nanopore) are "artificially" extended so that the
  amplicon is 1bp longer in the 5' and 3' directions. This allows `ivar` to properly mask reads that
  map just 1bp beyond their amplicon. This additional nucleotide should not biologically occur, but 
  is attributed to an Illumina sequencing artifact.

- Nanopore primers for SARS have a `.nanopore` extension in the directory name. Each directory should 
  contain a single `.bed` file and a `pairs.tsv` file. The pairs file is not required by the pipeline 
  (it can be empty), but is needed by Nextflow modules that are shared between Nanopore and Illumina workflows.
