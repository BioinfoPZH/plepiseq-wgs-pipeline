// Variables for processes
ExecutionDir = new File('.').absolutePath

// // Directory with main repository containing "modules" directory
params.projectDir = ""
modules = "${params.projectDir}/modules" // Modules are part of the project_dir


// ALL parameters are setup usomg bash wrapper except enterobase_api_token that MUST be part of nextflow config
// Comments were preserved in the  nf file for a local executor
params.genus = ""
params.reads = ""
params.machine = ""
params.main_image = "" 
params.prokka_image = "" 
params.alphafold_image = ""  
params.medaka_image = ""
params.enterobase_api_token = "" 
params.threads = ""
params.quality = ""   
params.min_number_of_reads = "" 
params.min_median_quality = ""
params.main_genus_value = ""
params.kmerfinder_coverage = "" 
params.main_species_coverage = ""  
params.min_genome_length = "" 
params.unique_loci = "" 
params.contig_number = "" 
params.L50 = "" 
params.final_coverage = ""  
params.model_medaka = ""
params.min_coverage_ratio = "" 
params.min_coverage_value = ""
params.db_absolute_path_on_host= ""
params.results_dir = ""

// Print information that this pipeline works only for for 3 pre-defined genra

if ( params.genus  == 'Salmonella' || params.genus  == 'Escherichia' || params.genus == 'Campylobacter') {
    println("The program auto-detects genera, thus if Salmonella, Escherichia, and Campylobacter is not detected")
    println("in provided sample(s), sub-programs will not execute, regardles of the genus that was provided by a user")
} else {
    println("This program is intended to work with following genera: Salmonella, Escherichia, and Campylobacter")
    println("It will continue but unless one of this genera is identified, sub-programs will not execute")
}

// User must use our config that has two profiles slurm and local, nextflow must be initialized with one of them

if ( !workflow.profile || ( workflow.profile != "slurm" && workflow.profile != "local") ) {
   println("Nextflow run must be executed with -profile option. The specified profile must be either 'local' or 'slurm'.")
   System.exit(1)
}

// Select language used in error msg allowed values 'en' or 'pl'
params.lan = "pl"

// Turn off alphafold
params.run_alphafold = true

// Import modules
include { run_medaka } from "${modules}/bacterial/medaka_bacteria.nf"
include { run_fastqc_illumina } from "${modules}/bacterial/fastqc_bacteria.nf"
include { run_fastqc_nanopore } from "${modules}/bacterial/medaka_bacteria.nf"

// genome assembly
include { run_flye } from "${modules}/bacterial/flye_bacteria.nf"
include { spades } from "${modules}/bacterial/spades.nf"

include { bwa_paired } from "${modules}/bacterial/bwa_related_bacterial.nf"
include { bwa_single } from "${modules}/bacterial/bwa_related_bacterial.nf"
include { merge_bams } from "${modules}/bacterial/bwa_related_bacterial.nf"

include { clean_fastq_illumina } from "${modules}/bacterial/etoki_related.nf"
include { clean_fastq_nanopore } from "${modules}/bacterial/etoki_related.nf"

include { run_Seqsero } from "${modules}/bacterial/seqsero_bacteria.nf"

include { run_amrfinder } from "${modules}/bacterial/AMR_analysis_bacteria.nf"
include { run_resfinder } from "${modules}/bacterial/AMR_analysis_bacteria.nf"
include { run_pilon } from "${modules}/bacterial/pilon_bacteria.nf"
include { extract_final_contigs } from "${modules}/bacterial/genome_analysis_bacteria.nf"
include { extract_final_stats } from "${modules}/bacterial/genome_analysis_bacteria.nf"

include { run_sistr } from "${modules}/bacterial/sistr_bacteria.nf"
include { run_ectyper } from "${modules}/bacterial/ectyper_bacteria.nf"

include { run_vfdb } from "${modules}/bacterial/vfdb_analysis_bacteria.nf"
include { parse_vfdb } from "${modules}/bacterial/vfdb_analysis_bacteria.nf"

include { run_prokka } from "${modules}/bacterial/prokka_bacteria.nf"
include { run_spifinder } from "${modules}/bacterial/spifinder_bacteria.nf"

// contamination analysis
include { run_metaphlan_illumina } from "${modules}/bacterial/contamination_analysis_bacteria.nf"
include { run_speciesfinder_illumina } from "${modules}/bacterial/contamination_analysis_bacteria.nf"
include { run_speciesfinder_nanopore } from "${modules}/bacterial/contamination_analysis_bacteria.nf"
include { run_kraken2_illumina } from "${modules}/bacterial/contamination_analysis_bacteria.nf"
include { run_kraken2_nanopore } from "${modules}/bacterial/contamination_analysis_bacteria.nf"

// species identification based on contamination data
include { get_species_illumina } from "${modules}/bacterial/get_species_bacteria.nf"
include { get_species_nanopore } from "${modules}/bacterial/get_species_bacteria.nf"

// alphafold processes
include { run_alphafold } from "${modules}/bacterial/alphafold_bacteria.nf"
include { run_alphafold_slurm } from "${modules}/bacterial/alphafold_bacteria.nf"
include { run_alphafold_dummy } from "${modules}/bacterial/alphafold_bacteria.nf"

include { run_plasmidfinder } from "${modules}/bacterial/plasmidfinder_bacteria.nf"

include { run_virulencefinder } from "${modules}/bacterial/virulencefinder_bacteria.nf"

// json aggregators
include { merge_all_subjsons_illumina } from "${modules}/bacterial/json_aggregators_bacteria.nf"
include { merge_all_subjsons_nanopore } from "${modules}/bacterial/json_aggregators_bacteria.nf"

include { run_split_fasta } from "${modules}/bacterial/split_fasta_bacteria.nf"

// minimap2
include { run_minimap2 } from "${modules}/bacterial/minimap_bacteria.nf"
include { run_minimap2_2nd } from "${modules}/bacterial/minimap_bacteria.nf"


// initial 7-gene mlst with mlsdtb on fastq files
include { run_initial_mlst_illumina } from "${modules}/bacterial/mlst_7genes_initial.nf"
include { run_initial_mlst_nanopore } from "${modules}/bacterial/mlst_7genes_initial.nf"

// mlst (7-gene and core genome) analysis on genome
include { run_7MLST } from "${modules}/bacterial/run_mlst_with_blast.nf"
include { run_cgMLST } from "${modules}/bacterial/run_mlst_with_blast.nf"

// mlst ( 7 gene and core genome) results parsers
include { parse_7MLST } from "${modules}/bacterial/mlst_parsers_bacteria.nf"
include { parse_cgMLST } from "${modules}/bacterial/mlst_parsers_bacteria.nf"

// phierCC assignment of cgMLST sequence type with external resources ( enterobase and pubmlst)
include { run_pHierCC_enterobase } from "${modules}/bacterial/pHierCC_assignment_external.nf"
include { run_pHierCC_pubmlst } from "${modules}/bacterial/pHierCC_assignment_external.nf"

// pHierCC assignment of cgMLST sequence type with local database
include { run_pHierCC_local } from "${modules}/bacterial/pHierCC_assignment_local_data.nf"


// analysis of location and infection date for strains with give cgMLST sequence type with enterobase database
include { extract_historical_data_enterobase } from "${modules}/bacterial/historical_records_analysis_enterobase.nf"
include { plot_historical_data_enterobase } from "${modules}/bacterial/historical_records_analysis_enterobase.nf"

// analysis of location and infection date for strains with give cgMLST sequence type with pubmlst database
include { extract_historical_data_pubmlst } from "${modules}/bacterial/historical_records_analysis_pubmlst.nf"
include { plot_historical_data_pubmlst } from "${modules}/bacterial/historical_records_analysis_pubmlst.nf"

// merge jsons produced during mlst analysis (7-gene and core genome)
include { run_cgMLST_final_json } from "${modules}/bacterial/cgmlst_json_aggregator.nf"


// SUB WORKFLOWS NANOPORE //

workflow predict_species_nanopore {
// Workflow puszcza 2 programy (kraken2,  kmerfinder)
// Metaphlan puszcza bowtie2 ktoru jest dla krtokich odczytow
take:
initial_fastq
inital_qc_status
main:
initial_fastq_and_status = initial_fastq.join(inital_qc_status, by : 0, remainder : true)
// kraken
kraken2_out = run_kraken2_nanopore(initial_fastq_and_status)

// Kmerfinder
kmerfinder_out = run_speciesfinder_nanopore(initial_fastq_and_status)

programs_out = kraken2_out.join(kmerfinder_out,  by : 0, remainder : true)
final_species = get_species_nanopore(programs_out)
emit:
final_species.species_and_qcstatus
final_species.genus_only
final_species.json
}

workflow polishing_with_medaka {
// pilona puscimy tylko raz bo zakladam ze flye po cos te rundy filtrowania robi 
// zamieniono pilona na medaka
take:
initial_scaffold_inner
processed_fastq_inner_SE
main:
// laczymy kanaly ze scaffoldem genomu i odczytmai
for_remaping_SE_inner = initial_scaffold_inner.join(processed_fastq_inner_SE, by : 0, remainder : true)
// mapujemy odczyty na genom
single_bams_and_genome = run_minimap2(for_remaping_SE_inner)

// laczymy bam-a z mapowania z genomem

// Zamieniamy pilona na medaka
// final_assembly = run_pilon_nanopore(single_bams_and_genome)

final_assembly = run_medaka(single_bams_and_genome)

// liczymy pokrycia i fitrujemy slabe contig

for_remaping_polished_assembly = final_assembly.join(processed_fastq_inner_SE, by : 0, remainder : true)
single_bams_and_polished_genome = run_minimap2_2nd(for_remaping_polished_assembly)

final_assembly_filtered = extract_final_contigs(single_bams_and_polished_genome)

emit:
extract_final_contigs.out[0] // fasta with contigs that passed coverage filter
extract_final_contigs.out[1] // fasta with contigs that passed coverage filter and second fasta with refejted contigs

}

// SUB WORKFLOWS ILLUMINA //

workflow pilon_first {
// First round of polishing initial scaffold with pilon
take:
initial_scaffold_inner
processed_fastq_inner_PE
processed_fastq_inner_SE

// initial_scaffold_inner jest kanalem zawierajacym
// // ientyfikator probki
// // plik fasta z genomem

// processed_fastq_inner_PE i SE kazdy jest kanalem zawierajacym
// // PE -> identyfikator probki i odczyty PE
// // SE -> identyfikator probki i odczyty SE
main:
// przygotowanie do mapowan
for_remaping_PE_inner = initial_scaffold_inner.join(processed_fastq_inner_PE, by : 0, remainder : true)
for_remaping_SE_inner = initial_scaffold_inner.join(processed_fastq_inner_SE, by : 0, remainder : true)

// mapowania
single_bams_inner = bwa_single(for_remaping_SE_inner)
paired_bams_inner = bwa_paired(for_remaping_PE_inner)

// laczenie wynikow mapowan
merged_bams_inner = paired_bams_inner.join(single_bams_inner, by : 0, remainder : true)
merged_bams_and_scaffold_inner = merged_bams_inner.join(initial_scaffold_inner,  by : 0, remainder : true)
run_pilon(merged_bams_and_scaffold_inner)

emit:
// sub pipeline zwraca identyfikator probki + nowy scaffold
// bamy sa zbedne bo w kolejenj iteracji musza byc remapowane na poprawiony genom 
run_pilon.out.ONLY_GENOME
}


workflow pilon_second {
// Polishing scaffold obtained after first pilon run
take:
initial_scaffold_inner
processed_fastq_inner_PE
processed_fastq_inner_SE
main:
for_remaping_PE_inner = initial_scaffold_inner.join(processed_fastq_inner_PE, by : 0, remainder : true)
for_remaping_SE_inner = initial_scaffold_inner.join(processed_fastq_inner_SE, by : 0, remainder : true)

single_bams_inner = bwa_single(for_remaping_SE_inner)
paired_bams_inner = bwa_paired(for_remaping_PE_inner)

merged_bams_inner = paired_bams_inner.join(single_bams_inner, by : 0, remainder : true)
merged_bams_and_scaffold_inner = merged_bams_inner.join(initial_scaffold_inner,  by : 0, remainder : true)
run_pilon(merged_bams_and_scaffold_inner)

emit:
run_pilon.out.ONLY_GENOME
}

workflow pilon_third {
// Polishing scaffold obtained after second pilon run
take:
initial_scaffold_inner
processed_fastq_inner_PE
processed_fastq_inner_SE
main:
for_remaping_PE_inner = initial_scaffold_inner.join(processed_fastq_inner_PE, by : 0, remainder : true)
for_remaping_SE_inner = initial_scaffold_inner.join(processed_fastq_inner_SE, by : 0, remainder : true)

single_bams_inner = bwa_single(for_remaping_SE_inner)
paired_bams_inner = bwa_paired(for_remaping_PE_inner)

merged_bams_inner = paired_bams_inner.join(single_bams_inner, by : 0, remainder : true)
merged_bams_and_scaffold_inner = merged_bams_inner.join(initial_scaffold_inner,  by : 0, remainder : true)
run_pilon(merged_bams_and_scaffold_inner)

emit:
run_pilon.out.ONLY_GENOME
}


workflow calculate_coverage {
// This workflow takes scafold polished with 3 rounds of pilon
// And save only contigs that coverage is above params.min_coverage_ratio of average coverage
take:
initial_scaffold_inner
processed_fastq_inner_PE
processed_fastq_inner_SE
main:

for_remaping_PE_inner = initial_scaffold_inner.join(processed_fastq_inner_PE, by : 0, remainder : true)
for_remaping_SE_inner = initial_scaffold_inner.join(processed_fastq_inner_SE, by : 0, remainder : true)

single_bams_inner = bwa_single(for_remaping_SE_inner)
paired_bams_inner = bwa_paired(for_remaping_PE_inner)

merged_bams_inner = paired_bams_inner.join(single_bams_inner, by : 0, remainder : true)

// W odroznieniu od workflow z piloenm laczymy pliki bam PE i SE w jeden plik
merged_bams_into_onefile_inner = merge_bams(merged_bams_inner)

merged_bams_into_onefile_and_scaffold_inner = merged_bams_into_onefile_inner.join(initial_scaffold_inner,  by : 0, remainder : true)
extract_final_contigs_out = extract_final_contigs(merged_bams_into_onefile_and_scaffold_inner)

emit:
extract_final_contigs.out[0] // fasta with contigs that passed coverage filter
extract_final_contigs.out[1] // fasta with contigs that passed coverage filter and second fasta with refejted contigs
}


workflow predict_species_illumina {
// Workflow puszcza 3 programy (kraken2, metaphlan i kmerfinder)
// jego celem jest zwrocenie informacji o gatunku (tak naprawde wazne tylko dla Campylo)
// tak bym nizej mogl okreslic poprawnie baze
take:
initial_fastq
inital_qc_status // Tu bedzie tylko jeden z emitow kanalu z FASTQC

main:
// join FASTQ with QC_status from FASTQC
initial_fastq_and_status = initial_fastq.join(inital_qc_status, by : 0, remainder : true)
// kraken
kraken2_out = run_kraken2_illumina(initial_fastq_and_status)

// Metaphlan
metaphlan_out = run_metaphlan_illumina(initial_fastq_and_status)

// Kmerfinder
kmerfinder_out = run_speciesfinder_illumina(initial_fastq_and_status)

merge1 = metaphlan_out.join(kmerfinder_out, by : 0, remainder : true)
programs_out = kraken2_out.join(merge1,  by : 0, remainder : true)
final_species = get_species_illumina(programs_out)
emit:
final_species.species_and_qcstatus
final_species.genus_only
final_species.json
}

// MAIN WORKFLOW //

workflow {

// Get data
if(params.machine == 'Illumina') {

Channel
  .fromFilePairs(params.reads)
  .set {initial_fastq}

// FASTQC
run_fastqc_illumina_out = run_fastqc_illumina(initial_fastq)

// Species prediction
(predict_species_out, predict_species_genus, predict_species_json) = predict_species_illumina(initial_fastq, run_fastqc_illumina_out.qcstatus_and_values)


//Inilat MLST

initial_mlst_out = run_initial_mlst_illumina(initial_fastq.join(predict_species_out, by : 0)) // initial_mlst_out  przekazuje zarowno fastq jak i qc_status
// FASTQ trimming
processed_fastq = clean_fastq_illumina(initial_mlst_out.reads_and_status)

// Initial scaffold
initial_scaffold = spades(processed_fastq.All_path)

// Polishing scaffold with pilon ( 3 times )
first_polish_run = pilon_first(initial_scaffold, processed_fastq.PE_path, processed_fastq.SE_path)
second_polish_run = pilon_second(first_polish_run, processed_fastq.PE_path, processed_fastq.SE_path)
third_polish_run = pilon_third(second_polish_run, processed_fastq.PE_path, processed_fastq.SE_path)

// Remove contigs with coverage less than 0.1 avarage coverage

(final_assembly, final_assembly_with_reject) = calculate_coverage(third_polish_run, processed_fastq.PE_path, processed_fastq.SE_path)

} else if (params.machine == 'Nanopore') {

// Get Data
Channel
  .fromPath(params.reads)
  .map {it -> tuple(it.getName().split("\\.")[0], it)}
  .set {initial_fastq}

// FASTQC
run_fastqc_nanopore_out = run_fastqc_nanopore(initial_fastq)

// Contaminations/subspecies prediction
(predict_species_out, predict_species_genus, predict_species_json) = predict_species_nanopore(initial_fastq, run_fastqc_nanopore_out.qcstatus_and_values)

//Initial MLST
initial_mlst_out = run_initial_mlst_nanopore(initial_fastq.join(predict_species_out, by : 0)) // initial_mlst_out  przekazuje zarowno fastq jak i qc_status

// FASTQ trimming

processed_fastq = clean_fastq_nanopore(initial_mlst_out.reads_and_status)

// initial scaffold with Flye (with 3 internalrounds of polishing)
initial_scaffold = run_flye(processed_fastq)

// one round of assembly polishing with medaka
(final_assembly, final_assembly_with_reject) = polishing_with_medaka(initial_scaffold, processed_fastq)

}

// All modules below are shared between nanopore and illumina

// Split multifasta into separate fastas and create json output 
run_split_fasta_out = run_split_fasta(final_assembly)

// Assembly quality

(extract_final_stats_statistics, extract_final_stats_genome, extract_final_stats_json) = extract_final_stats(final_assembly_with_reject.join(predict_species_genus,  by : 0))
 

// Species prediction with Achtman and core genome shemes
final_assembly_with_species = extract_final_stats_genome.join(predict_species_out, by : 0)
MLST_out = run_7MLST(final_assembly_with_species)
MLST_out_delayed = MLST_out.map {it -> sleep(20000); it}
parse_7MLST_out = parse_7MLST(MLST_out_delayed) // channel is delayed to avoid rare situation where multiple sample with unknown ST are assaign the same local ST number


cgMLST_out = run_cgMLST(final_assembly_with_species)
cgMLST_out_delayed = cgMLST_out.map {it -> sleep(60000); it}
(parse_cgMLST_out, parse_cgMLST_only_duplicates, parse_cgMLST_only_json) = parse_cgMLST(cgMLST_out_delayed) // channel is delayed to avoid rare situation where multiple sample with unknown cgST are assaign the same local cgST number

(run_pHierCC_local_pubdir, run_pHierCC_local_json) = run_pHierCC_local(parse_cgMLST_out)

parse_cgMLST_out_delayed = parse_cgMLST_out.map {it -> sleep(20000); it}
run_pHierCC_enterobase_out = run_pHierCC_enterobase(parse_cgMLST_out_delayed) // for Salmo and Escher input channel is delayed to avoid multiple simultaneous queries of Enterbase API

run_pHierCC_enterobase_out_pubmlst = run_pHierCC_pubmlst(parse_cgMLST_out) // only for jejuni

(extract_historical_data_enterobase_toplot, extract_historical_data_enterobase_json)  = extract_historical_data_enterobase(run_pHierCC_enterobase_out)
plot_historical_data_enterobase(extract_historical_data_enterobase_toplot)


(extract_historical_data_pubmlst_toplot, extract_historical_data_pubmlst_json) = extract_historical_data_pubmlst(run_pHierCC_enterobase_out_pubmlst)
plot_historical_data_pubmlst(extract_historical_data_pubmlst_toplot)

// // Combining some modules to produce final json output for cgMLST
 
cgMLST_path_to_json = parse_cgMLST_only_json.join(run_pHierCC_local_json.join(extract_historical_data_enterobase_json.join(extract_historical_data_pubmlst_json,  by : 0),  by : 0),  by : 0)
run_cgMLST_final_json_out = run_cgMLST_final_json(cgMLST_path_to_json)


// AMR predictions
run_resfinder_out = run_resfinder(final_assembly_with_species)
run_amrfinder_out = run_amrfinder(final_assembly_with_species)

// plasmids
run_plasmidfinder_out = run_plasmidfinder(final_assembly_with_species)

// Virulence
prokka_out = run_prokka(final_assembly_with_species)
run_VFDB_out = run_VFDB(prokka_out.prokka_all)
parse_VFDB_ecoli_out = parse_VFDB_ecoli(run_VFDB_out.ecoli)

run_virulencefinder_out = run_virulencefinder(final_assembly_with_species)
run_ectype_out = run_ectyper(final_assembly_with_species)

run_spifinder_out = run_spifinder(final_assembly_with_species)
run_seqsero_out = run_Seqsero(final_assembly_with_species)
run_sistr_out = run_sistr(final_assembly_with_species)

// Alphafold
if ( params.run_alphafold  ) {
  if ( workflow.profile == "slurm" ) {
    alphafold_out = run_alphafold_slurm(prokka_out.prokka_all)
  } else if ( workflow.profile == "local" ) {
      delayed_alphafold = prokka_out.prokka_all.map {it -> sleep(20000); it}
      alphafold_out = run_alphafold(delayed_alphafold)
  }
} else {
   alphafold_out = run_alphafold_dummy(prokka_out.prokka_all)
}

// Aggregate all modules that emit json 
// Split into two channels: channnel_to_json and initial_to_json is purely to split outputs of module that are 
// executed after genome is porposed from ones that work on fastq files
  
  channnel_to_json = run_sistr_out.json.join(run_seqsero_out.json, by : 0)
  channnel_to_json = channnel_to_json.join(run_spifinder_out.json, by : 0)
  channnel_to_json = channnel_to_json.join(run_ectype_out.json, by : 0)
  channnel_to_json = channnel_to_json.join(run_virulencefinder_out.json, by : 0)
  channnel_to_json = channnel_to_json.join(prokka_out.json, by : 0)
  channnel_to_json = channnel_to_json.join(alphafold_out.json, by : 0)
  channnel_to_json = channnel_to_json.join(run_VFDB_out.json, by : 0)
  channnel_to_json = channnel_to_json.join(parse_VFDB_ecoli_out.json, by : 0)
  channnel_to_json = channnel_to_json.join(run_plasmidfinder_out.json, by : 0)
  channnel_to_json = channnel_to_json.join(run_amrfinder_out.json, by : 0)
  channnel_to_json = channnel_to_json.join(run_resfinder_out.json, by : 0)
  channnel_to_json = channnel_to_json.join(run_cgMLST_final_json_out.json, by : 0)
  channnel_to_json = channnel_to_json.join(parse_7MLST_out.json, by : 0)
  


if(params.machine == 'Illumina') {
  initial_to_json = run_fastqc_illumina_out.json.join(predict_species_json, by : 0) // predict_species_json to explicite nazwany kanal wyjscia podczas wywolania subworkflow
  initial_to_json = initial_to_json.join(initial_mlst_out.json,  by : 0) 
  initial_to_json = initial_to_json.join(run_split_fasta_out.json, by : 0)
  initial_to_json = initial_to_json.join(extract_final_stats_json,  by : 0) // extract_final_stats_json to explicite nazwany kanal wyjscia podczas wywolania modulu
  

  channnel_to_json = channnel_to_json.join(initial_to_json, by : 0)
  merge_all_subjsons_illumina(channnel_to_json, ExecutionDir)
  // modul do zapisywania merge'u jsona  
} else if (params.machine == 'Nanopore') {
  // Nanopore rozni sie tylko outputem kanalu fastqc 
  initial_to_json = run_fastqc_nanopore_out.json.join(predict_species_json, by : 0) // predict_species_json to explicite nazwany kanal wyjscia podczas wywolania subworkflow
  initial_to_json = initial_to_json.join(initial_mlst_out.json,  by : 0)
  initial_to_json = initial_to_json.join(run_split_fasta_out.json, by : 0)
  initial_to_json = initial_to_json.join(extract_final_stats_json,  by : 0) // extract_final_stats_json to explicite nazwany kanal wyjscia podczas wywolania modulu
  channnel_to_json = channnel_to_json.join(initial_to_json, by : 0)

  merge_all_subjsons_nanopore(channnel_to_json, ExecutionDir)
}

}
