process run_prokka {
  // Propkka to soft do przewidywania genow w genomie (zwraca tez sekwencje bialek i annotace)
  // Ktory pod spodem korzysta z prodigal to przewiddywnia genow
    // Komentarz czemu uzywa prodigal 2.6.3 a nie 3.0 
    // Prodigal sluzy do predykcji genow w genomie
    // Uzywamy wersji 2.6.3 choc na ich wiki caly czas mowia o 3.0, ktorej nie moge znalezc
    // Moze ma to zwiazek z issues w ktorych facet pisze ze 3.0 to work in progress 
    // https://github.com/hyattpd/Prodigal/issues/45
  
  // instalacja prokka jest meczaca ale korzystamy z wystawionego przez autorow obrazu 

  // opcja --metagenome w prokka jest wywolaniem prodigal z opcja -p meta
  // opcja meta in which Prodigal applies pre-calculated training files to the provided input sequence and predicts genes based on the best results.
  //  i winnym miejscu
  // Isolated, short sequences (<100kbp) such as plasmids, phages, and viruses should generally be analyzed using meta mode
  // Testowe puszczenie z opcja meta i bez niej nawet na dosc stabilnym genomie jak sample 151 daje troche inne wyniki

  // opcja --compliant Force Genbank/ENA/DDJB compliance: --addgenes --mincontiglen 200 --centre XXX (default OFF)
  // opcja --kingdom Bacteria jest defaultem zaklada jaki typ genomu analizujemy
 
  // I assume there is no point checking here what is the organism
  publishDir "${params.results_dir}/${x}/", mode: 'copy', pattern: "${x}_prokka*"
  container  = params.prokka_image
  tag "Predicting genes for sample $x"
  cpus { params.threads > 25 ? 25 : params.threads }
  memory "10 GB"
  time "20m"
  input:
  tuple val(x), path(fasta), val(QC_status), val(SPECIES), val(GENUS), val(QC_status_contaminations)
  output:
  tuple val(x), path('*.gff'), path('prokka_out/*faa'), path('*.ffn'), path('prokka_out/*.tsv'), val(SPECIES), val(GENUS), val(QC_status), val(QC_status_contaminations), emit: prokka_all
  tuple val(x), path('prokka.json'), emit: json
  //when:
  //GENUS == 'Salmonella' || GENUS == 'Escherichia' || GENUS == 'Campylobacter'
  script:
  """
  if [[ ${QC_status} == "nie"  || ${QC_status_contaminations} == "nie" ]]; then
    mkdir prokka_out; touch prokka_out/prokka_out_dummy.gff; touch prokka_out/prokka_out_dummy.faa; touch prokka_out/prokka_out_dummy.ffn; touch prokka_out/prokka_out_dummy.tsv

    if [ "${params.lan}" == "pl" ]; then
      ERROR_MSG="Ten moduł został uruchomiony na próbce, która nie przeszła kontroli jakości."
    else
      ERROR_MSG="This module was eneterd with failed QC and poduced no valid output"
    fi

    echo -e "{\\"status\\": \\"nie\\", \
              \\"error_message\\": \\"\${ERROR_MSG}\\"}"  >> prokka.json
    # json z informacja o bledzie jakosci
    mv prokka_out/prokka_out_dummy.gff ${x}_prokka.gff
    mv prokka_out/prokka_out_dummy.ffn ${x}_prokka.ffn
  else
    if [[ ${GENUS} == "Salmonella" || ${GENUS} == "Escherichia" || ${GENUS} == "Campylobacter" ]]; then
      prokka --metagenome --cpus ${task.cpus} --outdir prokka_out --prefix prokka_out --compliant --kingdom Bacteria $fasta
      echo -e "{\\"status\\": \\"tak\\", \
            \\"prokka_gff\\": \\"${params.results_dir}/${x}/${x}_prokka.gff\\", \
            \\"prokka_ffn\\": \\"${params.results_dir}/${x}/${x}_prokka.ffn\\"}" >> prokka.json
      mv prokka_out/prokka_out.gff ${x}_prokka.gff
      mv prokka_out/prokka_out.ffn ${x}_prokka.ffn
    else
      mkdir prokka_out; touch prokka_out/prokka_out_dummy.gff; prokka_out/prokka_out_dummy.ffa; prokka_out/prokka_out_dummy.ffn; prokka_out/prokka_out_dummy.tsv
      # json z informacja o zlym gatunku
      
      if [ "${params.lan}" == "pl" ]; then
        ERROR_MSG=`echo Ten program jest przeznaczony do analizy bakterii z rodzajów: Salmonella, Escherichia oraz Campylobacter. W tej próbce wykryto: ${GENUS}`
      else
        ERROR_MSG=`echo This program works with the following genera: Salmonella, Escherichia, or Campylobacter. Following genus was identified in this sample is: ${GENUS}`
      fi
      
      echo -e "{\\"status\\": \\"nie\\", \
                \\"error_message\\": \\"\${ERROR_MSG}\\"}"  >> prokka.json

      mv prokka_out/prokka_out_dummy.gff ${x}_prokka.gff
      mv prokka_out/prokka_out_dummy.ffn ${x}_prokka.ffn
    fi
  fi

  """
}