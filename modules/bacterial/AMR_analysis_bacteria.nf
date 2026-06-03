process run_amrfinder {
  container  = params.main_image
  tag "Predicting microbial resistance with AMRfinder for sample $x"
  containerOptions "--volume ${params.db_absolute_path_on_host}/amrfinder_plus:/AMRfider"
  cpus 1
  memory "5 GB"
  time "5m"
  input:
  tuple val(x), path(fasta), val(QC_status), val(SPECIES), val(GENUS), val(QC_status_contaminations)
  output:
  tuple val(x), path('AMRfinder_resistance.txt'), path('AMRfinder_virulence.txt'), emit: to_pubdir
  tuple val(x), path('amrfinder.json'), emit: json
  // -n input, plik z sekwencja nukleotydowa
  // -d input, sciezka do bazy AMRfindera 
  // -i input, seq identity miedzy targetem a query
  // -c input, query coverage z blasta
  // -O input, nazwa organizmu 
  // -o outpu, nazwa pliku z outputem
  // --plus input, Add the plus genes to the report
  // The 'plus' subset include a less-selective set of genes of interest including genes involved in virulence, biocide, heat, metal, and acid resistance
  // --blast_bin input sciezka do binarek blast-a, podaje wxplicite po w kontenerze sa 2 binarki blasta te z etoki i instalowane recznie
  // Te z etoki sa za stare 
  // when:
  // GENUS == 'Salmonella' || GENUS == 'Escherichia' || GENUS == 'Campylobacter'
  script:
  """
  if [[ ${QC_status} == "nie"  || ${QC_status_contaminations} == "nie" ]]; then
    touch AMRfinder_resistance.txt
    touch AMRfinder_virulence.txt
    # json z wynikami
    if [ "${params.lan}" == "pl" ]; then
      ERR_MSG="Ten moduł został uruchomiony na próbce, która nie przeszła kontroli jakości."
    else
      ERR_MSG="This module was eneterd with failed QC and poduced no valid output"
    fi

    python /opt/docker/EToKi/externals/amrfinder_parser.py -i AMRfinder_resistance.txt -s "nie" -r "\${ERR_MSG}" -o amrfinder.json
    # Komentarz NIE uzywac exit 0 wewnatrz script
  else
    if [[ ${GENUS} == "Salmonella" || ${GENUS} == "Escherichia" || ${GENUS} == "Campylobacter" ]]; then
      amrfinder --blast_bin /blast/bin -n $fasta -d /AMRfider  -i 0.9 -c 0.5 -o initial_output.txt -O ${GENUS} --plus 
 
      cat initial_output.txt  | awk 'BEGIN{FS="\\t"}; {if(\$9 == "AMR" || \$1 == "Protein identifier") print \$0}' > AMRfinder_resistance.txt
      cat initial_output.txt  | awk 'BEGIN{FS="\\t"}; {if(\$9 == "VIRULENCE" || \$1 == "Protein identifier") print \$0}' > AMRfinder_virulence.txt
      python /opt/docker/EToKi/externals/amrfinder_parser.py -i AMRfinder_resistance.txt -s "tak" -o amrfinder.json
    elif [ ${GENUS} == "Legionella" ]; then
       # AMRFINDER PLUS does not support Legionella, but we can run it without -O option and analyze presence of genes responsible for resistance
       # Mutation list will be always empty
       amrfinder --blast_bin /blast/bin -n $fasta -d /AMRfider  -i 0.9 -c 0.5 -o initial_output.txt --plus 
       cat initial_output.txt  | awk 'BEGIN{FS="\\t"}; {if(\$9 == "AMR" || \$1 == "Protein identifier") print \$0}' > AMRfinder_resistance.txt
       cat initial_output.txt  | awk 'BEGIN{FS="\\t"}; {if(\$9 == "VIRULENCE" || \$1 == "Protein identifier") print \$0}' > AMRfinder_virulence.txt
       python /opt/docker/EToKi/externals/amrfinder_parser.py -i AMRfinder_resistance.txt -s "tak" -o amrfinder.json
    else
      touch AMRfinder_resistance.txt  
      touch AMRfinder_virulence.txt
      if [ "${params.lan}" == "pl" ]; then
        ERR_MSG=`echo Ten modul jest przeznaczony do analizy bakterii z rodzajów: Salmonella, Escherichia oraz Campylobacter. W tej próbce wykryto: ${GENUS}`
      else
        ERR_MSG=`echo This module works with the following genera: Salmonella, Escherichia, or Campylobacter. Following genus was identified in this sample is: ${GENUS}`
      fi

      python /opt/docker/EToKi/externals/amrfinder_parser.py -i AMRfinder_resistance.txt -s "nie" -r "\${ERR_MSG}" -o amrfinder.json
      # json z wynikami
    fi
  fi
  """ 
}


process run_resfinder {
  container  = params.main_image
  containerOptions "--volume ${params.db_absolute_path_on_host}:/db"
  cpus 1
  maxForks 5
  memory "10 GB"
  time "5m"
  // Too many simultaneous processes result in a "Broken pipe" error
  tag "Predicting microbial resistance for sample $x"
  input:
  tuple val(x), path(fasta), val(QC_status), val(SPECIES), val(GENUS), val(QC_status_contaminations)
  output:
  tuple val(x), path('resfinder/pheno_table*.txt'), path('resfinder/ResFinder_results_table.txt'), path('resfinder/PointFinder_results.txt'), emit: to_pubdir
  tuple val(x), path('resfinder.json'), emit: json
  // when:
  // GENUS == 'Salmonella' || GENUS == 'Escherichia' || GENUS == 'Campylobacter'
  script:
  // resfinder rozumie 4 organizmy
  // campylobacter jejuni
  // campylobacter coli
  // escherichia coli
  // salmonella enterica
  // inne opcje to
  // -l 0.6 Minimum (breadth-of) coverage of ResFinder within the range 0-1.
  // -r 0.8 Threshold for identity of ResFinder within the range 0-1
  // oba parametry decyduja jaki procent alignmentu i z jaka identycznoscia musi miec nasz sample w stosunku 
  // do odczytow z bazy aby uznac ze dany gen wystepuje w probce 
  // --acquired Run resfinder for acquired resistance genes, czyli szukaj znanych genow wywolujacych opornosci
  // --point Run pointfinder for chromosomal mutations, szukamy w konkretnych genach mutacji punktowych
  // odpowiedzialnych za nabycie konkretnych opornosci
  // pozostale opcje to sciezki do baz /instalowanych wraz z tworzeniem obrazu/, zastanowic sie nad "wypchnieciem ich na zewnatrz" 
  """
  
  # resfinder-owi mozna tez podac pliki fastq (-ifq)
  # resfinder operated on species-level, but for now we asume here that all Salmonella are s.enterica, or all Campylobaster are c.jejuni
  # even if different species is actually analyzed. 
  
  if [[ ${QC_status} == "nie"  || ${QC_status_contaminations} == "nie" ]]; then
    # Tworzenie json i output 
    mkdir resfinder
    cd resfinder
    touch pheno_table_1.txt
    touch ResFinder_results_tab.txt 
    touch ResFinder_results_table.txt
    touch PointFinder_results.txt 

    if [ "${params.lan}" == "pl" ]; then 
      ERR_MSG="Ten moduł został uruchomiony na próbce, która nie przeszła kontroli jakości." 
    else
      ERR_MSG="This module was eneterd with failed QC and poduced no valid output"
    fi 

    python /opt/docker/EToKi/externals/resfinder_parser.py  -i  ResFinder_results_tab.txt -j PointFinder_results.txt -s "nie" -r "\${ERR_MSG}" -o resfinder.json
    cp resfinder.json ../
  else

    if [[ "${GENUS}" == *"Salmo"* ]]; then
        resfinder_species="senterica"
    elif [[ "${GENUS}" == *"Escher"* ]]; then
        resfinder_species="ecoli"
    elif [ ${GENUS} == "Campylobacter" ]; then
        resfinder_species="cjejuni"
    else
        resfinder_species=""
    fi

    if [ -z \${resfinder_species} ]; then
      # Tworzenie json i dummy output
      mkdir resfinder
      cd resfinder
      touch pheno_table_1.txt
      touch ResFinder_results_tab.txt
      touch ResFinder_results_table.txt
      touch PointFinder_results.txt

      if [ "${params.lan}" == "pl" ]; then
        ERR_MSG=`echo Ten program jest przeznaczony do analizy bakterii z rodzajów: Salmonella, Escherichia oraz Campylobacter. W tej próbce wykryto: ${GENUS}`
      else
        ERR_MSG=`echo This program works with the following genera: Salmonella, Escherichia, or Campylobacter. Following genus was identified in this sample is: ${GENUS}`
      fi

      python /opt/docker/EToKi/externals/resfinder_parser.py  -i  ResFinder_results_tab.txt -j PointFinder_results.txt -s "nie" -r "\${ERR_MSG}" -o resfinder.json
      cp resfinder.json ../
    else 
      python -m resfinder -o resfinder/ -s \${resfinder_species}  -l 0.6 -t 0.8 --acquired --point -k /opt/docker/kma/kma -db_disinf /db/disinfinder_db -db_res /db/resfinder_db/ -db_point /db/pointfinder_db/ -ifa ${fasta}
      python /opt/docker/EToKi/externals/resfinder_parser.py  -i  resfinder/ResFinder_results_tab.txt -j resfinder/PointFinder_results.txt -s "tak" -o resfinder.json
    fi # koniec if-a na obslugiwany gatunek

  fi # koniec if-a na status QC przekazany modulowi
 
  """
}