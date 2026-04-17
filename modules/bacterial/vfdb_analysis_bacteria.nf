
process run_VFDB {
  // Baza z czynnikami wirulencji w roznych bakteriach w tym salmonelli
  // Przygotowana baza z instrukcja jak ja przygotowac jest w /mnt/sda1/michall/db/VFDB/README
  container  = params.main_image
  containerOptions "--volume ${params.db_absolute_path_on_host}/vfdb:/db"
  // Ponownie montujemy na sztyno do /db bo taka lokalizacje na sztywno ma wpisany moj skrypt
  tag "Predicting VirulenceFactors for sample $x"
  cpus { params.threads > 25 ? 25 : params.threads }
  memory "20 GB"
  time "40m"
  input:
  tuple val(x), path(gff), path(faa), path(ffn), path(tsv), val(SPECIES), val(GENUS), val(QC_status), val(QC_status_contaminations)
  output:
  tuple val(x), path('VFDB_summary*txt'), val(SPECIES), val(GENUS), emit: non_ecoli
  tuple val(x), path('VFDB_summary_Escherichia.txt'), path('VFDB_summary_Shigella.txt'), val(SPECIES), val(GENUS), val(QC_status), val(QC_status_contaminations), optional: true, emit: ecoli
  tuple val(x), path('vfdb.json'), emit: json
  //when:
  //GENUS == 'Salmonella' || GENUS == 'Escherichia' || GENUS == 'Campylobacter'
  script:
  """
  
  if [[ ${QC_status} == "nie"  || ${QC_status_contaminations} == "nie" ]]; then
    touch VFDB_summary_dummy.txt; touch VFDB_summary_Escherichia.txt  ; touch VFDB_summary_Shigella.txt
    # json na blad QC
    if [ "${params.lan}" == "pl" ]; then
      ERR_MSG="Ten moduł został uruchomiony na próbce, która nie przeszła kontroli jakości."
    else
      ERR_MSG="This module was eneterd with failed QC and poduced no valid output"
    fi
    
    python /opt/docker/EToKi/externals/vfdb_parser.py  -i VFDB_summary_Escherichia.txt -s "nie" -r "\${ERR_MSG}" -o vfdb.json
  else
    if [[ ${GENUS} == "Salmonella" || ${GENUS} == "Escherichia" || ${GENUS} == "Campylobacter" ]]; then
     touch VFDB_summary_dummy.txt; touch VFDB_summary_Escherichia.txt  ; touch VFDB_summary_Shigella.txt 
     SPEC2="unk"

      if [ "${GENUS}" == "Escherichia" ]; then
        # for Escherichia we must also check Shigella
        SPEC2="Shigella"  
      fi

      PIDENT=80 # minimalna identycznosc sekwencyjna aby stwierdzic ze jest hit 
      COV=80 # minimalne pokrycie query i hitu aby stwierdzic ze jest hit 
      EVAL=0.01  # maksymalne e-value
      /opt/docker/EToKi/externals/run_VFDB.sh $ffn ${task.cpus} ${GENUS} \${PIDENT} \${EVAL} \${COV}

      mv VFDB_summary.txt VFDB_summary_${GENUS}.txt

      if [ \${SPEC2} == "Shigella" ]; then
        /opt/docker/EToKi/externals/run_VFDB.sh $ffn ${task.cpus} \${SPEC2} \${PIDENT} \${EVAL} \${COV}
        mv VFDB_summary.txt VFDB_summary_\${SPEC2}.txt
        cat VFDB_summary_${GENUS}.txt VFDB_summary_\${SPEC2}.txt >> VFDB_summary_all.txt 
        python /opt/docker/EToKi/externals/vfdb_parser.py  -i VFDB_summary_all.txt -s "tak" -o vfdb.json
      else
        python /opt/docker/EToKi/externals/vfdb_parser.py  -i VFDB_summary_${GENUS}.txt -s "tak" -o vfdb.json
      fi

    else
      touch VFDB_summary_dummy.txt; touch VFDB_summary_Escherichia.txt  ; touch VFDB_summary_Shigella.txt
      # json na bledny rodzaj
      
      if [ "${params.lan}" == "pl" ]; then
        ERR_MSG=`echo Ten modul jest przeznaczony do analizy bakterii z rodzajów: Salmonella, Escherichia oraz Campylobacter. W tej próbce wykryto: ${GENUS}`
      else
        ERR_MSG=`echo This module works with the following genera: Salmonella, Escherichia, or Campylobacter. Following genus was identified in this sample is: ${GENUS}`
      fi

      python /opt/docker/EToKi/externals/vfdb_parser.py  -i VFDB_summary_Escherichia.txt -s "nie" -r "\${ERR_MSG}" -o vfdb.json
    fi # koniec if-a na rodzja
  
  fi # koniec if-a na przejscie QC
  """

}

process parse_VFDB_ecoli {
  // Parser wynikow dla E.coli w celu okreslenia czy jest to STEC/VTEC itd ...
  tag "Predicting phenotype for sample $x"
  cpus 1
  memory "1 GB"
  time "5m"
  input:
  tuple val(x), path('VFDB_summary_Escherichia.txt'), path('VFDB_summary_Shigella.txt'), val(SPECIES), val(GENUS), val(QC_status), val(QC_status_contaminations)
  output:
  tuple val(x), path('VFDB_phenotype.txt'), emit: to_pubdir
  tuple val(x), env(PATOTYP), emit: json
  //when:
  //GENUS == 'Escherichia'
  script:
  """
    PATOTYP=("NA")
    if [[ ${QC_status} == "nie"  || ${QC_status_contaminations} == "nie" ]]; then
       touch VFDB_phenotype.txt
       # json na zle QC
    else
      if [ ${GENUS} == 'Escherichia' ]; then 
        touch VFDB_phenotype.txt
        ### STEC ###
        ### Geny STX1 lub 2 
    
        STEC=0
        STEC=`cat VFDB_summary_Escherichia.txt | grep "stx1\\|stx2" | grep -v BRAK | wc -l`
        GENY_STEC=`cat VFDB_summary_Escherichia.txt | grep "stx1\\|stx2" | grep -v BRAK | cut -f4 | tr "\\n" " "`
        if [ \${STEC} -gt 0 ]; then
          echo -e "STEC\\t\${GENY_STEC}" >> VFDB_phenotype.txt
        fi
        ### Konice 

        ### EPEC ###
        ### EPEC definiujemy obecnosci intyminy
        ### ktora wystepuje w wynikach 2 razy jako czesc roznych sciezek (stad head -1 nizej)

        EPEC=0
        # eaeH to nie intymina
        EPEC=`cat VFDB_summary_Escherichia.txt | grep -w "eae"  | grep -v BRAK | wc -l`
        GENY_EPEC=`cat VFDB_summary_Escherichia.txt | grep -w "eae"  | grep -v BRAK | cut -f4 | tr "\\n" " "`
        if [ \${EPEC} -gt 0 ]; then
          echo -e "EPEC\\t\${GENY_EPEC}" >> VFDB_phenotype.txt
        fi
 
        ### Koniec

        ### EAEC ###
        ### Definicja Tomka i VFDB do uzgodnienia
        ### geny adherence-  aafA; aafB; aafC; aafD 
        ### geny wirulencji east1 oraz set1a i set1b
        ### dyspersyna - gen aap

        ### Geny AggR (kontroler kilku genow w tym dyspersyny), i gen aa1c czesc secration system
    
        EAST=0
        SET=0  
        AAF=0 
        AAP=0 

        AGGR=0 
        AAIC=0
 
        EAST=`cat VFDB_summary_Escherichia.txt | grep -w east1 | grep -v BRAK | wc -l`
        SET=`cat VFDB_summary_Escherichia.txt | grep "set1A\\|set1B" | grep -v BRAK | wc -l`
        AAF=`cat VFDB_summary_Escherichia.txt | grep "aafA\\|aafB\\|aafC\\|aafD" | grep -v BRAK | wc -l`
        AAP=`cat VFDB_summary_Escherichia.txt | grep -w aap | grep -v BRAK | wc -l`

        AGGR=`cat VFDB_summary_Escherichia.txt | grep -w aggR | grep -v BRAK | wc -l`
        AAIC=`cat VFDB_summary_Escherichia.txt | grep -w aaic-hcp |  grep -v BRAK | wc -l`
 
        if [ \${AGGR} -gt 0 ] && [ \${AAIC} -gt 0 ] ; then 
          echo -e "EAEC\\taggR\\taaiC" >> VFDB_phenotype.txt
        elif [ \${AGGR} -gt 0 ]; then
          echo -e "EAEC\\taggR" >> VFDB_phenotype.txt
        elif [ \${AAIC} -gt 0 ]; then
          echo -e "EAEC\\taaiC" >> VFDB_phenotype.txt
        fi

        ### Konice

        ### EIEC ###
        ### Tu jest prosta definicaja ale gen jest nie w bazie Ecoli a w bazie Shigella
        IPAH=0
  
        IPAH=`cat VFDB_summary_Shigella.txt | grep ipaH | grep -v BRAK | wc -l`
        IPAH_GENES=`cat VFDB_summary_Shigella.txt | grep ipaH | grep -v BRAK |  cut -f4 | tr "\\n" " "`
        if [ \${IPAH} -gt 0 ]; then
            echo -e "EIEC\\t\${IPAH_GENES}" >> VFDB_phenotype.txt
        fi
   
        ### Koniec 

        ### ETEC ###
        ELT=0 # Heat liable # wiele izoform
        EST=0 # Heat Stable
   
        ELT=`cat VFDB_summary_Escherichia.txt | grep elt | grep -v BRAK | wc -l`
        ELT_GENES=`cat VFDB_summary_Escherichia.txt | grep elt | grep -v BRAK | cut -f4 | tr "\\n" " "`
        EST=`cat VFDB_summary_Escherichia.txt | grep estIa | grep -v BRAK | wc -l`  
   
        if [ \${ELT} -gt 0 ] && [ \${EST} -gt 0 ] ; then
          echo -e "ETEC\\t\${ELT_GENES}\\testIa" >> VFDB_phenotype.txt
        elif [ \${ELT} -gt 0 ]; then
          echo -e "ETEC\\t\${ELT_GENES}" >> VFDB_phenotype.txt
        elif [ \${EST} -gt 0 ]; then
          echo -e "ETEC\testIa" >> VFDB_phenotype.txt
        fi

        ### Koniec
        PATOTYP=(`cat VFDB_phenotype.txt | cut -f1`)
    else
      touch VFDB_phenotype.txt
      # json na zly gatunek
    fi # koniec if-a na zly gatunek
  fi # Koniec if-a na Quality
  """
}