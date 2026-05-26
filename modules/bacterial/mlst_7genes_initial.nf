process run_initial_mlst_illumina {
  // Pierwszy check MLST 7 genomwego dla celow QC
  // z wykorzystaniem soft cge
  // Proces jest switchem dla wartosci QC
  // Zmienia status z tak na nie jesli gatunek to cos innego niz Salmonellea, Campylo lub Ecoli
  // oraz jesli liczba unikalnych loci nie wynosi co najmniej 5 (parametr params.unique_loci) 
  container  = params.main_image
  containerOptions "--volume ${params.db_absolute_path_on_host}:/db"
  cpus 1
  memory "5 GB"
  time "5m"
  tag "Initial MLST for sample $x"
  cpus 1
  input:
  tuple val(x), path(reads), val(SPECIES), val(GENUS), val(QC_status)
  output:
  tuple val(x), path(reads), env(QC_status_exit), emit: reads_and_status
  tuple val(x), path('initial_mlst.json'), emit: json
  script:
  read_1 = reads[0]
  read_2 = reads[1]
  """
  QC_status_exit="${QC_status}"
  mkdir tmp
  if [ ${QC_status} == "nie" ]; then
    
    if [ "${params.lan}" == "pl" ]; then
      ERR_MSG="Ten moduł został uruchomiony na próbce, która nie przeszła kontroli jakości."
    else
      ERR_MSG="This sample failed a QC analysis during an earlier phase of the analysis."
    fi
    
    QC_status_exit=`python /opt/docker/EToKi/externals/initial_mlst_parser.py -s ${QC_status} -r "\${ERR_MSG}" -o initial_mlst.json --lan ${params.lan}`
  else
    if [[ "${GENUS}" == *"Salmo"* ]]; then
      python /opt/docker/mlst/mlst.py -i ${read_1} ${read_2} -s senterica -p /db/mlst_db/ -mp kma -t tmp/
    elif [[ "${GENUS}" == *"Escher"* ]]; then
      python /opt/docker/mlst/mlst.py -i ${read_1} ${read_2} -s ecoli1 -p /db/mlst_db/ -mp kma -t tmp/
    elif [ ${GENUS} == "Campylobacter" ]; then
      # w tej bazie podgatunki campylo okreslane sa typowo z cjejuni, clari itd .. 
      python /opt/docker/mlst/mlst.py -i ${read_1} ${read_2} -s c${SPECIES} -p /db/mlst_db/ -mp kma -t tmp/
    elif [ ${GENUS} == "Legionella" ] && [ -f /db/mlst_db/legionella.fsa ]; then
      # Legionella obslugujemy tylko gdy baza MLST jest dostepna
      # w przeciwnym wypadku probka trafia do galezi "else" i traktowana jest jak gatunek nieobslugiwany
      python /opt/docker/mlst/mlst.py -i ${read_1} ${read_2} -s legionella -p /db/mlst_db/ -mp kma -t tmp/
       
    else
      # We encountered wrong genus
      
     if [ "${params.lan}" == "pl" ]; then
        ERR_MSG=`echo Ten program jest przeznaczony do analizy bakterii z rodzajów: Salmonella, Escherichia, Campylobacter oraz Legionella \\(o ile zainstalowano baze MLST dla Legionella\\). W tej próbce wykryto: ${GENUS}`
      else
        ERR_MSG=`echo This program works with the following genera: Salmonella, Escherichia, Campylobacter, or Legionella \\(provided the Legionella MLST database is installed\\). Following genus was identified in this sample is: ${GENUS}`
      fi

      QC_status_exit=`python /opt/docker/EToKi/externals/initial_mlst_parser.py -s blad -r "\${ERR_MSG}" -o initial_mlst.json --lan ${params.lan}`
    fi

    # Parsowanie wyniku i zwracanie json i statusu QC
    if [ \${QC_status_exit} == "tak" ] ;then
      # mamy poprawny gatunek (QC status nie zmienil sie na nie wyzej)
      # QC moze zienic sie na "nie" jesli nie spelania parametru QC
      # RESEULT=`ls *res` # plik ma zwykle nazwe kma_{nazwa_gatunkut}_{nazwapliku_fastq do momentu spotkania R{1,2}, przy czym numer jest opusczany}.res
      OUT_FILE=`ls *res`
      QC_status_exit=`python /opt/docker/EToKi/externals/initial_mlst_parser.py -i "\${OUT_FILE}" -x ${params.unique_loci}  -s tak -o initial_mlst.json --lan ${params.lan}`
    fi
  fi
  """
}


process run_initial_mlst_nanopore {
  // Kopia procesu dla illuminy ale obslugujacy jeden plik fastq.gz
  container  = params.main_image
  containerOptions "--volume ${params.db_absolute_path_on_host}:/db"
  cpus 1
  memory "5 GB"
  time "5m"
  tag "Initial MLST for sample $x"
  input:
  tuple val(x), path(reads), val(SPECIES), val(GENUS), val(QC_status)
  output:
  tuple val(x), path(reads), val(SPECIES), val(GENUS), env(QC_status_exit), emit: reads_and_status
  tuple val(x), path('initial_mlst.json'), emit: json
  script:
  """
  mkdir tmp
  QC_status_exit="${QC_status}"
  if [ ${QC_status} == "nie" ]; then
    
    if [ "${params.lan}" == "pl" ]; then
      ERR_MSG="Ten moduł został uruchomiony na próbce, która nie przeszła kontroli jakości."
    else
      ERR_MSG="This module was eneterd with failed QC and poduced no valid output"
    fi

    QC_status_exit=`python /opt/docker/EToKi/externals/initial_mlst_parser.py -s ${QC_status} -r "\${ERR_MSG}" -o initial_mlst.json --lan ${params.lan} `  
  else
    if [[ "${GENUS}" == *"Salmo"* ]]; then
    python /opt/docker/mlst/mlst.py -i ${reads} -s senterica -p /db/mlst_db/ -mp kma -t tmp/
    elif [[ "${GENUS}" == *"Escher"* ]]; then
    python /opt/docker/mlst/mlst.py -i ${reads} -s ecoli1 -p /db//mlst_db/ -mp kma -t tmp/
    elif [ ${GENUS} == "Campylobacter" ]; then
    # w tej bazie podgatunki campylo okreslane sa typowo z cjejuni, clari itd ..
    python /opt/docker/mlst/mlst.py -i ${reads} -s c${SPECIES} -p /db/mlst_db/ -mp kma -t tmp/
    elif [ ${GENUS} == "Legionella" ] && [ -f /db/mlst_db/legionella.fsa ]; then
    # Legionella obslugujemy tylko gdy baza MLST jest dostepna
    # w przeciwnym wypadku probka trafia do galezi "else" i traktowana jest jak gatunek nieobslugiwany
    python /opt/docker/mlst/mlst.py -i ${reads} -s legionella -p /db/mlst_db/ -mp kma -t tmp/
    else
      if [ "${params.lan}" == "pl" ]; then
        ERR_MSG=`echo Ten modul jest przeznaczony do analizy bakterii z rodzajów: Salmonella, Escherichia, Campylobacter oraz Legionella \\(o ile zainstalowano baze MLST dla Legionella\\). W tej próbce wykryto: ${GENUS}`
      else
        ERR_MSG=`echo This module works with the following genera: Salmonella, Escherichia, Campylobacter, or Legionella \\(provided the Legionella MLST database is installed\\). Following genus was identified in this sample is: ${GENUS}`
      fi

      QC_status_exit=`python /opt/docker/EToKi/externals/initial_mlst_parser.py -s blad -r "\${ERR_MSG}" -o initial_mlst.json --lan ${params.lan}`
    fi

    if [ \${QC_status_exit} == "tak" ] ;then
      # mamy poprawny gatunek (QC status nie zmienil sie na nie wyzej)
      # QC moze zienic sie na "nie" jesli nie spelania parametru QC
      # RESEULT=`ls *res` # plik ma zwykle nazwe kma_{nazwa_gatunkut}_{nazwapliku_fastq do momentu spotkania R{1,2}, przy czym numer jest opusczany}.res
      OUT_FILE=`ls *res`
      QC_status_exit=`python /opt/docker/EToKi/externals/initial_mlst_parser.py -i "\${OUT_FILE}" -x ${params.unique_loci}  -s tak -o initial_mlst.json --lan ${params.lan}`
    fi

  fi
  """
}