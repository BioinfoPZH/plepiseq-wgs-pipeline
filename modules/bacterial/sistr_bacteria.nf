process run_sistr {
  // Sistr works only for Salmonella
  container  =  params.main_image
  tag "Predicting OH for sample $x with Sistr"
  cpus { params.threads > 3 ? 3 : params.threads }
  memory "5 GB"
  time "5m"
  input:
  tuple val(x), path(fasta), val(QC_status), val(SPECIES), val(GENUS), val(QC_status_contaminations)
  output:
  tuple val(x), path('sistr-output.tab'), emit: to_pubdir
  tuple val(x), path('sistr.json'), emit: json
  // when:
  // GENUS == 'Salmonella'
  script:
  // Uwaga sistr korzysta z WLASNEJ BAZY do pobrania z 
  // SISTR_DB_URL = 'https://sairidapublic.blob.core.windows.net/downloads/sistr/database/SISTR_V_1.1_db.tar.gz'
  // adres ustawiony jest na sztywno w /usr/local/lib/python3.8/dist-packages/sistr/src/serovar_prediction/constants.py
  // Nie wiem co bedzie w przyszlych wersjachh sistr i nie wiem jak to jest aktualizowane
  // baza rozpakowywana jest do 
  // /usr/local/lib/python3.8/dist-packages/sistr/data
  // NIE ZNALAZLEM opcji podania innej sciezki do bazy niz ten default w opcjach programu sistr
  // Baza sciagana jest na poziomie dockera
  // jesli baza nie jest sciagnieta program robi to sam przy wywolaniu sistr-a
  // baza ma jakies 50 Mb /przed rozpakowanie/ wiecjej uzywanie to nie jest jaki wielki problem 
  // ponadto w katalogu /usr/local/lib/python3.8/dist-packages/sistr/ musi byc plik dbstatus.txt
  // bo jak go nie ma to sistr_cmd dalej wymusza sciaganie bazy
  """
  if [[ ${QC_status} == "nie"  || ${QC_status_contaminations} == "nie" ]]; then
    # json na zle QC
    touch sistr-output.tab
    
    if [ "${params.lan}" == "pl" ]; then
      ERR_MSG="Ten moduł został uruchomiony na próbce, która nie przeszła kontroli jakości."
    else
      ERR_MSG="This module was eneterd with failed QC and poduced no valid output"
    fi

    python /opt/docker/EToKi/externals/sistr_parser.py  -i sistr-output.tab -s "nie" -r "\${ERR_MSG}" -o sistr.json
  else
    if [ ${GENUS} == "Salmonella" ]; then
      /usr/local/bin/sistr --qc -vv --alleles-output allele-results.json --novel-alleles novel-alleles.fasta --cgmlst-profiles cgmlst-profiles.csv -f tab -t ${task.cpus} -o sistr-output.tab $fasta
      python /opt/docker/EToKi/externals/sistr_parser.py  -i sistr-output.tab -s "tak" -o sistr.json
    else
      # json na zly gatunek
      if [ "${params.lan}" == "pl" ]; then
        ERR_MSG="Ten moduł jest przeznaczony wyłącznie dla bakterii z rodzaju Salmonella"
      else
        ERR_MSG="This module works only with bacteria from Salmonella genus"
      fi

      touch sistr-output.tab
      python /opt/docker/EToKi/externals/sistr_parser.py  -i sistr-output.tab -s "nie" -r "\${ERR_MSG}" -o sistr.json
    fi # koniec if-a na zly gatunek
  fi # koniec if-a na zle QC
  """
}
