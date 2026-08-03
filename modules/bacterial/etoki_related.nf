
process clean_fastq_illumina {
  // Prosta funkcja zaimplementowa w etoki do czyszczenia plikow fastq, ma na celu rozdzielenie odczytow ktore sa sparowane
  // od tych ktore pary nie maja, trimmowanie odczytow na podstawie jakosci, zmiana nazwy odczytow .
  // Ta funkcja generalnie mimikuje dzialanie trimmomatic-a. Wiec teoretycznie mozna uzyc jego
  // Default na trimming to quality 6 (-q to zmienia)

  container  = params.main_image
  tag "Fixing fastq dla sample $x"
  cpus { params.threads > 10 ? 10 : params.threads }
  memory "10 GB"
  time "10m"
  input:
  tuple val(x), path(reads), val(QC_status)
  output:
  tuple val(x), path('prep_out_L1_R1.fastq.gz'), path('prep_out_L1_R2.fastq.gz'), emit: PE_path
  tuple val(x), path('prep_out_L1_SE.fastq.gz'), emit: SE_path
  tuple val(x), path('prep_out_L1_R1.fastq.gz'), path('prep_out_L1_R2.fastq.gz'), path('prep_out_L1_SE.fastq.gz'), val(QC_status), emit: All_path
  script:
  read_1 = reads[0]
  read_2 = reads[1]
  """
  if [ ${QC_status} == "nie" ]; then
     touch prep_out_L1_R1.fastq.gz
     touch prep_out_L1_R2.fastq.gz
     touch prep_out_L1_SE.fastq.gz
  else
    python /opt/docker/EToKi/EToKi.py prepare --pe ${read_1},${read_2} -p prep_out -c ${task.cpus} -q ${params.quality}
    # w niektorych przypadkach plik SE moze byc pusty (bo pewnie ktos go wczesniej czyscil) 
    # Etoki sie gubi i nie generuje poprawnie pliku SE
    # tworzymy samodzilenie plik z dummy sekwencja tak by reszta skryptu dzialala
    # NIE JEST TO POWOD DO ZMIANY PARAMETRU QC BO PLIKI pair-end SA POPRAWNE
    if [ -e prep_out.1.0.s.fastq.gz ]; then
      echo "@0_SE_0" >> prep_out_L1_SE.fastq
      echo "GTACTGACCAAACTGGTCGTGTAGCGTTTCATGCCACATCGTATTTTCGGCCATTGGCTGATACCTCCATTGTTAACACCCGTAAAAAAAGGGCGCAACATCATAGCTAACAATGACCGTGGATGCACGGTCATTATTTCAGCAATAGGAT" >> prep_out_L1_SE.fastq
      echo "+" >> prep_out_L1_SE.fastq
      echo "AFFFFFFFFFFFFAFFFFFFFFAFFFAFFF/FAFFAAAFF/=FFAAFFAFFFF/FFFFFF//FFFFFFFFA/FFFFFAFFAA=FFFFFFFF/FFFFFFFFF/FFF/FFFFFFFFAFFFAFFFFFAFAF/FF=FFFFAFFFFF/FF/FAFAF" >> prep_out_L1_SE.fastq
      gzip prep_out_L1_SE.fastq
    fi
  fi
  """
}


process clean_fastq_nanopore {
  container  = params.main_image
  cpus { params.threads > 10 ? 10 : params.threads }
  memory "10 GB"
  time "10m"
  tag "Fixing fastq dla sample $x"
  input:
  tuple val(x), path(read), val(SPECIES), val(GENUS), val(QC_status)
  output:
  tuple val(x), path('prep_out_L1_SE.fastq.gz'), val(SPECIES), val(GENUS), val(QC_status)
  script:
  """
  if [ ${QC_status} == "nie" ]; then
    touch prep_out_L1_SE.fastq.gz
  else
    python /opt/docker/EToKi/EToKi.py prepare --se ${read} -c ${task.cpus} -q ${params.quality} -p prep_out 
  fi
  """
}
