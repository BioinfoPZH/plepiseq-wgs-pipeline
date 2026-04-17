
process spades {
  // Funkcja do odpalania spadesa
  // Powstaly plik fasta jest poprawiany bo Hapo-G nie akceptuje "." w nazwach sekwencji w pliku fasta
  // Modul definiuje QC_status az do modulu extract_final_stats, gdzie status moze ulec zmianie
  cpus params.threads
  memory "20 GB"
  time "30m"
  container  = params.main_image
  tag "Spades dla sample $x"
  input:
  tuple val(x), path('R1.fastq.gz'), path('R2.fastq.gz'), path('SE.fastq.gz'), val(QC_status)
  output:
  tuple val(x),  path('scaffolds_fix.fasta'), val(QC_status)
  script:
  """
  #  for 150bp reads SPAdes uses k-mer sizes 21, 33, 55, 77
  #  We strongly recommend not to change -k parameter unless you are clearly aware about the effect.
  # --isolate This flag is highly recommended for high-coverage isolate and multi-cell Illumina data;
  # --careful Tries to reduce the number of mismatches and short indels. ale nie dziala z isolate
  #  wywolanie spades w ramach etoki to tylko definicja inputu + liczby procesorow
  # /opt/docker/EToKi/externals/spades.py -t 8 --pe-1 1 /Salomenlla/test_etoki/id_151_novel_etoki_with_comments/prep_out_L1_R1.fastq.gz --pe-2 1 /Salomenlla/test_etoki/id_151_novel_etoki_with_comments/prep_out_L1_R2.fastq.gz --pe-s 2 /Salomenlla/test_etoki/id_151_novel_etoki_with_comments/prep_out_L1_SE.fastq.gz -o spades

  if [ ${QC_status} == "nie" ]; then
    # We create dummy files so that pipeline can continue
    echo ">dummy_contig" >> scaffolds_fix.fasta
    echo "AAAAAAAAAAAAA" >> scaffolds_fix.fasta
    # zwracanie json z informacja ze na tym etapie pipeline zakoczyl dzialanie z powodu zbyt malej liczby odczytow
    # wszystkie podrogramy przecwytuja komentarz i zwracaja odpowidni komunikat 
  else
    python /opt/docker/EToKi/externals/spades.py --isolate -t ${task.cpus} --pe-1 1 R1.fastq.gz --pe-2 1 R2.fastq.gz --pe-s 2 SE.fastq.gz  -o spades_manual
    cat spades_manual/scaffolds.fasta  | awk '{if(\$0 ~ />/) {split(\$0, ala, "_"); print ">NODE"ala[2]} else print \$0}' >> scaffolds_fix.fasta
  
  fi
  """
}