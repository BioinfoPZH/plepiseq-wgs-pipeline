process run_cgMLST_final_json {
  // Proces aggreguje wszystkie moduly zwiazane do wygenerowania json zgodnego z zakladka mlst_data z dokumentacji
  container  = params.main_image
  cpus 1
  memory "1 GB"
  time "2m"
  tag "generate final cgMLST json for sample $x"
  input:
  tuple val(x), path('cgMLST_initial.json'), path('cgMLST_phiercc_local.json'), path('cgMLST_phiercc_enterobase.json'), path('cgMLST_phiercc_pubmlst.json')
  output:
  tuple val(x), path('cgMLST_full.json'), emit: json
  script:
  """
#!/usr/bin/python
import  sys
import re
import json

initial_json = json.load(open('cgMLST_initial.json'))
phiercc_local = json.load(open('cgMLST_phiercc_local.json'))
phiercc_enterobase = json.load(open('cgMLST_phiercc_enterobase.json'))
phiercc_pubmlst = json.load(open('cgMLST_phiercc_pubmlst.json'))

if "error_message" in initial_json.keys():
    # moduly do cgMLSST weszly z errorem wiec jedyne co robie to kopiuje plik 
    with open('cgMLST_full.json', 'w') as f:
        f.write(json.dumps(initial_json))
    sys.exit(0)
else:
    to_dump = initial_json
    # dodajemy wyniki kolejnych modulow, jesli maja one klucz dummy to takiego json nie dodaje
    if "dummy" not in phiercc_local.keys():
        to_dump = {**to_dump, **phiercc_local}
    if "dummy" not in phiercc_enterobase.keys():
        to_dump = {**to_dump, **phiercc_enterobase}
    if "dummy" not in phiercc_pubmlst.keys():
        to_dump = {**to_dump, **phiercc_pubmlst}
    with open('cgMLST_full.json', 'w') as f:
        f.write(json.dumps(to_dump))
    sys.exit(0)
  """
}