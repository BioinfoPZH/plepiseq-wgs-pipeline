process run_split_fasta {
  // Prosty proces to rozdzielenia pliku z wieloma fastami na podfasty
  // Oraz wygenerowanie odpowiedniego jsona
  container  = params.main_image
  cpus 1
  memory "1 GB"
  time "2m"
  tag "Splitting genome fasta for sample $x"
  publishDir "${params.results_dir}/${x}/fastas", mode: 'copy', pattern: "*fasta"
  input:
    tuple val(x), path(fasta), val(QC_status)
  output:
    tuple val(x), path("fasta_info.json"), emit: json
    tuple val(x), path("*fasta"), emit: to_pubdir
  script:
  """
#!/usr/bin/python
from Bio import SeqIO
import json

qc_status="$QC_status"
fasta_file="$fasta"

json_output = {}
tmp_list = []

if qc_status == "nie":
    with open("dummy.fasta", "w") as f:
       f.write("This module was eneterd with failed QC and poduced no valid output")
    json_output["status"] = "nie"
    json_output["error_message"] = "This module was eneterd with failed QC and poduced no valid output"
else:
    json_output["status"] = "tak"
    records = SeqIO.parse(fasta_file, "fasta")
    for record in records:
        tmp_list.append({"segment_name" : record.id,
                         "segment_file" : "${params.results_dir}/${x}/fastas/" + f"{record.id}.fasta"})
        with open(f"{record.id}.fasta", "w") as f:
            f.write(f">{record.id}\\n{str(record.seq)}")

    json_output["file_data"] = tmp_list

with open("fasta_info.json", 'w') as f1:
        f1.write(json.dumps(json_output, indent = 4))

  """
}