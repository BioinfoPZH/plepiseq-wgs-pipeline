# micro script to parse speciesfinder output stored as a jason
import json
import sys

# sys,argv[1] is a json file
# sys.aegv[2] is either "genus" or "species", otherwise script will print an empty string

best_hit = None
best_hit_score = 0
full_data = json.load(open(sys.argv[1]))
if sys.argv[2] == 'genus' or sys.argv[2] == 'species':
    for klucz, wartosc in full_data['seq_region'].items():
        if wartosc['query_coverage'] > best_hit_score:
            best_hit_score = wartosc['query_coverage']
            best_hit = wartosc['tax'].split(';')[-1].lstrip()


print(best_hit) if sys.argv[2] == 'species' else print(best_hit.split(';')[-1].lstrip().split(" ")[0])
