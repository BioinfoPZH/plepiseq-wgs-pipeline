#!/bin/bash

# Unified entry point for both viral and bacterial WGS pipelines.
# Routes to the correct sub-wrapper based on --organism value.
# All other arguments are forwarded verbatim.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

VIRAL_ORGANISMS=("SARS-CoV-2" "Influenza" "RSV")
BACTERIAL_ORGANISMS=("Salmonella" "Escherichia" "Campylobacter")

usage() {
    echo "Usage: $0 --organism ORGANISM [pipeline-specific options...]"
    echo ""
    echo "  --organism VALUE    Name of the organism being analyzed."
    echo "                      Viral:     ${VIRAL_ORGANISMS[*]}"
    echo "                      Bacterial: ${BACTERIAL_ORGANISMS[*]}"
    echo ""
    echo "All remaining arguments are forwarded to the appropriate pipeline wrapper."
    echo "Run with --organism VALUE -h to see pipeline-specific options."
    echo ""
    echo "Examples:"
    echo "  $0 --organism SARS-CoV-2 --machine Illumina --reads '/path/*_R{1,2}.fastq.gz' --primers_id Artic_V4.1"
    echo "  $0 --organism Salmonella --machine Illumina --reads '/path/*_R{1,2}.fastq.gz'"
}

if [[ $# -eq 0 ]]; then
    usage
    exit 1
fi

# Extract --organism from arguments, collect everything else
organism=""
forward_args=()

while [[ $# -gt 0 ]]; do
    case "$1" in
        --organism)
            if [[ -z "$2" || "$2" == --* ]]; then
                echo "Error: --organism requires a value."
                usage
                exit 1
            fi
            organism="$2"
            shift 2
            ;;
        -h|--help)
            if [[ -z "$organism" ]]; then
                usage
                exit 0
            fi
            forward_args+=("$1")
            shift
            ;;
        *)
            forward_args+=("$1")
            shift
            ;;
    esac
done

if [[ -z "$organism" ]]; then
    echo "Error: --organism is required."
    usage
    exit 1
fi

# Route to the correct sub-wrapper
for v in "${VIRAL_ORGANISMS[@]}"; do
    if [[ "$organism" == "$v" ]]; then
        exec "${SCRIPT_DIR}/run_nf_pipeline_viral.sh" --species "$organism" "${forward_args[@]}"
    fi
done

for b in "${BACTERIAL_ORGANISMS[@]}"; do
    if [[ "$organism" == "$b" ]]; then
        exec "${SCRIPT_DIR}/run_nf_pipeline_bacterial.sh" --genus "$organism" "${forward_args[@]}"
    fi
done

echo "Error: Unknown organism '${organism}'."
echo "Supported values: ${VIRAL_ORGANISMS[*]} ${BACTERIAL_ORGANISMS[*]}"
exit 1
