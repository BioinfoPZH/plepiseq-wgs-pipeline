#!/bin/bash

# Backward compatibility wrapper for run_nf_pipeline_viral.sh
# This script now delegates to the unified run_nf_pipeline.sh script
# to reduce code duplication and maintain DRY principle.

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Transform viral-specific arguments to unified script format
# This wrapper maps the old parameter names to the new unified script format
UNIFIED_ARGS=()
UNIFIED_ARGS+=("--pipeline_type" "viral")

# Process all command-line arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --main_image)
            UNIFIED_ARGS+=("--viral_main_image" "$2")
            shift 2
            ;;
        --manta_image)
            UNIFIED_ARGS+=("--viral_manta_image" "$2")
            shift 2
            ;;
        --medaka_image)
            UNIFIED_ARGS+=("--viral_medaka_image" "$2")
            shift 2
            ;;
        *)
            # Pass through all other arguments unchanged
            UNIFIED_ARGS+=("$1")
            if [[ "$1" != "--no-alphafold" && "$1" != "--all" && "$1" != "-h" && "$1" != "--help" && "$1" != "--" ]]; then
                if [[ $# -gt 1 && "$2" != --* ]]; then
                    UNIFIED_ARGS+=("$2")
                    shift
                fi
            fi
            shift
            ;;
    esac
done

# Call the unified script with transformed arguments
exec "${SCRIPT_DIR}/run_nf_pipeline.sh" "${UNIFIED_ARGS[@]}"
