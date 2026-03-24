process trimmomatic {
    // This module only passes QC to downstream modules
    container  = params.main_image
    tag "trimmomatic:${sampleId}"
    cpus 1
    memory "20 GB"
    input:
    tuple val(sampleId), path(reads), val(QC_status)

    output:
    tuple val(sampleId), path('*_paired.fastq.gz'), val(QC_status), emit: proper_reads_and_qc
    tuple val(sampleId), path('*_paired.fastq.gz'), emit: proper_reads
    tuple val(sampleId), path('*_paired.fastq.gz'), path('*_unpaired.fastq.gz'), val(QC_status), emit: all_reads

    script:
    """
    if [ ${QC_status} == "nie" ]; then
        touch dummy_forward_paired.fastq.gz
        touch dummy_reverse_paired.fastq.gz
        touch dummy_forward_unpaired.fastq.gz
        touch dummy_reverse_unpaired.fastq.gz
    else
    ADAPTERS="/home/data/common/adapters/${params.adapters_id}.fa"

    java -jar /opt/trimmomatic/trimmomatic.jar PE ${reads[0]} ${reads[1]} \
                                            forward_paired.fastq.gz \
                                            forward_unpaired.fastq.gz \
                                            reverse_paired.fastq.gz \
                                            reverse_unpaired.fastq.gz \
                                            ILLUMINACLIP:"\${ADAPTERS}":2:30:10:8:True \
                                            LEADING:${params.quality_initial} \
                                            TRAILING:${params.quality_initial} \
                                            SLIDINGWINDOW:4:4 \
                                            MINLEN:${params.length} \
                                            -phred33
    fi
    """
}

