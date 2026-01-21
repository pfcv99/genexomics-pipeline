#!/usr/bin/env nextflow
nextflow.enable.dsl = 2

/*
 main.nf - Nextflow workflow

 Steps:
   1. Find run_* folders under NAS directory (at workflow start)
   2. Upload files in each run folder to S3 (bin/async_s3_uploader.py)
   3. Create a Quilt package for each run from the S3 prefix (bin/make_quilt_from_s3.py)
   4. Attach metadata using bin/metadata_integrator.py (optional)
*/

params.nas_dir         = params.nas_dir ?: "./tests/nas"
params.s3_config       = params.s3_config ?: "config/s3_config.yaml"
params.section         = params.section ?: "genexomics"
params.bucket_key      = params.bucket_key ?: "genexomics-runs"
params.registry_bucket = params.registry_bucket ?: "genexomics-runs"

params.attach_metadata = params.attach_metadata ?: false

// metadata options (example: benchling)
params.metadata_source = params.metadata_source ?: "benchling"
params.benchling_api_key = params.benchling_api_key ?: null

workflow {

    def nas_dir = file(params.nas_dir)

    channel
        .fromPath("${nas_dir}/*")   // <- use Path object directly, no absolutePath
        .filter { it.isDirectory() }
//        .ifEmpty { error "No run directories found in ${nas_dir}" }
        .set { run_folders }

    upload_process(run_folders)
        .set { s3_uri_files }

    make_quilt(s3_uri_files)
        .set { quilt_packages }

    if (params.attach_metadata) {
        attach_metadata(quilt_packages)
    }
}


/*
 * Upload each run directory to S3.
 * async_s3_uploader.py prints one s3:// URI per uploaded object to stdout.
 */
process upload_process {

    tag { run_folder.baseName }
    publishDir "logs", mode: 'copy'

    input:
    path run_folder

    output:
    path "*.s3_uris.txt"

    script:
    def run_name = run_folder.baseName
    """
    set -euo pipefail

    rm -f ${run_name}.s3_uris.txt

    for f in \$(find "${run_folder}" -type f); do
        python3 bin/async_s3_uploader.py \
            --input "\$f" \
            --config "${params.s3_config}" \
            --section "${params.section}" \
            --bucket-key "${params.bucket_key}" \
            --log-dir "./logs"
    done | sort -u > ${run_name}.s3_uris.txt
    """
}


/*
 * Create a Quilt package from S3 objects.
 * make_quilt_from_s3.py generates the final package name (with timestamp)
 * and prints it via logging; we extract it from stdout/log.
 */
process make_quilt {

    tag { s3_uris_file.baseName }
    publishDir "logs", mode: 'copy'

    input:
    path s3_uris_file

    output:
    path "*.quilt_package.txt"

    script:
    def run_base = s3_uris_file.baseName
    def first_line = new File(s3_uris_file.toString()).readLines()[0]
    def bucket = first_line.tokenize('/')[2]
    def prefix = first_line.replace("s3://${bucket}/","").tokenize('/')[0]

    """
    set -euo pipefail

    python3 bin/make_quilt_from_s3.py \
        --bucket ${bucket} \
        --prefix ${prefix} \
        --namespace ${params.section} \
        --package-base ${run_base} \
        --registry s3://${params.registry_bucket} \
        --message "Quilt package for ${run_base}" \
        | tee ${run_base}.quilt.log

    grep "Created package:" ${run_base}.quilt.log | awk '{print \$NF}' > ${run_base}.quilt_package.txt
    """
}


/*
 * Attach metadata to the Quilt package (optional).
 * Requires a concrete metadata source (benchling or smartsheet).
 */
process attach_metadata {

    tag { q.baseName }
    publishDir "logs", mode: 'copy'

    input:
    path q

    output:
    stdout

    script:
    def pkg_name = new File(q.toString()).readLines()[0].trim()

    if (params.metadata_source == "benchling") {
        """
        set -euo pipefail

        python3 bin/metadata_integrator.py \
            --package ${pkg_name} \
            --registry s3://${params.registry_bucket} \
            benchling \
            --benchling-entity-id ${q.baseName} \
            --benchling-api-key "${params.benchling_api_key}"
        """
    } else {
        """
        echo "Metadata source '${params.metadata_source}' not implemented."
        """
    }
}
