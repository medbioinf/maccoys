nextflow.enable.dsl = 2

// The pre-set parameters are for high res mass spectrometry data, adjust them to your needs

// required arguments
params.maccoysBin = ""
params.specDir = ""
params.lowerMassTol = 10
params.upperMassTol = 10
params.maxVarPtm = 3
params.decoysPerTarget = 10
params.maxCharge = 6
params.targetUrl = ""
params.resultsDir = ""
params.fragmentTolerance = "0.02"
params.fragmentBinOffset = "0.0"
params.macpepdbWebApi = ""
// optional arguments
params.ptmFile = ""
params.decoyUrl = ""
params.decoyCacheUrl = ""
params.targetLookupUrl = ""
params.keepSearchFiles = "0"    // If non-0, the search engine config and FASTA files will be deleted after the search

// runtime arguments
params.cometThreads = 0
params.cometMaxForkOverride = 0

// debugging arguments
params.limitMs2 = ""


NUM_CORES = Runtime.runtime.availableProcessors()
comet_max_forks = params.cometThreads > 0 ? Math.round(NUM_CORES / params.cometThreads) : 1
comet_max_forks = params.cometMaxForkOverride > 0 ? params.cometMaxForkOverride : comet_max_forks

process convert_thermo_raw_files {
    maxForks 2
    container 'chambm/pwiz-skyline-i-agree-to-the-vendor-licenses'
    // by mounting the parent directory we can use a symlink to the raw file in the workdir
    containerOptions { "-v ${raw_file.getParent()}:/data" }

    input:
    path raw_file

    output:
    path "${raw_file.getBaseName()}.mzML"

    // can only run when profile conversion is enabled
    when: workflow.profile == 'conversion'

    """
    if [ ! -f ${raw_file.getBaseName()}.mzML ]; then
        wine msconvert ${raw_file} --mzML --zlib --filter "peakPicking true 1-"
    else
        echo "File ${raw_file.getBaseName()}.mzML already exists"
    fi
    """
}

process create_default_comet_params {
    output:
    path "comet.params.new"

    """
    comet -p
    """
}

process indexing {
    publishDir path: "${params.resultsDir}/${mzml.getBaseName()}", mode: 'copy', pattern: '*.json'

    input:
    path mzml

    output:
    tuple path(mzml), path("index.json"), stdout

    script:
    """
    mkdir -p ${params.resultsDir}/${mzml.getBaseName()}
    ${params.maccoysBin} index-spectrum-file ${mzml} index.json
    jq '.spectra | keys[]' index.json | sed 's;";;g' ${params.limitMs2 ? ' | tail -n ' + params.limitMs2 : '' }
    """
}

process search {
    // Try to maximize the number of forks for the comet search process
    if (params.cometThreads > 0) {
        cpus params.cometThreads
        maxForks comet_max_forks
    }

    input:
    tuple path(mzml), path(mzml_index), val(spectrum_id)
    path default_comet_params

    output:
    stdout
    path "*.tsv"

    """
    # Create the result directory for this spectrum ID
    spec_id_result_dir=${params.resultsDir}/${mzml.getBaseName()}/\$(${params.maccoysBin} sanitize-spectrum-id '${spectrum_id}')
    mkdir -p \$spec_id_result_dir

    cat ${default_comet_params} | sed 's/^num_threads = .*\$/num_threads = ${task.cpus}/g' > this.comet.params

    ${params.maccoysBin} search \\
        ${mzml} \\
        ${mzml_index} \\
        '${spectrum_id}' \\
        ./ \\
        ./this.comet.params \\
        ${params.lowerMassTol} \\
        ${params.upperMassTol} \\
        ${params.maxVarPtm} \\
        ${params.fragmentTolerance} \\
        ${params.fragmentBinOffset} \\
        ${params.maxCharge} \\
        ${params.decoysPerTarget} \\
        ${params.targetUrl} \\
        ${params.ptmFile ? '-p ' + params.ptmFile + ' \\': '\\'}
        ${params.decoyUrl ? '-d ' + params.decoyUrl + ' \\' : '\\'}
        ${params.decoyCacheUrl ? '-c ' + params.decoyCacheUrl + ' \\' : '\\'}
        ${params.targetLookupUrl ? '-t ' + params.targetLookupUrl + ' \\' : '\\'}

    # delete extracted mzML file as they can be saftly restored from the orignal mzML file
    rm extracted.mzML

    if [ "${params.keepSearchFiles}" -eq "0" ]; then
        rm *.fasta
        rm *.comet.params
    fi

    for file in *.txt; do
        mv -- "\$file" "\$(basename \$file .txt).psms.tsv"
    done

    # Return the result directory for this spectrum ID as stdout, so it is available as `val` in the next process
    echo -n \$spec_id_result_dir
    """

}

process post_processing {
    publishDir "${spectrum_result_dir}", mode: 'copy'

    input:
    val spectrum_result_dir
    path "*"

    output:
    path "*.tsv", includeInputs: true

    """
    psm_file=(*.tsv)
    ${params.maccoysBin} post-process \$psm_file \$(basename \$psm_file .tsv).goodness.tsv
    """
}

// process filter  {
//     input: 
// }


workflow() {
    raws = Channel.fromPath(params.specDir + "/*.raw")
    mzmls = Channel.fromPath(params.specDir + "/*.{mzML,mzml}")

    converted_raws = convert_thermo_raw_files(raws)

    // Merge mzmls and converted_raws
    mzmls = mzmls.concat(converted_raws)
    create_default_comet_params()
    indexing(mzmls)
    /** 
        Output of indexing is a Channel with tuples of (mzml, mzml_index, spectrum_ids (plural))
        To start a process for each spectrum_id we need to split them up and create a channel returning
        tuples of (mzml, mzml_index, spectrum_id (singular))
    */
    spectra_table = indexing.out.map { mzml, mzml_index, spectrum_ids -> 
        spectrum_ids.split("\n").collect { spectrum_id ->
            tuple(mzml, mzml_index, spectrum_id)
        }
    }.flatten().collate(3)
    search(spectra_table, create_default_comet_params.out)
    post_processing(search.out)
    // filter()
}