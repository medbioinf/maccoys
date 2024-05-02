nextflow.enable.dsl = 2

// The pre-set parameters are for high res mass spectrometry data, adjust them to your needs

// required arguments
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
params.result_copy_sanity_check = false     // set this to true to write an empy file with the basename of the mzml file to the result directory to make sure it is copied into the correct directory


NUM_CORES = Runtime.runtime.availableProcessors()
comet_max_forks = params.cometThreads > 0 ? Math.round(NUM_CORES / params.cometThreads) : 1
comet_max_forks = params.cometMaxForkOverride > 0 ? params.cometMaxForkOverride : comet_max_forks

process convert_thermo_raw_files {
    maxForks 2
    container 'chambm/pwiz-skyline-i-agree-to-the-vendor-licenses'

    input:
    path raw_file

    output:
    path "${raw_file.getBaseName()}.mzML"

    // can only run when profile docker is enabled
    when: workflow.profile == 'docker'

    """
    if [ ! -f ${raw_file.getBaseName()}.mzML ]; then
        wine msconvert ${raw_file} --mzML --zlib --filter "peakPicking true 1-"
    else
        echo "File ${raw_file.getBaseName()}.mzML already exists"
    fi
    """
}

process create_default_comet_params {
    container 'medbioinf/maccoys-comet:latest'

    output:
    path "comet.params.new"

    """
    comet -p
    """
}

process indexing {
    publishDir path: "${params.resultsDir}/${mzml.getBaseName()}", mode: 'copy', pattern: '*.json'
    container 'medbioinf/maccoys:latest'

    input:
    path mzml

    output:
    tuple path(mzml), path("index.json"), stdout

    script:
    """
    mkdir -p ${params.resultsDir}/${mzml.getBaseName()}
    maccoys index-spectrum-file ${mzml} index.json
    jq '.spectra | keys[]' index.json | sed 's;";;g' ${params.limitMs2 ? ' | tail -n ' + params.limitMs2 : '' }
    """
}

/**
 * Extract the MS2 spectrum and builds a search space for each charge state
 */
process search_preparation {
    container 'medbioinf/maccoys:latest'

    input:
    tuple path(mzml), path(mzml_index), val(spectrum_id)
    path default_comet_params

    output:
    tuple path("*", type: "dir"), stdout

    """
    # Create the result directory for this spectrum ID
    sanitized_spec_id=\$(maccoys sanitize-spectrum-id '${spectrum_id}')
    mkdir \$sanitized_spec_id

    maccoys search-preparation \\
        ${mzml} \\
        ${mzml_index} \\
        '${spectrum_id}' \\
        ./\$sanitized_spec_id \\
        ./comet.params.new \\
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

    if [ "${params.result_copy_sanity_check}" -eq "true" ]; then
        echo -n "" > \$sanitized_spec_id/${mzml.getBaseName()}
    fi
    
    # Construct and print the result folder for this mzML to make it accessible for the next processes
    echo -n ${params.resultsDir}/${mzml.getBaseName()}
    """
}

/**
 * Search the MS2 spectra against the search space
 */
process search {
    container 'medbioinf/maccoys-comet:latest'

    // Try to maximize the number of forks for the comet search process
    if (params.cometThreads > 0) {
        cpus params.cometThreads
        maxForks comet_max_forks
    }

    input:
    tuple path(search_dir), val(result_dir)

    output:
    tuple path("$search_dir", includeInputs: true), val(result_dir)

    """
    cd $search_dir

    # Iterate through 
    for comet_params_file in *.comet.params
    do
        # Basename == charge and basename of fasta file
        charge=\$(basename \$comet_params_file .comet.params)

        # cat + sed + > avoids differences in `-i` usage on BSD and Linux.
        # Reading first into varaibles avoids empty files which appear from time to time
        comet_params=\$(cat \$comet_params_file)
        echo "\$comet_params" | sed 's;^num_threads = .*\$;num_threads = ${task.cpus};g' > \$comet_params_file

        # Run the search
        comet -P\${comet_params_file} -D\${charge}.fasta -N\${charge} extracted.mzML
    done

    # Rename the PSM files to have the correct extension
    for file in *.txt; do
        mv -- "\$file" "\$(basename \$file .txt).psms.tsv"
    done

    # Delete extracted mzML file as they can be saftly restored from the orignal mzML file
    rm extracted.mzML

    # Remove the search engine config and FASTA files if requested
    if [ "${params.keepSearchFiles}" -eq "0" ]; then
        rm *.fasta
        rm *.comet.params
    fi
    """
}

/**
 * Marks target and decoys and calculates the FDR
 */
process fdr {
    container 'medbioinf/maccoys:latest'

    input:
    tuple path(search_dir), val(result_dir)

    output:
    tuple path("$search_dir", includeInputs: true), val(result_dir)

    """
    cd $search_dir
    
    for psm_file in *.psms.tsv
    do
        maccoys post-process \$psm_file
    done
    """
}

/**
 * Calculate goodness of fit and rescore the PSMs using the Python module
 */
process goodness_of_fit_and_rescoring {
    container 'medbioinf/maccoys-py:latest'
    publishDir "${result_dir}", mode: 'copy'

    input:
    tuple path(search_dir), val(result_dir)

    output:
    path "$search_dir", includeInputs: true

    """
    cd $search_dir

    for psm_file in *.psms.tsv
    do
        python -m maccoys comet goodness \$psm_file xcorr \$(basename \$psm_file .tsv).goodness.tsv 
        python -m maccoys comet scoring \$psm_file xcorr exp_score xcorr dist_score
    done
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
    search_dirs = search_preparation(spectra_table, create_default_comet_params.out)
    search_dirs = search(search_dirs)
    search_dirs = fdr(search_dirs)
    goodness_of_fit_and_rescoring(search_dirs)
    // filter()
}