nextflow.enable.dsl = 2

// The pre-set parameters are for high res mass spectrometry data, adjust them to your needs

// required arguments
params.maccoysBin = ""
params.mzmlDir = ""
params.lowerMassTol = 10
params.upperMassTol = 10
params.maxVarPtm = 3
params.decoysPerTarget = 10
params.maxCharge = 6
params.targetUrl = ""
params.resultsDir = ""
params.fragmentTolerance = "0.02"
params.fragmentBinOffset = "0.0"
// optional arguments
params.ptmFile = ""
params.decoyUrl = ""
params.decoyCacheUrl = ""
params.targetLookupUrl = ""
params.keepSearchFiles = "0"    // If non-0, the search engine config and FASTA files will be deleted after the search

// process raw_file_conversion {
// }

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
    cargo run -r -- index-spectrum-file ${mzml} index.json
    jq '.spectra | keys[]' index.json | sed 's;";;g' | tail -n 3
    """
}

process search {
    maxForks 4

    input:
    tuple path(mzml), path(mzml_index), val(spectrum_id)
    path default_comet_params

    output:
    val "${mzml.getBaseName()}"
    path "*.tsv"

    """
    ${params.maccoysBin} search \\
        ${mzml} \\
        ${mzml_index} \\
        '${spectrum_id}' \\
        ./ \\
        ${default_comet_params} \\
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
        ${params.targetLookupUrl ? '-p ' + params.targetLookupUrl + ' \\' : '\\'}

    # delete extracted mzML file as they can be saftly restored from the orignal mzML file
    rm *.extracted.mzML

    if [ "${params.keepSearchFiles}" -eq "0" ]; then
        rm *.fasta
        rm *.comet.params
    fi

    for file in *.txt; do
        mv -- "\$file" "\$(basename \$file .txt).tsv"
    done
    """
}

process rescoring {
    publishDir "${params.resultsDir}/${mzml_base_name}", mode: 'copy'

    input:
    val mzml_base_name
    path "*"

    output:
    path "*.tsv", includeInputs: true

    """
    ${params.maccoysBin} rescore *.tsv
    """
}

// process filter  {
//     input: 
// }


workflow() {
    mzmls = Channel.fromPath(params.mzmlDir + "/*.{mzML,mzml}")
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
    rescoring(search.out)
    // filter()
}