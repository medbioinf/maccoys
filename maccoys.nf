nextflow.enable.dsl = 2

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
// optional arguments
params.ptmFile = ""
// TODO: Add remaining optional arguments: target lookup, decoy db and decoy cache


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
        ${params.maxCharge} \\
        ${params.decoysPerTarget} \\
        ${params.targetUrl} \\
        ${params.ptmFile ? '-p ' + params.ptmFile : ''} 

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