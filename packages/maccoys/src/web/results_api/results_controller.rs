use std::{io::BufReader, path::PathBuf, sync::Arc};

use crate::{constants::COMET_SEPARATOR, functions::sanatize_string, web::web_error::WebError};
use anyhow::{bail, Context, Result};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use dihardts_omicstools::{
    mass_spectrometry::spectrum::{SimpleMsNSpectrum, Spectrum},
    proteomics::io::mzml::{index::Index, reader::Reader as MzMlReader},
};
use maccoys_exchange_entities::results_api::{
    Identification, MsRun as MsRunResponse, Search as SearchResponse, Spectrum as SpectrumResponse,
};
use polars::prelude::*;
use tokio::fs::read_to_string;
use tracing::{error, warn};

pub struct ResultController;

impl ResultController {
    pub async fn show_search(
        State(results_dir): State<Arc<PathBuf>>,
        Path(uuid): Path<String>,
    ) -> Result<Response, WebError> {
        let search_dir = results_dir.join(&uuid);

        if !search_dir.exists() {
            return Ok((
                StatusCode::NOT_FOUND,
                serde_json::to_string(&SearchResponse::empty())?,
            )
                .into_response());
        }

        let ms_runs: Vec<String> = search_dir
            .read_dir()?
            .filter_map(|entry| {
                let entry = match entry {
                    Ok(entry) => entry,
                    Err(err) => return Some(Err(err.into())),
                };
                match entry.file_type() {
                    Ok(file_type) => {
                        if file_type.is_dir() {
                            Some(Ok(entry.file_name().to_string_lossy().to_string()))
                        } else {
                            None
                        }
                    }
                    Err(err) => Some(Err(err.into())),
                }
            })
            .collect::<Result<Vec<String>>>()?;

        Ok((
            StatusCode::NOT_FOUND,
            serde_json::to_string(&SearchResponse::new(uuid, ms_runs))?,
        )
            .into_response())
    }

    pub async fn show_ms_run(
        State(results_dir): State<Arc<PathBuf>>,
        Path((uuid, ms_run)): Path<(String, String)>,
    ) -> Result<Response, WebError> {
        let search_dir = results_dir.join(&uuid);

        if !search_dir.exists() {
            return Ok((
                StatusCode::NOT_FOUND,
                serde_json::to_string(&MsRunResponse::empty())?,
            )
                .into_response());
        }

        let ms_run_dir = search_dir.join(&ms_run);

        if !ms_run_dir.exists() {
            return Ok((
                StatusCode::NOT_FOUND,
                serde_json::to_string(&MsRunResponse::empty())?,
            )
                .into_response());
        }

        let ms_run_index = Index::from_json(&read_to_string(ms_run_dir.join("index.json")).await?)?;

        Ok((
            StatusCode::NOT_FOUND,
            serde_json::to_string(&MsRunResponse::new(
                uuid,
                ms_run,
                ms_run_index
                    .get_spectra()
                    .keys()
                    .map(|s| s.to_string())
                    .collect::<Vec<String>>(),
            ))?,
        )
            .into_response())
    }

    pub async fn show_spectrum(
        State(results_dir): State<Arc<PathBuf>>,
        Path((uuid, ms_run, spectrum_id)): Path<(String, String, String)>,
    ) -> Result<Response, WebError> {
        let spectrum_id = urlencoding::decode(&spectrum_id)
            .map_err(|err| {
                WebError::new(
                    StatusCode::BAD_REQUEST,
                    format!("Failed to decode spectrum id: {}", err),
                )
            })?
            .to_string();

        let sanizied_spectrum_id = sanatize_string(&spectrum_id);

        let search_dir = results_dir.join(&uuid);

        if !search_dir.exists() {
            return Err(WebError::new(
                StatusCode::NOT_FOUND,
                "Search not found".to_string(),
            ));
        }

        let ms_run_dir = search_dir.join(&ms_run);

        if !ms_run_dir.exists() {
            return Err(WebError::new(
                StatusCode::NOT_FOUND,
                "MS run not found".to_string(),
            ));
        }

        let spectrum_dir = ms_run_dir.join(&sanizied_spectrum_id);

        if !spectrum_dir.exists() {
            return Err(WebError::new(
                StatusCode::NOT_FOUND,
                "Spectrum not found".to_string(),
            ));
        }

        // Get spectrum from mzML file
        let ms_run_index = Index::from_json(&read_to_string(ms_run_dir.join("index.json")).await?)?;
        let run_mzml_path = &ms_run_dir.join("run.mzML");
        let mut mzml_bytes_reader = BufReader::new(match std::fs::File::open(run_mzml_path) {
            Ok(file) => file,
            Err(err) => {
                return Err(WebError::new(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Failed to open mzML file: {}", err),
                ))
            }
        });
        let mut mzml_file =
            MzMlReader::read_pre_indexed(&mut mzml_bytes_reader, ms_run_index, None, false)?;
        let spectrum = match mzml_file.get_spectrum(&spectrum_id) {
            Ok(spectrum) => spectrum,
            Err(_) => {
                return Err(WebError::new(
                    StatusCode::NOT_FOUND,
                    "Spectrum not found in ms run".to_string(),
                ))
            }
        };

        let spectrum: SimpleMsNSpectrum = match SimpleMsNSpectrum::try_from(spectrum) {
            Ok(spectrum) => spectrum,
            Err(err) => {
                return Err(WebError::new(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Failed to convert spectrum: {}", err),
                ))
            }
        };

        // Collect psm files
        let psm_files_glob = match spectrum_dir.join("*.psms.tsv").to_str() {
            Some(psm_files_glob) => psm_files_glob.to_owned(),
            None => {
                return Err(WebError::new(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Failed to create glob for PSM files".to_string(),
                ))
            }
        };

        let psm_file_paths = match glob::glob(&psm_files_glob) {
            Ok(paths) => paths,
            Err(err) => {
                return Err(WebError::new(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Failed to glob for PSM files: {}", err),
                ))
            }
        };

        let psm_file_paths = match psm_file_paths
            .map(|path| match path {
                Ok(path) => Ok(path),
                Err(err) => Err(err.into()),
            })
            .collect::<Result<Vec<PathBuf>>>()
        {
            Ok(paths) => paths,
            Err(err) => {
                return Err(WebError::new(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Failed to collect PSM files: {}", err),
                ))
            }
        };

        let results_iter = psm_file_paths.into_iter().map(|psms_file_path| {
            let precursor_and_charge = psms_file_path
                .with_extension("")
                .with_extension("")
                .file_name()
                .unwrap()
                .to_string_lossy()
                .to_string();

            let (precursor, charge) = match precursor_and_charge.split_once("_") {
                Some((precursor, charge)) => (
                    precursor.parse::<f64>().unwrap_or(-1.0),
                    charge.parse::<u8>().unwrap_or(0),
                ),
                None => {
                    bail!("Failed to parse precursor and charge from file name");
                }
            };

            let reader = match CsvReadOptions::default()
                .with_parse_options(
                    CsvParseOptions::default().with_separator(COMET_SEPARATOR.as_bytes()[0]),
                )
                .with_has_header(true)
                .try_into_reader_with_file_path(Some(psms_file_path.clone()))
            {
                Ok(reader) => reader,
                Err(err) => {
                    error!("Error when opening PSMs file for reading: {}", err);
                    bail!("Error when opening PSMs file for reading: {}", err);
                }
            };

            // Do not use the PeptideSpectrumMatchTsv file reader here as it will skip the first row which is inly present in the initial Comet file but not after preprocessing
            let psms = match reader.finish() {
                Ok(df) => Some(df),
                Err(err) => match err {
                    PolarsError::NoData(msg) => {
                        warn!("No data in PSMs file: {}", msg);
                        None
                    }
                    _ => {
                        error!("Failed to read PSMs file: {}", err);
                        bail!("Failed to read PSMs file: {}", err);
                    }
                },
            };

            let mut goodness: Option<_> = None;
            // Remove the `.tsv` extension and replace the remaining `.psms` with `.goodness.tsv` extension
            let goodness_file_path = psms_file_path
                .with_extension("")
                .with_extension("goodness.tsv");

            if goodness_file_path.is_file() {
                goodness = match CsvReadOptions::default()
                    .with_has_header(true)
                    .with_parse_options(
                        CsvParseOptions::default().with_separator(COMET_SEPARATOR.as_bytes()[0]),
                    )
                    .try_into_reader_with_file_path(Some(goodness_file_path))
                    .context("Error when opening goodness file")?
                    .finish()
                {
                    Ok(goodness) => Some(goodness),
                    Err(err) => match err {
                        PolarsError::NoData(msg) => {
                            warn!("No data in goodness file: {}", msg);
                            None
                        }
                        _ => {
                            error!("Failed to read goodness file: {}", err);
                            bail!("Failed to read goodness file: {}", err);
                        }
                    },
                }
            }
            Ok(Identification::new(goodness, psms, precursor, charge))
        });

        let results = match results_iter.collect::<Result<Vec<_>>>() {
            Ok(results) => results,
            Err(err) => {
                return Err(WebError::new(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Failed to collect results: {}", err),
                ))
            }
        };

        let response = SpectrumResponse::new(
            uuid,
            ms_run,
            spectrum_id,
            spectrum.get_mz().clone(),
            spectrum.get_intensity().clone(),
            results,
        );

        Ok((StatusCode::OK, serde_json::to_string(&response)?).into_response())
    }
}
