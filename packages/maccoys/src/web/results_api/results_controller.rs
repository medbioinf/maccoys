use std::{io::BufReader, path::PathBuf, sync::Arc};

use crate::{
    pipeline::utils::{get_ms_run_index_path, get_ms_run_mzml_path},
    web::web_error::WebError,
};
use anyhow::Result;
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
    MsRun as MsRunResponse, Search as SearchResponse, Spectrum as SpectrumResponse,
};
use tokio::fs::read_to_string;

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

        let ms_run_index_path = results_dir.join(get_ms_run_index_path(&uuid, &ms_run));
        let run_mzml_path = get_ms_run_mzml_path(&uuid, &ms_run);

        if !ms_run_index_path.exists() {
            return Err(WebError::new(
                StatusCode::NOT_FOUND,
                "MS run index file not found".to_string(),
            ));
        }

        if !run_mzml_path.exists() {
            return Err(WebError::new(
                StatusCode::NOT_FOUND,
                "MS run mzML file not found".to_string(),
            ));
        }

        // Get spectrum from mzML file
        let ms_run_index = Index::from_json(&read_to_string(&ms_run_index_path).await?)?;
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

        let response = SpectrumResponse::new(
            uuid,
            ms_run,
            spectrum_id,
            spectrum.get_mz().clone(),
            spectrum.get_intensity().clone(),
        );

        Ok((StatusCode::OK, serde_json::to_string(&response)?).into_response())
    }
}
