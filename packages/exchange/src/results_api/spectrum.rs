/// Represents a spectrum and its content (e.g. the identifications that are part of the spectrum)
#[derive(serde::Serialize, serde::Deserialize)]
pub struct Spectrum {
    search_uuid: String,
    ms_run_name: String,
    spectrum_id: String,
    mz: Vec<f64>,
    intensity: Vec<f64>,
    // TODO: add list of precursor to reference PSMs per Pprecursor
}

impl Spectrum {
    pub fn new(
        search_uuid: String,
        ms_run_name: String,
        spectrum_id: String,
        mz: Vec<f64>,
        intensity: Vec<f64>,
    ) -> Self {
        Self {
            search_uuid,
            ms_run_name,
            spectrum_id,
            mz,
            intensity,
        }
    }

    pub fn get_search_uuid(&self) -> &str {
        &self.search_uuid
    }

    pub fn get_ms_run(&self) -> &str {
        &self.ms_run_name
    }

    pub fn get_spectra_id(&self) -> &str {
        &self.spectrum_id
    }

    pub fn get_mz(&self) -> &Vec<f64> {
        &self.mz
    }

    pub fn get_intensity(&self) -> &Vec<f64> {
        &self.intensity
    }
}
