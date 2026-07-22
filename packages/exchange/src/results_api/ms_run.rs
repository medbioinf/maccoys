/// Represents an MS run and its content (e.g. the spectra that are part of the MS run)
#[derive(serde::Serialize, serde::Deserialize)]
pub struct MsRun {
    search_uuid: String,
    ms_run_name: String,
    spectra_ids: Vec<String>,
}

impl MsRun {
    pub fn new(search_uuid: String, ms_run_name: String, spectra_ids: Vec<String>) -> Self {
        Self {
            search_uuid,
            ms_run_name,
            spectra_ids,
        }
    }

    pub fn empty() -> Self {
        Self {
            search_uuid: String::new(),
            ms_run_name: String::new(),
            spectra_ids: Vec::with_capacity(0),
        }
    }

    pub fn get_search_uuid(&self) -> &str {
        &self.search_uuid
    }

    pub fn get_ms_run(&self) -> &str {
        &self.ms_run_name
    }

    pub fn get_spectra_ids(&self) -> &Vec<String> {
        &self.spectra_ids
    }

}