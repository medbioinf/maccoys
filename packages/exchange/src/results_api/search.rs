/// Represents a search and it content (e.g. the ms runs that are part of the search)
/// 
#[derive(serde::Serialize, serde::Deserialize)]
pub struct Search {
    search_uuid: String,
    ms_run_names: Vec<String>,
}

impl Search {
    pub fn new(search_uuid: String, ms_run_names: Vec<String>) -> Self {
        Self { search_uuid, ms_run_names }
    }

    pub fn empty() -> Self {
        Self {
            search_uuid: String::new(),
            ms_run_names: Vec::with_capacity(0),
        }
    }

    pub fn get_search_uuid(&self) -> &str {
        &self.search_uuid
    }

    pub fn get_ms_run_names(&self) -> &Vec<String> {
        &self.ms_run_names
    }
}