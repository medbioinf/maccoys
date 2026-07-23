use std::cmp::min;
use std::collections::HashSet;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::Arc;

// 3rd party imports
use anyhow::Result;
use dihardts_omicstools::chemistry::amino_acid::{AminoAcid, CANONICAL_AMINO_ACIDS};
use dihardts_omicstools::proteomics::peptide::Terminus;
use dihardts_omicstools::proteomics::post_translational_modifications::{
    ModificationType as PtmType, Position as PtmPosition,
};
use fancy_regex::Regex;
use futures::{pin_mut, StreamExt};
use http::StatusCode;
use macpepdb::mass::{to_float as mass_to_float, to_int as mass_to_int};
use macpepdb::peptide_search::{MultiTaskSearch, PeptideSearchType, Search, UnionAllSearch};
use macpepdb::post_translational_modification::{PTMCollection, PostTranslationalModification};
use macpepdb::sequence::PeptideSequence;
use macpepdb_web_common::requests::peptide::{SearchRequestBody, SearchRequestMass};
use macpepdb_web_common::requests::ptm::{
    PostTranslationalModificationRequest, PtmPosition as RequestPtmPosition,
    PtmType as RequestPtmType,
};
use thiserror::Error;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tracing::error;

// internal imports
use super::decoy_generator::DecoyGenerator;
use super::decoy_part::DecoyPart;
use crate::constants::FASTA_SEQUENCE_LINE_LENGTH;
use crate::constants::{
    FASTA_DECOY_ENTRY_NAME_PREFIX, FASTA_DECOY_ENTRY_PREFIX, FASTA_TARGET_ENTRY_NAME_PREFIX,
    FASTA_TARGET_ENTRY_PREFIX,
};
use crate::search_space::decoy_cache::DecoyCache;
use crate::search_space::macpepdb_client::MaCPepDBClient;
use crate::search_space::target_lookup::TargetLookup;

static HTTP_BUFFER_SIZE: usize = 1024 * 1024 * 10; // 10 MiB

#[derive(Debug, Error)]
pub enum Error {
    #[error("MaCPepDB returned error in stream: {0}")]
    ApiStream(String),
    #[error("Error getting next peptide from HTTP stream: {0}")]
    NextFromHttpStream(Box<reqwest::Error>),
    #[error("Peptide search error in search space generator: {0}")]
    PeptideSearch(Box<macpepdb::peptide_search::Error>),
    #[error("Post-translational modification error in search space generator: {0}")]
    PostTranslationalModification(Box<macpepdb::post_translational_modification::Error>),
    #[error("Error adding peptide to search space: {0}")]
    SearchSpaceAddition(String),
    #[error("Unable to send search request: {0}")]
    SearchRequest(Box<reqwest::Error>),
    #[error("Seqeunce error in search space generator: {0}")]
    Sequence(Box<macpepdb::sequence::Error>),
    #[error(
        "Unsuccessful search request, status code: {0}, content type: {1}, response text: {2}"
    )]
    UnsuccessfullSearchRequest(StatusCode, String, String),
}

/// Path to the peptide search endpoint of MaCPepDB
///
const PEPTIDE_SEARCH_PATH: &str = "/api/peptides/search";

/// Trait defining a search space to accumulate target and decoy peptides
///
pub trait IsSearchSpace {
    /// Adds a target peptide sequence to the search space
    ///
    /// # Arguments
    /// * `sequence` - Peptide sequence
    ///
    fn add_target(
        &mut self,
        seqeunce: String,
    ) -> impl std::future::Future<Output = Result<()>> + Send;

    /// Adds a decoy peptide sequence to the search space
    ///
    /// # Arguments
    /// * `sequence` - Peptide sequence
    ///
    fn add_decoys(
        &mut self,
        seqeunce: String,
    ) -> impl std::future::Future<Output = Result<()>> + Send;

    /// Returns the number of target peptides in the search space
    ///
    fn target_count(&self) -> usize;

    /// Returns the number of decoy peptides in the search space
    ///
    fn decoy_count(&self) -> usize;

    /// Finalizes the search space, e.g by flushing buffers
    ///
    fn done(&mut self) -> impl std::future::Future<Output = Result<()>> + Send;
}

/// In-memory implementation of a search space
///
///
#[derive(Default)]
pub struct InMemorySearchSpace {
    targets: Vec<String>,
    decoys: Vec<String>,
}

impl InMemorySearchSpace {
    pub fn new() -> Self {
        Self::default()
    }
}

impl IsSearchSpace for InMemorySearchSpace {
    async fn add_target(&mut self, sequence: String) -> Result<()> {
        self.targets.push(sequence);
        Ok(())
    }

    async fn add_decoys(&mut self, sequence: String) -> Result<()> {
        self.decoys.push(sequence);
        Ok(())
    }

    fn target_count(&self) -> usize {
        self.targets.len()
    }

    fn decoy_count(&self) -> usize {
        self.decoys.len()
    }

    async fn done(&mut self) -> Result<()> {
        Ok(())
    }
}

#[allow(clippy::from_over_into)]
impl Into<(Vec<String>, Vec<String>)> for InMemorySearchSpace {
    fn into(self) -> (Vec<String>, Vec<String>) {
        (self.targets, self.decoys)
    }
}

/// FASTA implementation of a search space
/// Writes target and decoy peptides to a FASTA file
///
pub struct FastaSearchSpace {
    fasta: Pin<Box<dyn AsyncWrite + Send>>,
    target_ctr: usize,
    decoy_ctr: usize,
}

impl FastaSearchSpace {
    /// Creates a new FastaSearchSpace
    ///
    /// # Arguments
    /// * `fasta` - AsyncWrite to write the FASTA entries to
    pub fn new(fasta: Pin<Box<dyn AsyncWrite + Send>>) -> Self {
        Self {
            fasta,
            target_ctr: 0,
            decoy_ctr: 0,
        }
    }

    /// Creates a FASTA entry for the given sequence.
    ///
    /// # Arguments
    /// * `sequence` - Sequence
    /// * `index` - Sequence index
    /// * `entry_prefix` - Entry prefix
    /// * `entry_name_prefix` - Entry name prefix
    ///
    fn gen_fasta_entry(
        sequence: &str,
        index: usize,
        entry_prefix: &str,
        entry_name_prefix: &str,
    ) -> String {
        let mut entry = format!(
            ">{}|{entry_name_prefix}{index}|{entry_name_prefix}{index}\n",
            entry_prefix,
            entry_name_prefix = entry_name_prefix,
            index = index
        );
        for start in (0..sequence.len()).step_by(FASTA_SEQUENCE_LINE_LENGTH) {
            let stop = min(start + FASTA_SEQUENCE_LINE_LENGTH, sequence.len());
            entry.push_str(&sequence[start..stop]);
            entry.push('\n');
        }
        entry
    }
}

impl IsSearchSpace for FastaSearchSpace {
    async fn add_target(&mut self, sequence: String) -> Result<()> {
        self.fasta
            .write_all(
                Self::gen_fasta_entry(
                    sequence.as_str(),
                    self.target_ctr + 1,
                    FASTA_TARGET_ENTRY_PREFIX,
                    FASTA_TARGET_ENTRY_NAME_PREFIX,
                )
                .as_bytes(),
            )
            .await?;
        self.target_ctr += 1;
        if self.target_ctr.is_multiple_of(1000) {
            self.fasta.flush().await?;
        }
        Ok(())
    }

    async fn add_decoys(&mut self, sequence: String) -> Result<()> {
        self.fasta
            .write_all(
                Self::gen_fasta_entry(
                    sequence.as_str(),
                    self.decoy_ctr + 1,
                    FASTA_DECOY_ENTRY_PREFIX,
                    FASTA_DECOY_ENTRY_NAME_PREFIX,
                )
                .as_bytes(),
            )
            .await?;
        self.decoy_ctr += 1;
        if self.decoy_ctr.is_multiple_of(1000) {
            self.fasta.flush().await?;
        }
        Ok(())
    }

    fn target_count(&self) -> usize {
        self.target_ctr
    }

    fn decoy_count(&self) -> usize {
        self.decoy_ctr
    }

    /// This flushs the underlying writer
    ///
    async fn done(&mut self) -> Result<()> {
        self.fasta.flush().await?;
        Ok(())
    }
}

/// Generator for the search space
///
pub struct SearchSpaceGenerator {
    target_url: String,
    decoy_url: Option<String>,
    target_client: MaCPepDBClient,
    decoy_client: Option<MaCPepDBClient>,
    target_lookup_url: Option<String>,
    decoy_cache_url: Option<String>,
}

impl SearchSpaceGenerator {
    pub async fn new(
        target_url: &str,
        search_type: PeptideSearchType,
        decoy_url: Option<String>,
        target_lookup_url: Option<String>,
        decoy_cache_url: Option<String>,
    ) -> Result<Self> {
        let target_client = MaCPepDBClient::from_url(target_url, search_type).await?;
        let decoy_client = match decoy_url.as_ref() {
            Some(url) => Some(MaCPepDBClient::from_url(url, search_type).await?),
            None => None,
        };

        let target_url = target_url.to_owned();

        Ok(Self {
            target_client,
            decoy_client,
            target_lookup_url,
            decoy_cache_url,
            decoy_url,
            target_url: target_url.to_owned(),
        })
    }

    /// Add peptides to the FASTA file adn returns the number of added peptides.
    /// If decoys should be added but decoy_client is None, the 0 is returned.
    ///
    /// # Arguments
    /// * `is_target` - Whether the peptides are target or decoy peptides
    /// * `search_space` - IsSearchSpace to write the peptides tos
    /// * `mass` - The mass of the peptides
    /// * `lower_mass_tolerance` - The lower mass tolerance
    /// * `upper_mass_tolerance` - The upper mass tolerance
    /// * `max_variable_modifications` - The maximum number of variable modifications
    /// * `taxonomy_id` - The taxonomy id of the peptides
    /// * `proteome_id` - The proteome id of the peptides
    /// * `is_reviewed` - Whether the peptides are reviewed or not
    /// * `ptms` - The post-translational modifications of the peptides
    /// * `limit` - The maximum number of peptides to add
    ///
    #[allow(clippy::too_many_arguments)]
    async fn add_peptides<T>(
        &self,
        is_target: bool,
        search_space: &mut T,
        mass: i64,
        lower_mass_tolerance_ppm: i64,
        upper_mass_tolerance_ppm: i64,
        max_variable_modifications: usize,
        taxonomy_ids: Option<Vec<i32>>,
        proteome_id: Option<String>,
        is_reviewed: Option<bool>,
        ptms: &[Arc<PostTranslationalModification>],
        concurrent_searches: NonZeroUsize,
        limit: Option<usize>,
    ) -> Result<(), Error>
    where
        T: IsSearchSpace,
    {
        // Determine the client
        let client = if is_target {
            &self.target_client
        } else {
            match self.decoy_client.as_ref() {
                Some(client) => client,
                None => return Ok(()),
            }
        };

        // Get URL for HTTP client
        let http_url = match client {
            MaCPepDBClient::Db(..) => "".to_string(),
            MaCPepDBClient::Http(_) => {
                let base_url = if is_target {
                    &self.target_url
                } else {
                    self.decoy_url.as_ref().unwrap()
                };
                format!("{}{}", base_url, PEPTIDE_SEARCH_PATH)
            }
        };

        match client {
            MaCPepDBClient::Db(ref client, configuration, search_type) => {
                let ptm_collection = PTMCollection::new(ptms.iter().cloned())
                    .map(Arc::new)
                    .map_err(|err| Error::PostTranslationalModification(Box::new(err)))?;
                let target_stream = match search_type {
                    PeptideSearchType::UnionAll => UnionAllSearch::search(
                        client.clone(),
                        configuration.clone(),
                        mass,
                        lower_mass_tolerance_ppm,
                        upper_mass_tolerance_ppm,
                        max_variable_modifications,
                        true,
                        taxonomy_ids,
                        proteome_id.map(|proteome_id| vec![proteome_id]),
                        is_reviewed,
                        ptm_collection,
                        true,
                        concurrent_searches,
                    )
                    .await
                    .map_err(|err| Error::PeptideSearch(Box::new(err)))?,
                    PeptideSearchType::MultiTask => MultiTaskSearch::search(
                        client.clone(),
                        configuration.clone(),
                        mass,
                        lower_mass_tolerance_ppm,
                        upper_mass_tolerance_ppm,
                        max_variable_modifications,
                        true,
                        taxonomy_ids,
                        proteome_id.map(|proteome_id| vec![proteome_id]),
                        is_reviewed,
                        ptm_collection.clone(),
                        true,
                        concurrent_searches,
                    )
                    .await
                    .map_err(|err| Error::PeptideSearch(Box::new(err)))?,
                };

                pin_mut!(target_stream);
                while let Some(peptide) = target_stream.next().await {
                    let peptidoforms =
                        peptide.map_err(|err| Error::PeptideSearch(Box::new(err)))?;

                    for peptidoform in peptidoforms {
                        if is_target {
                            search_space
                                .add_target(peptidoform.sequence().to_string())
                                .await
                                .map_err(|err| Error::SearchSpaceAddition(err.to_string()))?;
                        } else {
                            search_space
                                .add_decoys(peptidoform.sequence().to_string())
                                .await
                                .map_err(|err| Error::SearchSpaceAddition(err.to_string()))?;
                        }
                    }
                    if let Some(limit) = limit {
                        if search_space.target_count() >= limit {
                            break;
                        }
                    }
                }
            }
            MaCPepDBClient::Http(ref client) => {
                let modifications = ptms
                    .iter()
                    .map(|ptm| PostTranslationalModificationRequest {
                        name: ptm.name().to_string(),
                        amino_acid: ptm.amino_acid().code(),
                        mass_delta: mass_to_float(ptm.mass_delta()),
                        mod_type: match ptm.mod_type() {
                            PtmType::Static => RequestPtmType::Static,
                            PtmType::Variable => RequestPtmType::Variable,
                        },
                        position: match ptm.position() {
                            PtmPosition::Anywhere => RequestPtmPosition::Anywhere,
                            PtmPosition::Terminus(Terminus::N) => RequestPtmPosition::NTerminus,
                            PtmPosition::Terminus(Terminus::C) => RequestPtmPosition::CTerminus,
                            PtmPosition::Bond(Terminus::N) => RequestPtmPosition::NBond,
                            PtmPosition::Bond(Terminus::C) => RequestPtmPosition::CBond,
                        },
                    })
                    .collect::<Vec<_>>();

                let request_body = SearchRequestBody {
                    modifications,
                    lower_mass_tolerance_ppm,
                    upper_mass_tolerance_ppm,
                    max_variable_modifications,
                    proteome_id,
                    is_reviewed,
                    mass: SearchRequestMass::Dalton(mass_to_float(mass)),
                    taxonomy_id: taxonomy_ids.and_then(|ids| ids.first().cloned()),
                    resolve_modifications: Some(true),
                };

                let response = client
                    .post(http_url.as_str())
                    .header("Accept", "text/plain")
                    .json(&request_body)
                    .send()
                    .await
                    .map_err(|err| Error::SearchRequest(Box::new(err)))?;

                if !response.status().is_success() {
                    let status_code = response.status();
                    let content_type = response
                        .headers()
                        .get("content-type")
                        .and_then(|ct| ct.to_str().ok())
                        .unwrap_or("unknown content type")
                        .to_string();
                    let response_text = response
                        .text()
                        .await
                        .unwrap_or("text is not decodable".to_string());
                    return Err(Error::UnsuccessfullSearchRequest(
                        status_code,
                        content_type,
                        response_text,
                    ));
                }
                let peptide_stream = response.bytes_stream().peekable();
                pin_mut!(peptide_stream);
                let mut buffer = Vec::with_capacity(HTTP_BUFFER_SIZE); // 10 MiB
                while let Some(chunk) = peptide_stream.as_mut().next().await {
                    let chunk = chunk.map_err(|err| Error::NextFromHttpStream(Box::new(err)))?;
                    buffer.extend_from_slice(&chunk);

                    if let Some(Ok(next)) = peptide_stream.as_mut().peek().await {
                        if buffer.len() + next.len() > HTTP_BUFFER_SIZE {
                            Self::http_buffer_to_search_space(
                                search_space,
                                &mut buffer,
                                is_target,
                                limit,
                            )
                            .await?;
                        }
                    }
                }
                if !buffer.is_empty() {
                    Self::http_buffer_to_search_space(search_space, &mut buffer, is_target, limit)
                        .await?;
                }
            }
        }
        Ok(())
    }

    async fn http_buffer_to_search_space<T>(
        search_space: &mut T,
        buffer: &mut Vec<u8>,
        is_target: bool,
        limit: Option<usize>,
    ) -> Result<(), Error>
    where
        T: IsSearchSpace,
    {
        let mut last_newline_pos = 0;
        for newline_pos in memchr::memchr_iter(b'\n', buffer) {
            let sequence =
                String::from_utf8_lossy(&buffer[last_newline_pos..newline_pos]).to_string();

            if sequence.starts_with("!!!") {
                return Err(Error::ApiStream(sequence));
            }

            if is_target {
                search_space
                    .add_target(sequence)
                    .await
                    .map_err(|err| Error::SearchSpaceAddition(err.to_string()))?;
            } else {
                search_space
                    .add_decoys(sequence)
                    .await
                    .map_err(|err| Error::SearchSpaceAddition(err.to_string()))?;
            }
            last_newline_pos = newline_pos + 1;
            if let Some(limit) = limit {
                if search_space.target_count() >= limit {
                    break;
                }
            }
        }
        drop(buffer.drain(..last_newline_pos));

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn generate_missing_decoys<T>(
        &self,
        fasta: &mut T,
        needed_decoys: usize,
        mass: i64,
        lower_mass_tolerance_ppm: i64,
        upper_mass_tolerance_ppm: i64,
        max_variable_modifications: usize,
        ptms: &[Arc<PostTranslationalModification>],
    ) -> Result<()>
    where
        T: IsSearchSpace,
    {
        let target_lookup: Option<TargetLookup> = match self.target_lookup_url {
            Some(ref url) => Some(TargetLookup::new(url).await?),
            None => None,
        };

        let decoy_cache: Option<DecoyCache> = match self.decoy_cache_url {
            Some(ref url) => Some(DecoyCache::new(url).await?),
            None => None,
        };

        let statically_modified_amino_acids = ptms
            .iter()
            .filter(|ptm| ptm.is_static())
            .map(|ptm| ptm.amino_acid().code())
            .collect::<HashSet<char>>();

        let mut decoy_part_ctr: usize = 0;
        // First add all amino acids which are not statically modified
        let mut decoy_parts = CANONICAL_AMINO_ACIDS
            .iter()
            .filter(|amino_acid| !statically_modified_amino_acids.contains(amino_acid.get_code()))
            .map(|amino_acid| {
                let part = DecoyPart::new(
                    decoy_part_ctr,
                    *amino_acid.get_code(),
                    mass_to_int(*amino_acid.get_mono_mass()), // need to convert it here as we use the amino acid representation from omicstools, not from MaCPepDB
                    None,
                    PtmPosition::Anywhere, // Does not matter as modification_type is None
                    0,
                );
                decoy_part_ctr += 1;
                part
            })
            .collect::<Vec<DecoyPart>>();

        // Then add all statically modified amino acids
        decoy_parts.extend(
            ptms.iter()
                .filter(|ptm| ptm.is_anywhere() && ptm.is_static())
                .map(|ptm| {
                    let part = DecoyPart::new(
                        decoy_part_ctr,
                        ptm.amino_acid().code(),
                        ptm.amino_acid().mono_mass(),
                        Some(ptm.mod_type().clone()),
                        ptm.position().clone(),
                        ptm.mass_delta(),
                    );
                    decoy_part_ctr += 1;
                    part
                }),
        );

        let static_n_terminus_modification =
            match ptms.iter().find(|ptm| ptm.is_n_bond() && ptm.is_static()) {
                Some(ptm) => ptm.mass_delta(),
                None => 0,
            };

        let static_c_terminus_modification =
            match ptms.iter().find(|ptm| ptm.is_c_bond() && ptm.is_static()) {
                Some(ptm) => ptm.mass_delta(),
                None => 0,
            };

        let decoy_gen = DecoyGenerator::new(
            Terminus::C,
            vec!['K', 'R'],
            Regex::new(r"(?<=[KR])(?!(P|$))")?,
            decoy_parts,
            vec![
                None,
                Some(0),
                Some(0),
                Some(0),
                Some(0),
                Some(0),
                Some(1),
                Some(1),
                Some(1),
                Some(1),
                Some(1),
            ],
            static_n_terminus_modification,
            static_c_terminus_modification,
        )
        .await?;
        let decoy_stream = decoy_gen
            .stream(
                mass,
                lower_mass_tolerance_ppm,
                upper_mass_tolerance_ppm,
                6,
                50,
                max_variable_modifications,
                3,
                180.0,
                false,
            )
            .await?;

        pin_mut!(decoy_stream);

        while let Some(decoy) = decoy_stream.next().await {
            let sequence = decoy?;
            if let Some(ref target_lookup) = target_lookup {
                if target_lookup.is_target(&sequence).await? {
                    continue;
                }
            }
            if let Some(ref decoy_cache) = decoy_cache {
                let seqeunce = PeptideSequence::try_from(sequence.as_str())
                    .map_err(|err| Error::Sequence(Box::new(err)))?;
                decoy_cache.cache(vec![(seqeunce, 0)]).await?;
            }
            fasta.add_decoys(sequence).await?;
            if fasta.decoy_count() == needed_decoys {
                break;
            }
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn create<T>(
        &self,
        fasta: &mut T,
        mass: i64,
        lower_mass_tolerance: i64,
        upper_mass_tolerance: i64,
        max_variable_modifications: usize,
        ptms: &[Arc<PostTranslationalModification>],
        decoys_per_peptide: usize,
        concurrent_searches: NonZeroUsize,
    ) -> Result<()>
    where
        T: IsSearchSpace,
    {
        loop {
            let target_result = self
                .add_peptides(
                    true,
                    fasta,
                    mass,
                    lower_mass_tolerance,
                    upper_mass_tolerance,
                    max_variable_modifications,
                    None,
                    None,
                    None,
                    ptms,
                    concurrent_searches,
                    None,
                )
                .await;
            match target_result {
                Ok(_) => {
                    break;
                }
                Err(err) => {
                    error!("Error adding target peptides: `{}, retry... ", err);
                    continue;
                }
            }
        }

        let needed_decoys = decoys_per_peptide * fasta.target_count();

        if needed_decoys > 0 {
            loop {
                // Fetch decoys from cache if possible
                //
                let cached_decoy_result = self
                    .add_peptides(
                        false,
                        fasta,
                        mass,
                        lower_mass_tolerance,
                        upper_mass_tolerance,
                        max_variable_modifications,
                        None, // Not applicable for decoys
                        None, // Not applicable for decoys
                        None, // Not applicable for decoys
                        ptms,
                        concurrent_searches,
                        Some(needed_decoys),
                    )
                    .await;

                if let Err(err) = cached_decoy_result {
                    error!("Error adding cached decoy peptides: `{}, retry... ", err);
                    continue;
                }

                if fasta.decoy_count() < needed_decoys {
                    let decoy_result = self
                        .generate_missing_decoys(
                            fasta,
                            needed_decoys,
                            mass,
                            lower_mass_tolerance,
                            upper_mass_tolerance,
                            max_variable_modifications,
                            ptms,
                        )
                        .await;

                    match decoy_result {
                        Ok(_) => {
                            break;
                        }
                        Err(err) => {
                            error!("Error adding missing decoy peptides: `{}, retry... ", err);
                            continue;
                        }
                    }
                }
            }
        }

        fasta.done().await?;
        Ok(())
    }
}
