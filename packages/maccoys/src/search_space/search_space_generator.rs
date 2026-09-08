use std::cmp::min;
use std::collections::HashSet;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::Arc;

// 3rd party imports
use dihardts_omicstools::chemistry::amino_acid::{AminoAcid, CANONICAL_AMINO_ACIDS};
use dihardts_omicstools::proteomics::peptide::Terminus;
use dihardts_omicstools::proteomics::post_translational_modifications::Position as PtmPosition;
use fancy_regex::Regex;
use futures::{pin_mut, StreamExt};
use http::StatusCode;
use macpepdb::mass::to_int as mass_to_int;
use macpepdb::peptidoform_search_client::PeptidoformSearchClient;
use macpepdb::post_translational_modification::{PTMCollection, PostTranslationalModification};
use macpepdb::sequence::PeptideSequence;
use serde::{Deserialize, Serialize};
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
use crate::peptide_arena::PeptideArena;
use crate::search_space::decoy_cache::DecoyCache;
use crate::search_space::target_lookup::TargetLookup;

#[derive(Debug, Error)]
pub enum Error {
    #[error("MaCPepDB returned error in stream: {0}")]
    ApiStream(String),
    #[error("Regex error in search space generator: {0}")]
    CleavageRegex(Box<fancy_regex::Error>),
    #[error("Decoy cache error in search space generator: {0}")]
    DecoyCache(Box<crate::search_space::decoy_cache::Error>),
    #[error("Decoy generator error in search space generator: {0}")]
    DecoyGenerator(anyhow::Error),
    #[error("Unable to flush FASTA file: {0}")]
    FastaFlush(Box<std::io::Error>),
    #[error("Unable to write to FASTA file: {0}")]
    FastaWrite(Box<std::io::Error>),
    #[error("Error getting next peptide from HTTP stream: {0}")]
    NextFromHttpStream(Box<reqwest::Error>),
    #[error("Error getting next decoy from HTTP stream: {0}")]
    NextDecoyFromStream(anyhow::Error),
    #[error("Peptifoform search client error in search space generator: {0}")]
    PeptidoformSearchClient(Box<macpepdb::peptidoform_search_client::Error>),
    #[error("Post-translational modification error in search space generator: {0}")]
    PostTranslationalModification(Box<macpepdb::post_translational_modification::Error>),
    #[error("Error adding peptide to search space: {0}")]
    SearchSpaceAddition(String),
    #[error("Unable to send search request: {0}")]
    SearchRequest(Box<reqwest::Error>),
    #[error("Seqeunce error in search space generator: {0}")]
    Sequence(Box<macpepdb::sequence::Error>),
    #[error("target lookup error in search space generator: {0}")]
    TargetLookup(Box<crate::search_space::target_lookup::Error>),
    #[error(
        "Unsuccessful search request, status code: {0}, content type: {1}, response text: {2}"
    )]
    UnsuccessfullSearchRequest(StatusCode, String, String),
}

into_thiserror_boxed!(fancy_regex::Error, Error, CleavageRegex);
into_thiserror_boxed!(crate::search_space::decoy_cache::Error, Error, DecoyCache);
into_thiserror_boxed!(
    macpepdb::peptidoform_search_client::Error,
    Error,
    PeptidoformSearchClient
);
into_thiserror_boxed!(
    macpepdb::post_translational_modification::Error,
    Error,
    PostTranslationalModification
);
into_thiserror_boxed!(
    crate::search_space::target_lookup::Error,
    Error,
    TargetLookup
);

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
    ) -> impl std::future::Future<Output = Result<(), Error>> + Send;

    /// Adds a decoy peptide sequence to the search space
    ///
    /// # Arguments
    /// * `sequence` - Peptide sequence
    ///
    fn add_decoys(
        &mut self,
        seqeunce: String,
    ) -> impl std::future::Future<Output = Result<(), Error>> + Send;

    /// Returns the number of target peptides in the search space
    ///
    fn target_count(&self) -> usize;

    /// Returns the number of decoy peptides in the search space
    ///
    fn decoy_count(&self) -> usize;

    /// Finalizes the search space, e.g by flushing buffers
    ///
    fn done(&mut self) -> impl std::future::Future<Output = Result<(), Error>> + Send;
}

/// In-memory implementation of a search space
///
///
#[derive(Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct InMemorySearchSpace {
    targets: PeptideArena,
    decoys: PeptideArena,
}

impl InMemorySearchSpace {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.targets.len() + self.decoys.len()
    }

    pub fn is_empty(&self) -> bool {
        self.targets.is_empty() && self.decoys.is_empty()
    }

    pub fn into_single_peptide_arena(self) -> PeptideArena {
        let mut combined = PeptideArena::new();
        combined.merge(self.targets);
        combined.merge(self.decoys);
        combined
    }

    pub fn size(&self) -> usize {
        self.targets.size() + self.decoys.size()
    }
}

impl IsSearchSpace for InMemorySearchSpace {
    async fn add_target(&mut self, sequence: String) -> Result<(), Error> {
        self.targets.push(sequence);
        Ok(())
    }

    async fn add_decoys(&mut self, sequence: String) -> Result<(), Error> {
        self.decoys.push(sequence);
        Ok(())
    }

    fn target_count(&self) -> usize {
        self.targets.len()
    }

    fn decoy_count(&self) -> usize {
        self.decoys.len()
    }

    async fn done(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

#[allow(clippy::from_over_into)]
impl Into<(Vec<String>, Vec<String>)> for InMemorySearchSpace {
    fn into(self) -> (Vec<String>, Vec<String>) {
        (self.targets.into(), self.decoys.into())
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
    async fn add_target(&mut self, sequence: String) -> Result<(), Error> {
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
            .await
            .map_err(|err| Error::FastaWrite(Box::new(err)))?;
        self.target_ctr += 1;
        if self.target_ctr.is_multiple_of(1000) {
            self.fasta
                .flush()
                .await
                .map_err(|err| Error::FastaFlush(Box::new(err)))?;
        }
        Ok(())
    }

    async fn add_decoys(&mut self, sequence: String) -> Result<(), Error> {
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
            .await
            .map_err(|err| Error::FastaWrite(Box::new(err)))?;
        self.decoy_ctr += 1;
        if self.decoy_ctr.is_multiple_of(1000) {
            self.fasta
                .flush()
                .await
                .map_err(|err| Error::FastaFlush(Box::new(err)))?;
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
    async fn done(&mut self) -> Result<(), Error> {
        self.fasta
            .flush()
            .await
            .map_err(|err| Error::FastaFlush(Box::new(err)))?;
        Ok(())
    }
}

/// Generator for the search space
///
pub struct SearchSpaceGenerator {
    target_client: PeptidoformSearchClient,
    decoy_client: Option<PeptidoformSearchClient>,
    target_lookup_url: Option<String>,
    decoy_cache_url: Option<String>,
}

impl SearchSpaceGenerator {
    pub async fn new(
        target_url: &str,
        decoy_url: Option<String>,
        target_lookup_url: Option<String>,
        decoy_cache_url: Option<String>,
    ) -> Result<Self, Error> {
        let target_client = PeptidoformSearchClient::try_from_url(target_url).await?;
        let decoy_client = match decoy_url.as_ref() {
            Some(url) => Some(PeptidoformSearchClient::try_from_url(url).await?),
            None => None,
        };

        Ok(Self {
            target_client,
            decoy_client,
            target_lookup_url,
            decoy_cache_url,
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
        taxonomy_id: Option<i32>,
        is_reviewed: Option<bool>,
        ptms: Arc<PTMCollection<Arc<PostTranslationalModification>>>,
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

        let mut peptidoform_stream = client
            .search(
                mass,
                lower_mass_tolerance_ppm,
                upper_mass_tolerance_ppm,
                max_variable_modifications,
                taxonomy_id,
                is_reviewed,
                ptms,
                true,
                concurrent_searches,
            )
            .await
            .map_err(|err| Error::PeptidoformSearchClient(Box::new(err)))?;

        while let Some(peptidoform_result) = peptidoform_stream.next().await {
            let peptidoform =
                peptidoform_result.map_err(|err| Error::PeptidoformSearchClient(Box::new(err)))?;

            if is_target {
                search_space
                    .add_target(peptidoform)
                    .await
                    .map_err(|err| Error::SearchSpaceAddition(err.to_string()))?;
            } else {
                search_space
                    .add_decoys(peptidoform)
                    .await
                    .map_err(|err| Error::SearchSpaceAddition(err.to_string()))?;
            }
            if let Some(limit) = limit {
                if search_space.target_count() >= limit {
                    break;
                }
            }
        }

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
        ptms: Arc<PTMCollection<Arc<PostTranslationalModification>>>,
    ) -> Result<(), Error>
    where
        T: IsSearchSpace,
    {
        let target_lookup: Option<TargetLookup> = match self.target_lookup_url {
            Some(ref url) => Some(TargetLookup::try_from_url(url).await?),
            None => None,
        };

        let decoy_cache: Option<DecoyCache> = match self.decoy_cache_url {
            Some(ref url) => Some(DecoyCache::try_from_url(url).await?),
            None => None,
        };

        let statically_modified_amino_acids = ptms
            .get_static_ptms()
            .iter()
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
            ptms.all()
                .iter()
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

        let static_n_terminus_modification = match ptms
            .all()
            .iter()
            .find(|ptm| ptm.is_n_bond() && ptm.is_static())
        {
            Some(ptm) => ptm.mass_delta(),
            None => 0,
        };

        let static_c_terminus_modification = match ptms
            .all()
            .iter()
            .find(|ptm| ptm.is_c_bond() && ptm.is_static())
        {
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
        .await
        .map_err(Error::DecoyGenerator)?;
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
            .await
            .map_err(Error::DecoyGenerator)?;

        pin_mut!(decoy_stream);

        while let Some(decoy) = decoy_stream.next().await {
            let sequence = decoy.map_err(Error::NextDecoyFromStream)?;
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
        ptms: Arc<PTMCollection<Arc<PostTranslationalModification>>>,
        decoys_per_peptide: usize,
        concurrent_searches: NonZeroUsize,
    ) -> Result<(), Error>
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
                    ptms.clone(),
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
                        None,
                        None,
                        ptms.clone(),
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
                            ptms.clone(),
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
