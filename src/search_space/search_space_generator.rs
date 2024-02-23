// std imports
use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;

// 3rd party imports
use anyhow::{anyhow, Result};
use dihardts_omicstools::chemistry::amino_acid::{AminoAcid, CANONICAL_AMINO_ACIDS};
use dihardts_omicstools::proteomics::peptide::Terminus;
use dihardts_omicstools::proteomics::post_translational_modifications::{
    Position as PtmPosition, PostTranslationalModification as PTM,
};
use fancy_regex::Regex;
use futures::{pin_mut, StreamExt};
use macpepdb::database::generic_client::GenericClient;
use macpepdb::{
    database::{
        configuration_table::ConfigurationTable as ConfigurationTableTrait,
        scylla::{
            client::Client as DbClient, configuration_table::ConfigurationTable,
            peptide_table::PeptideTable,
        },
    },
    mass::convert::{to_float as mass_to_float, to_int as mass_to_int},
};
use reqwest::Client as HttpClient;
use serde_json::json;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

// internal imports
use super::decoy_generator::DecoyGenerator;
use super::decoy_part::DecoyPart;
use crate::search_space::decoy_cache::DecoyCache;
use crate::search_space::target_lookup::TargetLookup;
use crate::{
    constants::{
        FASTA_DECOY_ENTRY_NAME_PREFIX, FASTA_DECOY_ENTRY_PREFIX, FASTA_TARGET_ENTRY_NAME_PREFIX,
        FASTA_TARGET_ENTRY_PREFIX,
    },
    functions::gen_fasta_entry,
};

/// Path to the peptide search endpoint of MaCPepDB
///
const PEPTIDE_SEARCH_PATH: &str = "/api/peptides/search";

/// Client for either a MaCPepDB database or a MaCPepDB web API
///
enum Client {
    DbClient(Arc<DbClient>),
    HttpClient(HttpClient),
}

/// Generator for the search space
///
pub struct SearchSpaceGenerator {
    target_url: String,
    decoy_url: Option<String>,
    target_client: Client,
    decoy_client: Option<Client>,
    target_lookup_url: Option<String>,
    decoy_cache_url: Option<String>,
}

impl SearchSpaceGenerator {
    pub async fn new(
        target_url: &str,
        decoy_url: Option<String>,
        target_lookup_url: Option<String>,
        decoy_cache_url: Option<String>,
    ) -> Result<Self> {
        let target_client = Self::create_client(target_url).await?;
        let decoy_client = match decoy_url.as_ref() {
            Some(url) => Some(Self::create_client(url).await?),
            None => None,
        };

        let target_url = target_url.to_owned();

        Ok(Self {
            target_client,
            decoy_client,
            target_lookup_url,
            decoy_cache_url,
            target_url: target_url.to_owned(),
            decoy_url: decoy_url,
        })
    }

    async fn create_client(url: &str) -> Result<Client> {
        if url.starts_with("http") {
            Ok(Client::HttpClient(HttpClient::new()))
        } else if url.starts_with("scylla") {
            Ok(Client::DbClient(Arc::new(DbClient::new(url).await?)))
        } else {
            Err(anyhow!("Invalid protocol in URL: {}", url))
        }
    }

    /// Add peptides to the FASTA file adn returns the number of added peptides.
    /// If decoys should be added but decoy_client is None, the 0 is returned.
    ///
    /// # Arguments
    /// * `is_target` - Whether the peptides are target or decoy peptides
    /// * `fasta_file` - The FASTA file to write the peptides to
    /// * `mass` - The mass of the peptides
    /// * `lower_mass_tolerance` - The lower mass tolerance
    /// * `upper_mass_tolerance` - The upper mass tolerance
    /// * `max_variable_modifications` - The maximum number of variable modifications
    /// * `taxonomy_id` - The taxonomy id of the peptides
    /// * `proteome_id` - The proteome id of the peptides
    /// * `is_reviewed` - Whether the peptides are reviewed or not
    /// * `ptms` - The post-translational modifications of the peptides
    ///
    async fn add_peptides(
        &self,
        is_target: bool,
        fasta_file: &mut File,
        mass: i64,
        lower_mass_tolerance_ppm: i64,
        upper_mass_tolerance_ppm: i64,
        max_variable_modifications: i16,
        taxonomy_ids: Option<Vec<i64>>,
        proteome_id: Option<String>,
        is_reviewed: Option<bool>,
        ptms: &Vec<PTM>,
        limit: Option<usize>,
    ) -> Result<usize> {
        // Determine the client
        let client = if is_target {
            &self.target_client
        } else {
            match self.decoy_client.as_ref() {
                Some(client) => client,
                None => return Ok(0),
            }
        };

        // Determine the entry prefix and entry name prefix
        let (entry_prefix, entry_name_prefix) = if is_target {
            (FASTA_TARGET_ENTRY_PREFIX, FASTA_TARGET_ENTRY_NAME_PREFIX)
        } else {
            (FASTA_DECOY_ENTRY_PREFIX, FASTA_DECOY_ENTRY_NAME_PREFIX)
        };

        // Get URL for HTTP client
        let http_url = match client {
            Client::DbClient(_) => "".to_string(),
            Client::HttpClient(_) => {
                let base_url = if is_target {
                    &self.target_url
                } else {
                    self.decoy_url.as_ref().unwrap()
                };
                format!("{}{}", base_url, PEPTIDE_SEARCH_PATH)
            }
        };

        let mut peptide_ctr: usize = 0;
        match client {
            Client::DbClient(ref client) => {
                let configuration = ConfigurationTable::select(client.as_ref()).await?;
                let target_stream = PeptideTable::search(
                    client.clone(),
                    Arc::new(configuration),
                    mass,
                    lower_mass_tolerance_ppm,
                    upper_mass_tolerance_ppm,
                    max_variable_modifications,
                    taxonomy_ids,
                    proteome_id,
                    is_reviewed,
                    ptms.clone(),
                )
                .await?;
                pin_mut!(target_stream);
                while let Some(peptide) = target_stream.next().await {
                    fasta_file
                        .write(
                            gen_fasta_entry(
                                peptide?.get_sequence(),
                                peptide_ctr,
                                entry_prefix,
                                entry_name_prefix,
                            )
                            .as_bytes(),
                        )
                        .await?;
                    peptide_ctr += 1;
                    if peptide_ctr % 1000 == 0 {
                        fasta_file.flush().await?;
                    }
                    if let Some(limit) = limit {
                        if peptide_ctr >= limit {
                            break;
                        }
                    }
                }
            }
            Client::HttpClient(ref client) => {
                let mut peptide_stream = client
                    .post(http_url.as_str())
                    .header("Connection", "close")
                    .header("Accept", "text/plain")
                    .json(&json!({
                        "mass": mass_to_float(mass),
                        "lower_mass_tolerance_ppm": lower_mass_tolerance_ppm,
                        "upper_mass_tolerance_ppm": upper_mass_tolerance_ppm,
                        "max_variable_modifications": max_variable_modifications,
                        "modifications": ptms.iter().map(|ptm|
                            (
                                *ptm.get_amino_acid().get_code(),
                                *ptm.get_mass_delta(),
                                ptm.get_mod_type().to_string(),
                                ptm.get_position().to_string()
                            )
                        ).collect::<Vec<(char, f64, String, String)>>(),
                    }))
                    .send()
                    .await?
                    .bytes_stream();
                let mut buffer = Vec::with_capacity(1024);
                while let Some(chunk) = peptide_stream.next().await {
                    chunk?.iter().for_each(|byte| buffer.push(*byte));
                    if let Some(newline_pos) = buffer.iter().position(|byte| *byte == b'\n') {
                        fasta_file
                            .write(
                                gen_fasta_entry(
                                    std::str::from_utf8(&buffer[..newline_pos])?,
                                    peptide_ctr,
                                    entry_prefix,
                                    entry_name_prefix,
                                )
                                .as_bytes(),
                            )
                            .await?;
                        peptide_ctr += 1;
                        if peptide_ctr % 1000 == 0 {
                            fasta_file.flush().await?;
                        }
                        if let Some(limit) = limit {
                            if peptide_ctr >= limit {
                                break;
                            }
                        }
                        // remove the written sequence from the buffer
                        buffer = buffer[newline_pos + 1..].to_vec();
                    }
                }
                // write the rest of the buffer if needed
                if let Some(limit) = limit {
                    if peptide_ctr < limit {
                        let sequence = std::str::from_utf8(&buffer)?.trim().to_string();
                        if sequence.len() > 0 {
                            fasta_file
                                .write(
                                    gen_fasta_entry(
                                        std::str::from_utf8(&buffer).unwrap(),
                                        peptide_ctr,
                                        entry_prefix,
                                        entry_name_prefix,
                                    )
                                    .as_bytes(),
                                )
                                .await?;
                            peptide_ctr += 1;
                        }
                    }
                }
            }
        }
        Ok(peptide_ctr)
    }

    async fn generate_missing_decoys(
        &self,
        fasta_file: &mut File,
        needed_decoys: usize,
        decoy_ctr: usize,
        mass: i64,
        lower_mass_tolerance_ppm: i64,
        upper_mass_tolerance_ppm: i64,
        max_variable_modifications: i8,
        ptms: Vec<PTM>,
    ) -> Result<usize> {
        let target_lookup: Option<TargetLookup> = match self.target_lookup_url {
            Some(ref url) => Some(TargetLookup::new(url).await?),
            None => None,
        };

        let decoy_cache: Option<DecoyCache> = match self.decoy_cache_url {
            Some(ref url) => Some(DecoyCache::new(url).await?),
            None => None,
        };

        let mut decoy_ctr = decoy_ctr;

        let statically_modified_amino_acids = ptms
            .iter()
            .filter(|ptm| ptm.is_static())
            .map(|ptm| *ptm.get_amino_acid().get_code())
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
                        *ptm.get_amino_acid().get_code(),
                        mass_to_int(*ptm.get_amino_acid().get_mono_mass()), // need to convert it here as we use the amino acid representation from omicstools, not from MaCPepDB
                        Some(ptm.get_mod_type().clone()),
                        ptm.get_position().clone(),
                        mass_to_int(*ptm.get_mass_delta()),
                    );
                    decoy_part_ctr += 1;
                    part
                }),
        );

        let static_n_terminus_modification =
            match ptms.iter().find(|ptm| ptm.is_n_bond() && ptm.is_static()) {
                Some(ptm) => mass_to_int(*ptm.get_mass_delta()),
                None => 0,
            };

        let static_c_terminus_modification =
            match ptms.iter().find(|ptm| ptm.is_c_bond() && ptm.is_static()) {
                Some(ptm) => mass_to_int(*ptm.get_mass_delta()),
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
                decoy_cache.cache(vec![(sequence.clone(), 0)]).await?;
            }
            fasta_file
                .write(
                    gen_fasta_entry(
                        sequence.as_str(),
                        decoy_ctr,
                        FASTA_DECOY_ENTRY_PREFIX,
                        FASTA_DECOY_ENTRY_NAME_PREFIX,
                    )
                    .as_bytes(),
                )
                .await?;
            decoy_ctr += 1;
            if decoy_ctr % 1000 == 0 {
                fasta_file.flush().await?;
            }
            if decoy_ctr == needed_decoys {
                break;
            }
        }
        Ok(decoy_ctr)
    }

    pub async fn create(
        &self,
        fasta_path: &Path,
        mass: i64,
        lower_mass_tolerance: i64,
        upper_mass_tolerance: i64,
        max_variable_modifications: i8,
        ptms: Vec<PTM>,
        decoys_per_peptide: usize,
    ) -> Result<(usize, usize)> {
        let mut fasta_file = File::create(fasta_path).await?;
        let target_ctr = self
            .add_peptides(
                true,
                &mut fasta_file,
                mass,
                lower_mass_tolerance,
                upper_mass_tolerance,
                max_variable_modifications as i16,
                None,
                None,
                None,
                &ptms,
                None,
            )
            .await?;

        let needed_decoys = decoys_per_peptide * target_ctr;

        // Fetch decoys from cache if possible
        //
        let mut decoy_ctr = self
            .add_peptides(
                false,
                &mut fasta_file,
                mass,
                lower_mass_tolerance,
                upper_mass_tolerance,
                max_variable_modifications as i16,
                None, // Not applicable for decoys
                None, // Not applicable for decoys
                None, // Not applicable for decoys
                &ptms,
                Some(needed_decoys),
            )
            .await?;

        if decoy_ctr < needed_decoys {
            decoy_ctr = self
                .generate_missing_decoys(
                    &mut fasta_file,
                    needed_decoys,
                    decoy_ctr,
                    mass,
                    lower_mass_tolerance,
                    upper_mass_tolerance,
                    max_variable_modifications,
                    ptms,
                )
                .await?;
        }

        fasta_file.flush().await?;
        Ok((target_ctr, decoy_ctr))
    }
}
