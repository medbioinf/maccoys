// std imports
use std::str::FromStr;
use std::time::Instant;

// 3rd party imports
use anyhow::{bail, Result};
use async_stream::try_stream;
use dihardts_omicstools::proteomics::peptide::Terminus;
use dihardts_omicstools::proteomics::post_translational_modifications::ModificationType;
use dihardts_omicstools::proteomics::post_translational_modifications::Position as ModificationPosition;
use fancy_regex::Regex;
use futures::Stream;
use macpepdb::chemistry::amino_acid::calc_sequence_mass;
use macpepdb::chemistry::molecule::WATER;
use macpepdb::database::configuration_table::ConfigurationTable as ConfigurationTableTrait;
use macpepdb::database::scylla::client::Client as DbClient;
use macpepdb::database::scylla::client::GenericClient;
use macpepdb::database::scylla::configuration_table::ConfigurationTable;
use macpepdb::database::scylla::peptide_table::PeptideTable;
use macpepdb::database::selectable_table::SelectableTable;
use macpepdb::entities::configuration::Configuration as DbConfiguration;
use macpepdb::tools::peptide_partitioner::get_mass_partition;
use rand::{thread_rng, Rng};
use reqwest;
use scylla::frame::response::result::CqlValue;

// internal imports
use crate::search_space::decoy_part::DecoyPart;

/// Target loopup url
/// For http urls, a placeholder for the sequence added to the url `:sequence:`
///
#[derive(Clone)]
pub enum TargetLookupUrl {
    None,
    Web(String),
    Database(String),
    WebBloomFilter(String),
}

impl FromStr for TargetLookupUrl {
    type Err = anyhow::Error;

    fn from_str(url: &str) -> Result<Self> {
        if url.is_empty() {
            Ok(TargetLookupUrl::None)
        } else if url.starts_with("http") {
            Ok(TargetLookupUrl::Web(format!(
                "{}/api/peptides/:sequence:/exists",
                url
            )))
        } else if url.starts_with("bloom+http") {
            Ok(TargetLookupUrl::WebBloomFilter(format!(
                "{}/lookup/everywhere/:sequence:",
                &url[6..]
            )))
        } else if url.starts_with("scylla") {
            Ok(TargetLookupUrl::Database(url.to_string()))
        } else {
            bail!("Unknown target lookup type for url: {}", url)
        }
    }
}

/// Generate a decoy within the given mass range and parameters
/// by generating a random sequence of amino acids until the mass is larger
/// as the targeted range. Than interchange amino acids until the mass is within
/// the range.
///
pub struct DecoyGenerator {
    target_lookup_url: TargetLookupUrl,
    initial_target_lookup_url: Option<String>,
    cleavage_terminus: Terminus,
    allowed_cleavage_amino_acids: Vec<char>,
    missed_cleavage_regex: Regex,
    decoy_parts: Vec<DecoyPart>,
    cleavage_decoy_parts: Vec<Vec<DecoyPart>>,
    initial_cleavage_propability: Vec<Option<usize>>,
    static_n_terminus_modification: i64,
    static_c_terminus_modification: i64,
    mass_distance_matrix: Vec<Vec<i64>>,
    http_client: Option<reqwest::Client>,
    db_client: Option<DbClient>,
    db_configuration: Option<DbConfiguration>,
}

impl DecoyGenerator {
    pub async fn new(
        target_lookup_url: Option<String>,
        cleavage_terminus: Terminus,
        allowed_cleavage_amino_acids: Vec<char>,
        missed_cleavage_regex: Regex,
        decoy_parts: Vec<DecoyPart>,
        initial_cleavage_propability: Vec<Option<usize>>,
        static_n_terminus_modification: i64,
        static_c_terminus_modification: i64,
    ) -> Result<Self> {
        let mass_distance_matrix = Self::build_mass_distance_matrix(&decoy_parts);
        let initial_target_lookup_url = target_lookup_url.clone();
        let target_lookup_url = match target_lookup_url {
            Some(url) => TargetLookupUrl::from_str(url.as_str())?,
            None => TargetLookupUrl::None,
        };

        let http_client = match target_lookup_url {
            TargetLookupUrl::Web(_) | TargetLookupUrl::WebBloomFilter(_) => {
                Some(reqwest::Client::new())
            }
            _ => None,
        };

        let (db_client, db_configuration): (Option<DbClient>, Option<DbConfiguration>) =
            match &target_lookup_url {
                TargetLookupUrl::Database(url) => {
                    let client = DbClient::new(url.as_str()).await?;
                    let configuration = ConfigurationTable::select(&client).await?;
                    (Some(client), Some(configuration))
                }
                _ => (None, None),
            };

        // Collect decoy parts for each allowed cleavage amino acid
        // without modification or with modification fitting the cleavage site
        let mut cleavage_decoy_parts: Vec<Vec<DecoyPart>> = Vec::new();
        for amino_acid in allowed_cleavage_amino_acids.iter() {
            let mut cleavage_decoy_parts_for_amino_acid: Vec<DecoyPart> = Vec::new();
            for decoy_part in &decoy_parts {
                // Skip if amino acid is not allowed
                if decoy_part.get_amino_acid() != *amino_acid {
                    continue;
                }
                // Add to list if modification is none, anywhere or at the cleavage site
                if decoy_part.get_modification_type().is_none()
                    || *decoy_part.get_modification_position() == ModificationPosition::Anywhere
                    || *decoy_part.get_modification_position()
                        == ModificationPosition::Terminus(Terminus::N)
                        && cleavage_terminus == Terminus::N
                    || *decoy_part.get_modification_position()
                        == ModificationPosition::Terminus(Terminus::C)
                        && cleavage_terminus == Terminus::C
                {
                    cleavage_decoy_parts_for_amino_acid.push(decoy_part.clone());
                }
            }
            cleavage_decoy_parts.push(cleavage_decoy_parts_for_amino_acid);
        }

        Ok(Self {
            target_lookup_url,
            cleavage_terminus,
            allowed_cleavage_amino_acids,
            missed_cleavage_regex,
            decoy_parts,
            cleavage_decoy_parts,
            initial_cleavage_propability,
            static_n_terminus_modification,
            static_c_terminus_modification,
            mass_distance_matrix,
            http_client,
            db_client,
            db_configuration,
            initial_target_lookup_url,
        })
    }

    /// Builds distance matrix between each of the given amino acids
    ///
    /// # Arguments
    /// * `decoy_parts` - A vector of tuples containing the amino acid one letter code and mass
    ///
    fn build_mass_distance_matrix(decoy_parts: &Vec<DecoyPart>) -> Vec<Vec<i64>> {
        let mut matrix = vec![vec![0; decoy_parts.len()]; decoy_parts.len()];
        for i in 0..decoy_parts.len() {
            for j in 0..decoy_parts.len() {
                matrix[i][j] = decoy_parts[j].get_total_mass() - decoy_parts[i].get_total_mass();
            }
        }
        matrix
    }

    /// Generates one decoys within the given mass range and parameters.
    ///
    /// # Arguments
    /// * `mass_tolerance_lower` - The lower bound of the mass tolerance
    /// * `mass_tolerance_upper` - The upper bound of the mass tolerance
    /// * `min_decoy_length` - Min decoy length
    /// * `max_decoy_length` - Max decoy length
    /// * `max_number_of_variable_modifications` - The maximum number of modifications
    /// * `max_number_of_missed_cleavages` - The maximum number of missed cleavages
    /// * `timeout` - Timeout in seconds
    /// * `annotate_modifications` - Whether to annotate the modifications in the sequence
    ///
    pub async fn generate(
        &self,
        target_mass: i64,
        mass_tolerance_lower: i64,
        mass_tolerance_upper: i64,
        min_decoy_length: usize,
        max_decoy_length: usize,
        max_number_of_variable_modifications: i8,
        max_number_of_missed_cleavages: i8,
        timeout: f64,
        annotate_modifications: bool,
    ) -> Result<Option<String>> {
        let start = Instant::now();
        // Cast once so it can be used multiple times
        let max_number_of_missed_cleavages_u = max_number_of_missed_cleavages as usize;
        let mut rng = thread_rng();
        'decoy_loop: while start.elapsed().as_secs_f64() < timeout {
            let mut mass = WATER.get_mono_mass()
                + self.static_n_terminus_modification
                + self.static_c_terminus_modification;
            let mut sequence: Vec<&DecoyPart> = Vec::new();
            let mut number_of_variable_modifications = 0;
            let mut interchanges = 0;

            let random_initialization_idx =
                rng.gen_range(0..self.initial_cleavage_propability.len());
            if let Some(part_vec_idx) = self.initial_cleavage_propability[random_initialization_idx]
            {
                let part_idx = rng.gen_range(0..self.cleavage_decoy_parts[part_vec_idx].len());
                sequence.push(&self.cleavage_decoy_parts[part_vec_idx][part_idx]);
                mass += self.cleavage_decoy_parts[part_vec_idx][part_idx].get_total_mass();
                if self.cleavage_decoy_parts[part_vec_idx][part_idx]
                    .get_modification_type()
                    .as_ref()
                    .is_some_and(|mod_type| *mod_type == ModificationType::Variable)
                {
                    number_of_variable_modifications += 1;
                }
            }

            // Build random sequence
            while mass < mass_tolerance_upper {
                let random_amino_acid_idx = rng.gen_range(0..self.decoy_parts.len());
                match self.cleavage_terminus {
                    // C-terminus is cleavage site. Adding to N-terminus
                    Terminus::C => sequence.insert(0, &self.decoy_parts[random_amino_acid_idx]),
                    // N-terminus is cleavage site. Adding to C-terminus
                    Terminus::N => sequence.push(&self.decoy_parts[random_amino_acid_idx]),
                }
                mass += self.decoy_parts[random_amino_acid_idx].get_total_mass();
                if self.decoy_parts[random_amino_acid_idx]
                    .get_modification_type()
                    .as_ref()
                    .is_some_and(|mod_type| *mod_type == ModificationType::Variable)
                {
                    number_of_variable_modifications += 1;
                }
            }

            // Interchange amino acids until mass is within range
            while interchanges < 100 {
                interchanges += 1;

                let sequence_str = sequence
                    .iter()
                    .map(|x| x.get_amino_acid())
                    .collect::<String>();

                // Count missed cleavages using the provided regex
                let number_of_missed_cleavages = self
                    .missed_cleavage_regex
                    .find_iter(sequence_str.as_str())
                    .count();

                // Check if mass, missed_cleavages and modifications are within tolerances
                if mass_tolerance_lower <= mass
                    && mass <= mass_tolerance_upper
                    && min_decoy_length <= sequence.len()
                    && sequence.len() <= max_decoy_length
                    && number_of_variable_modifications <= max_number_of_variable_modifications
                    && number_of_missed_cleavages <= max_number_of_missed_cleavages_u
                {
                    if self.is_not_a_target(&sequence_str).await? {
                        if !annotate_modifications {
                            return Ok(Some(sequence_str));
                        } else {
                            return Ok(Some(
                                sequence
                                    .iter()
                                    .map(|decoy_part| decoy_part.to_string())
                                    .collect::<String>(),
                            ));
                        }
                    } else {
                        // If sequence was target, just generate a new one
                        continue 'decoy_loop;
                    }
                }

                let target_difference = mass - target_mass;

                let replace_choices = match self.cleavage_terminus {
                    // C-terminus is cleavage site. Interchange everything but last amino acid
                    Terminus::C => 0..sequence.len() - 1,
                    // N-terminus is cleavage site. Interchange everything but first amino acid
                    Terminus::N => 1..sequence.len(),
                };
                let replace_idx = rng.gen_range(replace_choices);
                let replace_decoy_part = sequence[replace_idx];

                // Find amino acid with mass closest to target difference
                let mut replace_mass_differences: Vec<(usize, i64)> = self.mass_distance_matrix
                    [replace_decoy_part.get_index()]
                .iter()
                .enumerate()
                .map(|(idx, mass_diff)| (idx, (target_difference + mass_diff).abs()))
                .collect();
                replace_mass_differences.sort_by_key(|(_, mass_diff)| mass_diff.abs());

                for (replace_with_idx, _) in replace_mass_differences.iter() {
                    // Replace if:
                    // Replacment part is not the same as the current part and
                    //   replacement_part has no modification
                    //   or replacment_part has a modification and
                    //       it is a anywhere modification
                    //       or a n-terminus modification and we interchange at 0 (n-terminus)
                    //       or a c-terminus modification and we interchange at len(seq)-1 (c-terminus)
                    let replacement_part = &self.decoy_parts[*replace_with_idx];
                    if replacement_part.get_index() != replace_decoy_part.get_index()
                        && (replacement_part.get_modification_type().is_none()
                            || (replacement_part
                                .get_modification_type()
                                .as_ref()
                                .is_some_and(|mod_type| *mod_type == ModificationType::Variable)
                                && (*replacement_part.get_modification_position()
                                    == ModificationPosition::Anywhere
                                    || (*replacement_part.get_modification_position()
                                        == ModificationPosition::Terminus(Terminus::N)
                                        && replace_idx == 0)
                                    || (*replacement_part.get_modification_position()
                                        == ModificationPosition::Terminus(Terminus::C)
                                        && replace_idx == sequence.len() - 1))))
                    {
                        mass = mass - sequence[replace_idx].get_total_mass()
                            + replacement_part.get_total_mass();
                        if sequence[replace_idx]
                            .get_modification_type()
                            .as_ref()
                            .is_some_and(|mod_type| *mod_type == ModificationType::Variable)
                            && replacement_part
                                .get_modification_type()
                                .as_ref()
                                .is_some_and(|mod_type| *mod_type != ModificationType::Variable)
                        {
                            number_of_variable_modifications -= 1;
                        }
                        sequence[replace_idx] = replacement_part;
                        break;
                    }
                }
            }
        }
        Ok(None)
    }

    async fn is_not_a_target(&self, sequence: &str) -> Result<bool> {
        match &self.target_lookup_url {
            TargetLookupUrl::None => Ok(true),
            TargetLookupUrl::Database(_) => {
                let sequence = sequence.to_uppercase();
                let mass = calc_sequence_mass(sequence.as_str())?;
                let partition = get_mass_partition(
                    self.db_configuration
                        .as_ref()
                        .unwrap()
                        .get_partition_limits(),
                    mass,
                )?;
                let peptide_opt = PeptideTable::select(
                    self.db_client.as_ref().unwrap(),
                    "WHERE partition = ? AND mass = ? and sequence = ?",
                    &[
                        &CqlValue::BigInt(partition as i64),
                        &CqlValue::BigInt(mass),
                        &CqlValue::Text(sequence),
                    ],
                )
                .await?;
                Ok(peptide_opt.is_none())
            }
            TargetLookupUrl::Web(url) | TargetLookupUrl::WebBloomFilter(url) => {
                let url = url.replace(":sequence:", sequence);
                // let response = match &self.http_client {
                //     Some(client) => client.get(&url).send().await?,
                //     None => bail!(
                //         "No http client available, but TargetLookupType is Web or WebBloomFilter"
                //     ),
                // };

                match self
                    .http_client
                    .as_ref()
                    .unwrap()
                    .get(&url)
                    .send()
                    .await?
                    .status()
                    .as_u16()
                {
                    200 => {
                        // If sequence was target, just generate a new one
                        Ok(false)
                    }
                    404 => Ok(true),
                    unknown => {
                        bail!("Unexpected response status: {}", unknown);
                    }
                }
            }
        }
    }

    /// Generates multiple decoys within the given mass range and parameters.
    /// Stops if the amount of decoys or the timeout is reached.
    ///
    /// # Arguments
    /// * `mass_tolerance_lower` - The lower bound of the mass tolerance
    /// * `mass_tolerance_upper` - The upper bound of the mass tolerance
    /// * `min_decoy_length` - Min decoy length
    /// * `max_decoy_length` - Max decoy length
    /// * `max_number_of_variable_modifications` - The maximum number of modifications
    /// * `max_number_of_missed_cleavages` - The maximum number of missed cleavages
    /// * `timeout` - Timeout in seconds
    /// * `annotate_modifications` - Whether to annotate the modifications in the sequence
    /// * `decoy_amount` - The amount of decoys to generate
    ///
    pub async fn generate_multiple(
        &mut self,
        target_mass: i64,
        mass_tolerance_lower: i64,
        mass_tolerance_upper: i64,
        min_decoy_length: usize,
        max_decoy_length: usize,
        max_number_of_variable_modifications: i8,
        max_number_of_missed_cleavages: i8,
        timeout: f64,
        annotate_modifications: bool,
        decoy_amount: usize,
    ) -> Result<Vec<String>> {
        let start = Instant::now();
        let mut decoys = Vec::with_capacity(decoy_amount);
        for _ in 0..decoy_amount {
            match self
                .generate(
                    target_mass,
                    mass_tolerance_lower,
                    mass_tolerance_upper,
                    min_decoy_length,
                    max_decoy_length,
                    max_number_of_variable_modifications,
                    max_number_of_missed_cleavages,
                    timeout - start.elapsed().as_secs_f64(),
                    annotate_modifications,
                )
                .await?
            {
                Some(decoy) => decoys.push(decoy),
                None => break,
            }
        }
        return Ok(decoys);
    }

    /// Returns a decoy generator stream which generates decoys within the given mass range and parameters.
    /// Stops if timeout is reached.
    ///
    /// # Arguments
    /// * `target_mass` - The target mass
    /// * `mass_tolerance_lower` - The lower bound of the mass tolerance
    /// * `mass_tolerance_upper` - The upper bound of the mass tolerance
    /// * `min_decoy_length` - Min decoy length
    /// * `max_decoy_length` - Max decoy length
    /// * `max_number_of_variable_modifications` - The maximum number of modifications
    /// * `max_number_of_missed_cleavages` - The maximum number of missed cleavages
    /// * `timeout` - Timeout in seconds
    /// * `annotate_modifications` - Whether to annotate the modifications in the sequence
    ///     
    pub async fn stream<'a>(
        &'a self,
        target_mass: i64,
        mass_tolerance_lower: i64,
        mass_tolerance_upper: i64,
        min_decoy_length: usize,
        max_decoy_length: usize,
        max_number_of_variable_modifications: i8,
        max_number_of_missed_cleavages: i8,
        timeout: f64,
        annotate_modifications: bool,
    ) -> Result<impl Stream<Item = Result<String>> + 'a> {
        Ok(try_stream! {
            let mut remaining_time = timeout;
            loop {
                let start = Instant::now();
                match self.generate(
                    target_mass,
                    mass_tolerance_lower,
                    mass_tolerance_upper,
                    min_decoy_length,
                    max_decoy_length,
                    max_number_of_variable_modifications,
                    max_number_of_missed_cleavages,
                    remaining_time,
                    annotate_modifications,
                ).await? {
                    Some(decoy) => {
                        yield decoy;
                        remaining_time -= start.elapsed().as_secs_f64();
                    },
                    None => break,
                }
            }
        })
    }

    /// Aynchronous clone of the decoy generator
    ///
    pub async fn async_clone(&self) -> Result<Self> {
        Ok(Self::new(
            self.initial_target_lookup_url.clone(),
            self.cleavage_terminus.clone(),
            self.allowed_cleavage_amino_acids.clone(),
            self.missed_cleavage_regex.clone(),
            self.decoy_parts.clone(),
            self.initial_cleavage_propability.clone(),
            self.static_n_terminus_modification,
            self.static_c_terminus_modification,
        )
        .await?)
    }
}
