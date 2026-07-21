// std imports
use std::time::Instant;

// 3rd party imports
use anyhow::Result;
use async_stream::try_stream;
use dihardts_omicstools::proteomics::peptide::Terminus;
use dihardts_omicstools::proteomics::post_translational_modifications::ModificationType;
use dihardts_omicstools::proteomics::post_translational_modifications::Position as ModificationPosition;
use fancy_regex::Regex;
use futures::Stream;
use macpepdb::chemistry::molecule::WATER_MONO_MASS;
use rand::{thread_rng, Rng};

// internal imports
use crate::search_space::decoy_part::DecoyPart;

/// Generate a decoy within the given mass range and parameters
/// by generating a random sequence of amino acids until the mass is larger
/// as the targeted range. Than interchange amino acids until the mass is within
/// the range.
///
pub struct DecoyGenerator {
    cleavage_terminus: Terminus,
    allowed_cleavage_amino_acids: Vec<char>,
    missed_cleavage_regex: Regex,
    decoy_parts: Vec<DecoyPart>,
    cleavage_decoy_parts: Vec<Vec<DecoyPart>>,
    initial_cleavage_propability: Vec<Option<usize>>,
    static_n_terminus_modification: i64,
    static_c_terminus_modification: i64,
    mass_distance_matrix: Vec<Vec<i64>>,
}

impl DecoyGenerator {
    pub async fn new(
        cleavage_terminus: Terminus,
        allowed_cleavage_amino_acids: Vec<char>,
        missed_cleavage_regex: Regex,
        decoy_parts: Vec<DecoyPart>,
        initial_cleavage_propability: Vec<Option<usize>>,
        static_n_terminus_modification: i64,
        static_c_terminus_modification: i64,
    ) -> Result<Self> {
        let mass_distance_matrix = Self::build_mass_distance_matrix(&decoy_parts);

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
            cleavage_terminus,
            allowed_cleavage_amino_acids,
            missed_cleavage_regex,
            decoy_parts,
            cleavage_decoy_parts,
            initial_cleavage_propability,
            static_n_terminus_modification,
            static_c_terminus_modification,
            mass_distance_matrix,
        })
    }

    /// Builds distance matrix between each of the given amino acids
    ///
    /// # Arguments
    /// * `decoy_parts` - A vector of tuples containing the amino acid one letter code and mass
    ///
    fn build_mass_distance_matrix(decoy_parts: &[DecoyPart]) -> Vec<Vec<i64>> {
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
    /// * `upper_mass_tolerance_ppm` - The lower bound of the mass tolerance
    /// * `lower_mass_tolerance_ppm` - The upper bound of the mass tolerance
    /// * `min_decoy_length` - Min decoy length
    /// * `max_decoy_length` - Max decoy length
    /// * `max_number_of_variable_modifications` - The maximum number of modifications
    /// * `max_number_of_missed_cleavages` - The maximum number of missed cleavages
    /// * `timeout` - Timeout in seconds
    /// * `annotate_modifications` - Whether to annotate the modifications in the sequence
    ///
    #[allow(clippy::too_many_arguments)]
    pub async fn generate(
        &self,
        target_mass: i64,
        upper_mass_tolerance_ppm: i64,
        lower_mass_tolerance_ppm: i64,
        min_decoy_length: usize,
        max_decoy_length: usize,
        max_number_of_variable_modifications: i8,
        max_number_of_missed_cleavages: i8,
        timeout: f64,
        annotate_modifications: bool,
    ) -> Result<Option<String>> {
        let start = Instant::now();
        let lower_mass_limit = target_mass - target_mass * lower_mass_tolerance_ppm / 1_000_000;
        let upper_mass_limit = target_mass + target_mass * upper_mass_tolerance_ppm / 1_000_000;

        // Cast once so it can be used multiple times
        let max_number_of_missed_cleavages_u = max_number_of_missed_cleavages as usize;
        let mut rng = thread_rng();
        while start.elapsed().as_secs_f64() < timeout {
            let mut mass = *WATER_MONO_MASS
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
            while mass < upper_mass_limit {
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
                if lower_mass_limit <= mass
                    && mass <= upper_mass_limit
                    && min_decoy_length <= sequence.len()
                    && sequence.len() <= max_decoy_length
                    && number_of_variable_modifications <= max_number_of_variable_modifications
                    && number_of_missed_cleavages <= max_number_of_missed_cleavages_u
                {
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

    /// Generates multiple decoys within the given mass range and parameters.
    /// Stops if the amount of decoys or the timeout is reached.
    ///
    /// # Arguments
    /// * `lower_mass_tolerance` - The lower bound of the mass tolerance
    /// * `upper_mass_tolerance` - The upper bound of the mass tolerance
    /// * `min_decoy_length` - Min decoy length
    /// * `max_decoy_length` - Max decoy length
    /// * `max_number_of_variable_modifications` - The maximum number of modifications
    /// * `max_number_of_missed_cleavages` - The maximum number of missed cleavages
    /// * `timeout` - Timeout in seconds
    /// * `annotate_modifications` - Whether to annotate the modifications in the sequence
    /// * `decoy_amount` - The amount of decoys to generate
    ///
    #[allow(clippy::too_many_arguments)]
    pub async fn generate_multiple(
        &mut self,
        target_mass: i64,
        lower_mass_tolerance_ppm: i64,
        upper_mass_tolerance_ppm: i64,
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
                    lower_mass_tolerance_ppm,
                    upper_mass_tolerance_ppm,
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
        Ok(decoys)
    }

    /// Returns a decoy generator stream which generates decoys within the given mass range and parameters.
    /// Stops if timeout is reached.
    ///
    /// # Arguments
    /// * `target_mass` - The target mass
    /// * `mass_tolerance_lower` - The lower bound of the mass tolerance
    /// * `upper_mass_tolerance` - The upper bound of the mass tolerance
    /// * `min_decoy_length` - Min decoy length
    /// * `max_decoy_length` - Max decoy length
    /// * `max_number_of_variable_modifications` - The maximum number of modifications
    /// * `max_number_of_missed_cleavages` - The maximum number of missed cleavages
    /// * `timeout` - Timeout in seconds
    /// * `annotate_modifications` - Whether to annotate the modifications in the sequence
    ///
    #[allow(clippy::too_many_arguments)]
    pub async fn stream(
        &self,
        target_mass: i64,
        lower_mass_tolerance_ppm: i64,
        upper_mass_tolerance_ppm: i64,
        min_decoy_length: usize,
        max_decoy_length: usize,
        max_number_of_variable_modifications: i8,
        max_number_of_missed_cleavages: i8,
        timeout: f64,
        annotate_modifications: bool,
    ) -> Result<impl Stream<Item = Result<String>> + '_> {
        Ok(try_stream! {
            let mut remaining_time = timeout;
            loop {
                let start = Instant::now();
                match self.generate(
                    target_mass,
                    lower_mass_tolerance_ppm,
                    upper_mass_tolerance_ppm,
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

    /// Asynchronous clone of the decoy generator
    ///
    pub async fn async_clone(&self) -> Result<Self> {
        Self::new(
            self.cleavage_terminus.clone(),
            self.allowed_cleavage_amino_acids.clone(),
            self.missed_cleavage_regex.clone(),
            self.decoy_parts.clone(),
            self.initial_cleavage_propability.clone(),
            self.static_n_terminus_modification,
            self.static_c_terminus_modification,
        )
        .await
    }
}
