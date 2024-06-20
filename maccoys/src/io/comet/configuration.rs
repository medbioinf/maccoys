// std import
use std::collections::HashSet;
use std::fs::{read_to_string, write as write_to_file};
use std::path::Path;

// 3rd party imports
use anyhow::{bail, Result};
use dihardts_omicstools::chemistry::amino_acid::CANONICAL_AMINO_ACIDS;
use dihardts_omicstools::{
    chemistry::amino_acid::{AminoAcid, ISOLEUCINE, NON_CANONICAL_AMINO_ACIDS, PYRROLYSINE},
    proteomics::post_translational_modifications::PostTranslationalModification,
};
use fancy_regex::Regex;

// internal imports
use crate::constants::FASTA_DECOY_ENTRY_PREFIX;

/// Crate for handling comet configuration files.
///
#[derive(Clone)]
pub struct Configuration {
    content: String,
    num_variable_modifications: u8,
    used_static_modifications: HashSet<char>,
    is_n_term_bond_used: bool,
    is_c_term_bond_used: bool,
}

impl Configuration {
    pub fn new(default_comet_file_path: &Path) -> Result<Self> {
        let mut conf = Self {
            content: read_to_string(default_comet_file_path)?,
            num_variable_modifications: 0,
            used_static_modifications: HashSet::with_capacity(
                CANONICAL_AMINO_ACIDS.len() + NON_CANONICAL_AMINO_ACIDS.len(),
            ),
            is_n_term_bond_used: false,
            is_c_term_bond_used: false,
        };
        conf.cleanup_content()?;

        Ok(conf)
    }

    /// Replaces the line of the option name with the option name and the value,
    /// ignoring the rest of the line including comments.
    ///
    /// # Arguments
    /// * `option_name` - Name of the option to replace
    /// * `value` - Value to replace the option with
    ///
    pub fn set_option(&mut self, option_name: &str, value: &str) -> Result<()> {
        let raw_replace_regex = format!(r"(?m)^{} = .+$", option_name); // (?m) is multiline flags
        let replacement_regex = Regex::new(&raw_replace_regex)?;
        self.content = replacement_regex
            .replace(&self.content, format!("{} = {}", option_name, value))
            .to_string();
        Ok(())
    }

    /// Clean up the pre-defined configuration file, includes
    /// * Setting the spectrum batch size to zero, so each spectrum is search in one loop (each spectrum is separated in it own file!)
    /// * Activate TSV-output
    /// * Define mass for J
    /// * Enable NL ions
    /// * Sets decoy prefix
    /// * Sets digest mass range (Max is the MaCPepDB max peptide length * the mass of the largest amino acid)
    /// * Removes Carbamidomethylation of Cysteine and Oxidation of M
    ///
    fn cleanup_content(&mut self) -> Result<()> {
        // Set spectrum batch size to zero, so each spectrum is search in one loop (each spectrum is separated in it own file!)
        self.set_option("spectrum_batch_size", "0")?;

        // Define mass for J
        self.set_option(
            "add_J_user_amino_acid",
            &*ISOLEUCINE.get_mono_mass().to_string(),
        )?;

        // Change output values
        self.set_option("output_txtfile", "1")?;
        self.set_option("output_pepxmlfile", "0")?;

        // Change fragment ions
        self.set_option("use_NL_ions", "1")?;

        // Change misc parameter
        self.set_option("decoy_prefix", FASTA_DECOY_ENTRY_PREFIX)?;
        self.set_option(
            "digest_mass_range",
            &format!("1.0 {}", 60.0 * PYRROLYSINE.get_mono_mass()),
        )?;

        // Spectral processing
        // Set minimum peaks to 5, so it is possible to find at least 5 ions with one sequence completely missing.
        self.set_option("minimum_peaks", "5")?;

        // Unset default modifications
        self.set_option("variable_mod01", "0.0 X 0 3 -1 0 0 0.0")?;
        self.set_option("add_C_cysteine", "0.0")?;

        Ok(())
    }

    /// Sets the charge option so comet only searches for the given charge.
    ///
    /// # Arguments
    /// * `charge` - Charge to search for.
    ///
    pub fn set_charge(&mut self, charge: u8) -> Result<()> {
        self.set_option("precursor_charge", &charge.to_string())?;
        self.set_option("precursor_charge", &format!("{} {}", &charge, &charge))?;

        self.set_option("max_fragment_charge", &charge.to_string())?;
        self.set_option("max_precursor_charge", &charge.to_string())?;
        Ok(())
    }

    /// Sets the mass tolerance for the precursor.
    ///
    /// # Arguments
    /// * `tolerance` - Mass tolerance in ppm.
    ///
    pub fn set_mass_tolerance(&mut self, tolerance: i64) -> Result<()> {
        self.set_option("peptide_mass_tolerance", &tolerance.to_string())?;
        Ok(())
    }

    /// Sets the number of results to return.
    ///
    /// # Arguments
    /// * `num_results` - Number of results to return.
    ///
    pub fn set_num_results(&mut self, num_results: u32) -> Result<()> {
        self.set_option("num_results", &num_results.to_string())?;
        self.set_option("num_output_lines", &num_results.to_string())?;
        Ok(())
    }

    /// Sets the maximum number of variable modifications per peptide.
    ///
    /// # Arguments
    /// * `max_variable_mods` - Maximum number of variable modifications per peptide.
    pub fn set_max_variable_mods(&mut self, max_variable_mods: i8) -> Result<()> {
        self.set_option(
            "max_variable_mods_in_peptide",
            &max_variable_mods.to_string(),
        )?;
        Ok(())
    }

    /// Sets fragment bin tolerance.
    ///
    /// # Arguments
    /// * `tolerance` - Fragment tolerance in ppm.
    ///
    pub fn set_fragment_bin_tolerance(&mut self, tolerance: f64) -> Result<()> {
        self.set_option("fragment_bin_tol", &tolerance.to_string())?;
        Ok(())
    }

    /// Sets fragment bin offset.
    ///
    /// # Arguments
    /// * `offset` - Fragment tolerance in ppm.
    ///
    pub fn set_fragment_bin_offset(&mut self, offset: f64) -> Result<()> {
        self.set_option("fragment_bin_offset", &offset.to_string())?;
        Ok(())
    }

    /// Sets the post translational modifications
    /// * Static/anywhere will be set as `add_<amino_acid_code>_<amino_acid_name>`
    /// * Static/n-bond will be set as `add_Nterm_peptide`
    /// * Static/c-bond will be set as `add_Cterm_peptide`
    /// * Variable will be set as `variable_mod0<index>` if it is
    /// TODO: How to handle Static/terminal? As variable? Otherwise there is no option to set a terminal modification to a specific amino acid in comet.
    ///
    /// # Arguments
    /// * `ptms` - Post translational modifications to set
    /// * `max_variable_modifications` - Maximum number of variable modifications per peptide.
    ///
    pub fn set_ptms(
        &mut self,
        ptms: &Vec<PostTranslationalModification>,
        max_variable_modifications: i8,
    ) -> Result<()> {
        for ptm in ptms {
            // Handle static PTMs for any position
            if ptm.is_static() {
                if ptm.is_anywhere() {
                    if self
                        .used_static_modifications
                        .contains(ptm.get_amino_acid().get_code())
                    {
                        bail!(
                            "Static modification for {} is already used!",
                            ptm.get_amino_acid().get_code()
                        );
                    }
                    let option_name =
                        // Cannot use `.contains()` as AminoAcid nor NonCanonicalAminoAcid implement `PartialEq`
                        match NON_CANONICAL_AMINO_ACIDS
                            .iter()
                            .find(|non_canonical_amino_acid| {
                                non_canonical_amino_acid.get_code()
                                    == ptm.get_amino_acid().get_code()
                            }) {
                            Some(_) => format!(
                                "add_{}_{}",
                                ptm.get_amino_acid().get_code(),
                                ptm.get_amino_acid().get_name().to_lowercase()
                            ),
                            None => {
                                format!("add_{}_user_amino_acid", ptm.get_amino_acid().get_code(),)
                            }
                        };
                    self.set_option(&option_name, &ptm.get_mass_delta().to_string())?;
                    self.used_static_modifications
                        .insert(*ptm.get_amino_acid().get_code());
                }
                if ptm.is_n_bond() {
                    if self.is_n_term_bond_used {
                        bail!("N-term bond is already used!");
                    }
                    self.set_option("add_Nterm_peptide", &ptm.get_mass_delta().to_string())?;
                    self.is_n_term_bond_used = true;
                }
                if ptm.is_c_bond() {
                    if self.is_c_term_bond_used {
                        bail!("C-term bond is already used!");
                    }
                    self.set_option("add_Cterm_peptide", &ptm.get_mass_delta().to_string())?;
                    self.is_c_term_bond_used = true;
                }
            }
            if ptm.is_variable() {
                self.num_variable_modifications += 1;
                if self.num_variable_modifications > 9 {
                    bail!("Comet only supports 9 variable modifications!");
                }
                let option_name = format!("variable_mod0{}", self.num_variable_modifications);
                let terminus = if ptm.is_n_terminus() {
                    2
                } else if ptm.is_c_terminus() {
                    1
                } else {
                    0
                };
                let distance = if ptm.is_n_terminus() || ptm.is_c_terminus() {
                    0
                } else {
                    -1
                };
                let value = format!(
                    "{} {} 0 {} {} {} 0 0.0",
                    ptm.get_mass_delta(),
                    ptm.get_amino_acid().get_code(),
                    max_variable_modifications,
                    terminus,
                    distance
                );
                self.set_option(&option_name, &value)?;
            }
        }
        Ok(())
    }

    /// Writes the configuration to the given path.
    ///
    /// # Arguments
    /// * `path` - Path to write the configuration to.
    ///
    pub fn to_file(&self, path: &Path) -> Result<()> {
        write_to_file(path, &self.content)?;
        Ok(())
    }

    /// Writes the configuration to the given path asynchrounously.
    ///
    /// # Arguments
    /// * `path` - Path to write the configuration to.
    ///
    pub async fn async_to_file(&self, path: &Path) -> Result<()> {
        tokio::fs::write(path, &self.content).await?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn test_configuration() {
        use super::*;

        let conf = Configuration::new(Path::new("test_files/comet.params")).unwrap();
        let expected_content =
            read_to_string(Path::new("test_files/expected.comet.params")).unwrap();
        let conf_split = conf.content.split("\n").collect::<Vec<&str>>();
        let expected_split = expected_content.split("\n").collect::<Vec<&str>>();
        assert_eq!(conf_split.len(), expected_split.len());
        for (line, expected_line) in conf_split.iter().zip(expected_split.iter()) {
            assert_eq!(line, expected_line);
        }
    }
}
