// std import
use std::fs::read_to_string;
use std::path::Path;

// 3rd party imports
use anyhow::Result;
use dihardts_omicstools::chemistry::amino_acid::{AminoAcid, ISOLEUCINE, PYRROLYSINE};
use fancy_regex::Regex;

// internal imports
use crate::constants::FASTA_DECOY_ENTRY_PREFIX;

/// Crate for handling comet configuration files.
///
pub struct Configuration {
    content: String,
}

impl Configuration {
    pub fn new(default_comet_file_path: &Path) -> Result<Self> {
        let mut conf = Self {
            content: read_to_string(default_comet_file_path)?,
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
        println!("{:?}", replacement_regex);
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
