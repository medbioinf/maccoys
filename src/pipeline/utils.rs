use std::path::PathBuf;

use fancy_regex::Regex;
use lazy_static::lazy_static;

lazy_static! {
    /// Regex for finding illegal path characters:
    /// * Resereved ext4 character and whitespaces
    ///
    static ref ILLEGAL_PATH_CHARACTER_REGEX: Regex = fancy_regex::Regex::new(r"[\s\/\.\u0000]+").unwrap();
}

/// Sanitizes the given string by replacing all illegal path characters with `_`.
///
/// # Arguments
/// * `some_str` - string
///
pub fn sanatize_string_for_path(some_str: &str) -> String {
    ILLEGAL_PATH_CHARACTER_REGEX
        .replace_all(some_str.trim(), "_")
        .to_string()
}

/// Filepath on Ms run level, e.g. `<uuid>/<ms_run>/ms_run.<extension>`
///
/// # Arguments
/// * `uuid` - Search UUID
/// * `ms_run_name` - MS run name
/// * `extension`` - File extension
///
pub fn create_file_path_on_ms_run_level(uuid: &str, ms_run_name: &str, extension: &str) -> PathBuf {
    PathBuf::from(uuid)
        .join(sanatize_string_for_path(ms_run_name))
        .join(format!("ms_run.{}", sanatize_string_for_path(extension)))
}

/// Filepath on spectrum level, e.g. `<uuid>/<ms_run>/<spectrum_id>/spectrum.<extension>`
///
/// # Arguments
/// * `uuid` - Search UUID
/// * `ms_run_name` - MS run name
/// * `spectrum_id` - Spectrum ID
/// * `extension`` - File extension
///
pub fn create_file_path_on_spectrum_level(
    uuid: &str,
    ms_run_name: &str,
    spectrum_id: &str,
    extension: &str,
) -> PathBuf {
    PathBuf::from(uuid)
        .join(sanatize_string_for_path(ms_run_name))
        .join(sanatize_string_for_path(spectrum_id))
        .join(format!("spectrum.{}", sanatize_string_for_path(extension)))
}

/// Filepath on precursor level, e.g. `<uuid>/<ms_run>/<spectrum_id>/<precursor_mz>_<precursor_charge>.<extension>`
///
/// # Arguments
/// * `uuid` - Search UUID
/// * `ms_run_name` - MS run name
/// * `spectrum_id` - Spectrum ID
/// * `precursor` - Precursor mass to charge ratio and charge
/// * `extension` - File extension
///
pub fn create_file_path_on_precursor_level(
    uuid: &str,
    ms_run_name: &str,
    spectrum_id: &str,
    precursor: &(f64, u8),
    extension: &str,
) -> PathBuf {
    PathBuf::from(uuid)
        .join(sanatize_string_for_path(ms_run_name))
        .join(sanatize_string_for_path(spectrum_id))
        .join(format!(
            "{}_{}.{}",
            precursor.0,
            precursor.1,
            sanatize_string_for_path(extension)
        ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    pub fn test_sanatize_string_for_path() {
        let input = "this../..is.a\u{0000}filename to\tsanatize\n";
        let expected = "this_is_a_filename_to_sanatize";

        let result = sanatize_string_for_path(input);
        assert_eq!(result, expected);
    }
}
