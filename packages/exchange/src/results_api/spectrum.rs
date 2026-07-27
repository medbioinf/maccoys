// 3rd party imports
use polars::prelude::*;

/// PSMS and goodness of fit for a spectrums charge state
///
#[derive(serde::Serialize, serde::Deserialize)]
pub struct Identification {
    goodnesses: Option<DataFrame>,
    psms: Option<DataFrame>,
    precursor: f64,
    charge: u8,
}

impl Identification {
    pub fn new(
        goodnesses: Option<DataFrame>,
        psms: Option<DataFrame>,
        precursor: f64,
        charge: u8,
    ) -> Self {
        Self {
            goodnesses,
            psms,
            precursor,
            charge,
        }
    }

    pub fn get_goodnesses(&self) -> &Option<DataFrame> {
        &self.goodnesses
    }

    pub fn get_psms(&self) -> &Option<DataFrame> {
        &self.psms
    }

    pub fn get_precursor(&self) -> f64 {
        self.precursor
    }

    pub fn get_charge(&self) -> u8 {
        self.charge
    }

    /// Histogram of the original search engine score (xcorr, for Comet)
    /// Bin number is calculated using the rule of Sturges
    ///
    /// TODO: Error handling. Urgent.
    pub fn get_score_histogram(&self) -> Option<(Vec<f64>, Vec<usize>)> {
        let score = self.psms.as_ref()?["xcorr"].as_materialized_series();

        // rule of sturges to determine number of bins
        let num_bins = (1.0 + (score.len() as f64).log2()).round() as usize;

        let min = score.min::<f64>().unwrap().unwrap();
        let max = score.max::<f64>().unwrap().unwrap();

        let bin_width = (max - min) / num_bins as f64;

        // Define bins
        let mut bins: Vec<f64> = Vec::new();
        for i in 0..=num_bins {
            bins.push(min + i as f64 * bin_width);
        }

        // Calculate histogram counts
        let mut histogram: Vec<usize> = vec![0; num_bins];

        for val in score.f64().unwrap().iter() {
            let val = val.unwrap();
            for (i, &bin) in bins.iter().enumerate().skip(1) {
                if val <= bin {
                    histogram[i - 1] += 1;
                    break;
                }
            }
        }

        Some((bins, histogram))
    }
}

/// Represents a spectrum and its content (e.g. the identifications that are part of the spectrum)
#[derive(serde::Serialize, serde::Deserialize)]
pub struct Spectrum {
    search_uuid: String,
    ms_run_name: String,
    spectrum_id: String,
    mz: Vec<f64>,
    intensity: Vec<f64>,
    identifications: Vec<Identification>,
}

impl Spectrum {
    pub fn new(
        search_uuid: String,
        ms_run_name: String,
        spectrum_id: String,
        mz: Vec<f64>,
        intensity: Vec<f64>,
        identifications: Vec<Identification>,
    ) -> Self {
        Self {
            search_uuid,
            ms_run_name,
            spectrum_id,
            mz,
            intensity,
            identifications,
        }
    }

    pub fn get_search_uuid(&self) -> &str {
        &self.search_uuid
    }

    pub fn get_ms_run(&self) -> &str {
        &self.ms_run_name
    }

    pub fn get_spectra_id(&self) -> &str {
        &self.spectrum_id
    }

    pub fn get_mz(&self) -> &Vec<f64> {
        &self.mz
    }

    pub fn get_intensity(&self) -> &Vec<f64> {
        &self.intensity
    }

    pub fn get_identifications(&self) -> &Vec<Identification> {
        &self.identifications
    }
}
