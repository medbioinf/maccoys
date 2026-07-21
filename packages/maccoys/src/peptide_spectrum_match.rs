use std::{borrow::Cow, cmp::Ordering};

use dihardts_omicstools::mass_spectrometry::unit_conversions::mass_to_charge_to_dalton;
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use xcorrrs::scoring_result::ScoringResult;

use crate::precursor::Precursor;

/// Struct representing a peptide-spectrum match (PSM)
///
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PeptideSpectrumMatch<'a> {
    /// Scan ID
    scan_id: Cow<'a, str>,
    /// Amino acid sequence of the peptide in ProForma format
    peptide: Cow<'a, str>,
    /// Precursor
    #[serde(flatten)]
    precursor: Cow<'a, Precursor>,
    /// Experimental mass of the precursor
    experimental_mass: f64,
    /// Xcorr
    xcorr: f64,
    /// Ions in the theoretical spectrum
    ions_total: u64,
    /// Ions matched between theoretical and observed spectrum
    ions_matched: u64,
    /// Theoretical mass of the peptide
    theoretical_mass: f64,
    /// Second theoretical mass of the peptide, occused if the molecule is ambiguous
    second_theoretical_mass: Option<f64>,
}

impl<'a> PeptideSpectrumMatch<'a> {
    /// Creates a new PeptideSpectrumMatch
    /// # Arguments
    /// * `scan_id` - Scan ID
    /// * `peptide` - Amino acid sequence of the peptide in ProForma format
    /// * `precursor` - Precursor
    /// * `scoreing_result` - Result of the scoring
    ///
    pub fn new(
        scan_id: Cow<'a, str>,
        peptide: Cow<'a, str>,
        precursor: Cow<'a, Precursor>,
        scoreing_result: ScoringResult,
    ) -> Self {
        let experimental_mass = mass_to_charge_to_dalton(precursor.mz(), precursor.charge());
        Self {
            scan_id,
            peptide,
            precursor,
            experimental_mass,
            xcorr: scoreing_result.score,
            ions_total: scoreing_result.ions_total as u64,
            ions_matched: scoreing_result.ions_matched as u64,
            theoretical_mass: scoreing_result.min_theoretical_mass,
            second_theoretical_mass: if scoreing_result.min_theoretical_mass
                == scoreing_result.max_theoretical_mass
            {
                None
            } else {
                Some(scoreing_result.max_theoretical_mass)
            },
        }
    }

    /// Returns the scan ID
    pub fn scan_id(&self) -> &str {
        &self.scan_id
    }

    /// Returns the peptide in ProForma format
    pub fn peptide(&self) -> &str {
        &self.peptide
    }

    /// Returns the Xcorr score
    pub fn xcorr(&self) -> f64 {
        self.xcorr
    }

    /// Returns the total number of ions in the theoretical spectrum
    pub fn ions_total(&self) -> u64 {
        self.ions_total
    }

    /// Returns the number of matched ions between theoretical and observed spectrum
    pub fn ions_matched(&self) -> u64 {
        self.ions_matched
    }

    /// Returns the minimal theoretical mass of the peptide
    pub fn min_theoretical_mass(&self) -> f64 {
        self.theoretical_mass
    }

    /// Returns the maximal theoretical mass of the peptide
    pub fn max_theoretical_mass(&self) -> Option<f64> {
        self.second_theoretical_mass
    }
}

/// Collection of peptide-spectrum matches (PSMs) attributes.
/// Each attribute is stored in a separate column (Vec).
/// Which is easier to serialize and deserialize.
///
#[derive(Debug, Default, Clone, Deserialize, Serialize)]
pub struct PeptideSpectrumMatchCollection {
    #[serde(skip)]
    inner_index: Vec<usize>,
    #[serde(rename = "scan_id")]
    scan_id_col: Vec<String>,
    #[serde(rename = "peptide")]
    peptide_col: Vec<String>,
    #[serde(rename = "precursor")]
    precursor_col: Vec<Precursor>,
    #[serde(rename = "experimental_mass")]
    experimental_mass_col: Vec<f64>,
    #[serde(rename = "xcorr")]
    xcorr_col: Vec<f64>,
    #[serde(rename = "ions_total")]
    ions_total_col: Vec<u64>,
    #[serde(rename = "ions_matched")]
    ions_matched_col: Vec<u64>,
    #[serde(rename = "theoretical_mass")]
    theoretical_mass_col: Vec<f64>,
    #[serde(rename = "second_theoretical_mass")]
    second_theoretical_mass_col: Vec<Option<f64>>,
}

impl PeptideSpectrumMatchCollection {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            inner_index: Vec::with_capacity(capacity),
            scan_id_col: Vec::with_capacity(capacity),
            peptide_col: Vec::with_capacity(capacity),
            precursor_col: Vec::with_capacity(capacity),
            experimental_mass_col: Vec::with_capacity(capacity),
            xcorr_col: Vec::with_capacity(capacity),
            ions_total_col: Vec::with_capacity(capacity),
            ions_matched_col: Vec::with_capacity(capacity),
            theoretical_mass_col: Vec::with_capacity(capacity),
            second_theoretical_mass_col: Vec::with_capacity(capacity),
        }
    }

    pub fn push(&mut self, psm: PeptideSpectrumMatch) {
        self.inner_index.push(self.len());
        self.scan_id_col.push(psm.scan_id.into_owned());
        self.peptide_col.push(psm.peptide.into_owned());
        self.precursor_col.push(psm.precursor.into_owned());
        self.experimental_mass_col.push(psm.experimental_mass);
        self.xcorr_col.push(psm.xcorr);
        self.ions_total_col.push(psm.ions_total);
        self.ions_matched_col.push(psm.ions_matched);
        self.theoretical_mass_col.push(psm.theoretical_mass);
        self.second_theoretical_mass_col
            .push(psm.second_theoretical_mass);
    }

    pub fn len(&self) -> usize {
        self.scan_id_col.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn get(&self, index: usize) -> Option<PeptideSpectrumMatch<'_>> {
        if index >= self.len() {
            return None;
        }
        let index = self.inner_index[index];
        Some(PeptideSpectrumMatch {
            scan_id: Cow::Borrowed(&self.scan_id_col[index]),
            peptide: Cow::Borrowed(&self.peptide_col[index]),
            precursor: Cow::Borrowed(&self.precursor_col[index]),
            experimental_mass: self.experimental_mass_col[index],
            xcorr: self.xcorr_col[index],
            ions_total: self.ions_total_col[index],
            ions_matched: self.ions_matched_col[index],
            theoretical_mass: self.theoretical_mass_col[index],
            second_theoretical_mass: self.second_theoretical_mass_col[index],
        })
    }

    /// Sorts the PSMs by their Xcorr score in descending order
    pub fn sort(&mut self) {
        let mut indices = (0..self.len()).collect::<Vec<_>>();
        indices.sort_by(|i, j| {
            self.xcorr_col[*i]
                .partial_cmp(&self.xcorr_col[*j])
                .unwrap_or(Ordering::Equal)
        });
        indices.reverse();
        self.inner_index = indices;
    }
}

impl TryInto<DataFrame> for PeptideSpectrumMatchCollection {
    type Error = PolarsError;

    fn try_into(self) -> Result<DataFrame, Self::Error> {
        let mut precusor_mz: Vec<f64> = Vec::with_capacity(self.len());
        let mut precusor_charge: Vec<i16> = Vec::with_capacity(self.len());
        let mut precursor_da: Vec<f64> = Vec::with_capacity(self.len());
        for precursor in self.precursor_col.into_iter() {
            precursor_da.push(precursor.to_dalton());
            precusor_mz.push(precursor.mz());
            precusor_charge.push(precursor.charge() as i16);
        }

        let scan_id_series = Column::new("scan_id".into(), self.scan_id_col);
        let peptide_series = Column::new("peptide".into(), self.peptide_col);
        let charge_series = Column::new("charge".into(), precusor_charge);
        let precursor_mz_series = Column::new("precursor_mz".into(), precusor_mz);
        let precursor_da_series = Column::new("precursor_da".into(), precursor_da);
        let experimental_mass_series =
            Column::new("experimental_mass".into(), self.experimental_mass_col);
        let xcorr_series = Column::new("xcorr".into(), self.xcorr_col);
        let ions_total_series = Column::new("ions_total".into(), self.ions_total_col);
        let ions_matched_series = Column::new("ions_matched".into(), self.ions_matched_col);
        let min_theoretical_mass_series =
            Column::new("theoretical_mass".into(), self.theoretical_mass_col);
        let max_theoretical_mass_series = Column::new(
            "second_theoretical_mass".into(),
            self.second_theoretical_mass_col,
        );

        DataFrame::new(vec![
            scan_id_series,
            peptide_series,
            charge_series,
            precursor_mz_series,
            precursor_da_series,
            experimental_mass_series,
            xcorr_series,
            ions_total_series,
            ions_matched_series,
            min_theoretical_mass_series,
            max_theoretical_mass_series,
        ])
    }
}
