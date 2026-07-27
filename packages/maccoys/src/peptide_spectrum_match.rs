use std::{borrow::Cow, io::Write};

use dihardts_omicstools::mass_spectrometry::unit_conversions::mass_to_charge_to_dalton;
use local_outlier_probabilities::PointSource;
use polars::{io::mmap::MmapBytesReader, prelude::*};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use xcorrrs::scoring_result::ScoringResult;

use crate::{pipeline::tasks::scoring_task::FEATURES as LOOP_FEATURES, precursor::Precursor};

/// Key under which the [`PsmCollectionMetadata`] is stored in the parquet file's
/// key-value metadata (footer), since `scan_id`/`precursor`/`experimental_mass`
/// are not part of the columnar `values` DataFrame.
///
const PSM_METADATA_KEY: &str = "maccoys.psm_collection";

/// Errors that can occur while reading/writing a [`PeptideSpectrumMatchCollection`]
/// to/from a parquet file.
///
#[derive(Debug, Error)]
pub enum Error {
    #[error("Polars error:\n\t{0}")]
    Polars(#[from] polars::error::PolarsError),
    #[error("Failed to read parquet input:\n\t{0}")]
    Io(#[from] std::io::Error),
    #[error("Failed to (de)serialize PSM collection metadata:\n\t{0}")]
    Metadata(#[from] serde_json::Error),
    #[error("Parquet file is missing metadata")]
    MissingMetadata,
    #[error("Parquet file is missing the `{PSM_METADATA_KEY}` key-value metadata entry")]
    MissingPsmMetadata,
    #[error("Parquet file is missing the `{PSM_METADATA_KEY}` key-value metadata value")]
    MissingPsmMetadataValue,
    #[error("Parquet file is missing column `{0}` or it has an unexpected data type")]
    InvalidColumn(&'static str),
}

/// Scalar attributes of a [`PeptideSpectrumMatchCollection`] that are shared by
/// every row and therefore stored once as parquet file-level metadata instead of
/// as columns.
///
#[derive(Serialize, Deserialize)]
struct PsmCollectionMetadata {
    scan_id: String,
    precursor: Precursor,
    experimental_mass: f64,
}

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

/// Collection of peptide-spectrum matches (PSMs) for a single spectrum/precursor.
/// Each per-PSM attribute that actually varies between peptide candidates is stored
/// in its own column (Vec). `scan_id`, `precursor` and `experimental_mass` are
/// identical for every PSM in the collection (one collection == one spectrum), so
/// they're stored once instead of once per row.
///
#[derive(Debug)]
pub struct PeptideSpectrumMatchCollectionBuilder {
    scan_id: String,
    precursor: Precursor,
    experimental_mass: f64,
    peptide_col: Vec<String>,
    xcorr_col: Vec<f64>,
    ions_total_col: Vec<u64>,
    ions_matched_col: Vec<u64>,
    theoretical_mass_col: Vec<f64>,
    second_theoretical_mass_col: Vec<Option<f64>>,
}

impl PeptideSpectrumMatchCollectionBuilder {
    /// Creates a new, empty collection for the given scan/precursor.
    ///
    /// # Arguments
    /// * `scan_id` - Scan ID shared by every PSM in this collection
    /// * `precursor` - Precursor shared by every PSM in this collection
    ///
    pub fn new(scan_id: String, precursor: Precursor) -> Self {
        Self::with_capacity(scan_id, precursor, 0)
    }

    /// Creates a new, empty collection for the given scan/precursor with preallocated capacity.
    ///
    /// # Arguments
    /// * `scan_id` - Scan ID shared by every PSM in this collection
    /// * `precursor` - Precursor shared by every PSM in this collection
    /// * `capacity` - Number of PSMs to preallocate space for
    ///
    pub fn with_capacity(scan_id: String, precursor: Precursor, capacity: usize) -> Self {
        let experimental_mass = mass_to_charge_to_dalton(precursor.mz(), precursor.charge());
        Self {
            scan_id,
            precursor,
            experimental_mass,
            peptide_col: Vec::with_capacity(capacity),
            xcorr_col: Vec::with_capacity(capacity),
            ions_total_col: Vec::with_capacity(capacity),
            ions_matched_col: Vec::with_capacity(capacity),
            theoretical_mass_col: Vec::with_capacity(capacity),
            second_theoretical_mass_col: Vec::with_capacity(capacity),
        }
    }

    /// Adds a PSM for the given peptide/scoring result to the collection.
    ///
    /// # Arguments
    /// * `peptide` - Amino acid sequence of the peptide in ProForma format
    /// * `scoring_result` - Result of the scoring
    ///
    pub fn push(&mut self, peptide: String, scoring_result: ScoringResult) {
        self.peptide_col.push(peptide);
        self.xcorr_col.push(scoring_result.score);
        self.ions_total_col.push(scoring_result.ions_total as u64);
        self.ions_matched_col
            .push(scoring_result.ions_matched as u64);
        self.theoretical_mass_col
            .push(scoring_result.min_theoretical_mass);
        self.second_theoretical_mass_col.push(
            if scoring_result.min_theoretical_mass == scoring_result.max_theoretical_mass {
                None
            } else {
                Some(scoring_result.max_theoretical_mass)
            },
        );
    }

    pub fn len(&self) -> usize {
        self.peptide_col.len()
    }

    pub fn is_empty(&self) -> bool {
        self.peptide_col.is_empty()
    }
}

#[derive(Clone, PartialEq, Deserialize, Serialize)]
pub struct PeptideSpectrumMatchCollection {
    scan_id: String,
    precursor: Precursor,
    experimental_mass: f64,
    values: DataFrame,
}

impl PeptideSpectrumMatchCollection {
    pub fn new(
        scan_id: String,
        precursor: Precursor,
        experimental_mass: f64,
        values: DataFrame,
    ) -> Self {
        Self {
            scan_id,
            precursor,
            experimental_mass,
            values,
        }
    }

    pub fn scan_id(&self) -> &str {
        &self.scan_id
    }

    pub fn precursor(&self) -> &Precursor {
        &self.precursor
    }

    pub fn experimental_mass(&self) -> f64 {
        self.experimental_mass
    }

    pub fn values(&self) -> &DataFrame {
        &self.values
    }

    pub(crate) fn values_mut(&mut self) -> &mut DataFrame {
        &mut self.values
    }

    pub fn len(&self) -> usize {
        self.values.height()
    }

    pub fn is_empty(&self) -> bool {
        self.values.height() == 0
    }

    pub fn estimated_size(&self) -> usize {
        self.scan_id.len() + self.precursor.size() + self.values.estimated_size()
    }

    pub async fn write_parquet<W: Write + Send>(&mut self, writer: W) -> Result<(), Error> {
        let metadata = PsmCollectionMetadata {
            scan_id: self.scan_id.clone(),
            precursor: self.precursor.clone(),
            experimental_mass: self.experimental_mass,
        };

        let metadata = KeyValueMetadata::from_static(vec![(
            PSM_METADATA_KEY.to_string(),
            serde_json::to_string(&metadata)?,
        )]);

        ParquetWriter::new(writer)
            .with_compression(ParquetCompression::Zstd(None))
            .with_key_value_metadata(Some(metadata))
            .finish(&mut self.values)
            .unwrap();

        Ok(())
    }

    pub async fn read_parquet<R: MmapBytesReader>(reader: R) -> Result<Self, Error> {
        let mut parquet_reader = ParquetReader::new(reader);

        let file_metadata = parquet_reader
            .get_metadata()?
            .key_value_metadata()
            .as_ref()
            .ok_or(Error::MissingMetadata)?;

        let metadata = file_metadata
            .iter()
            .find(|elem| elem.key == PSM_METADATA_KEY)
            .ok_or(Error::MissingPsmMetadata)?
            .value
            .as_ref()
            .map(|value| serde_json::from_str::<PsmCollectionMetadata>(value))
            .ok_or(Error::MissingPsmMetadataValue)??;

        let values = parquet_reader.finish()?;

        Ok(Self {
            scan_id: metadata.scan_id,
            precursor: metadata.precursor,
            experimental_mass: metadata.experimental_mass,
            values,
        })
    }
}

impl TryFrom<PeptideSpectrumMatchCollectionBuilder> for PeptideSpectrumMatchCollection {
    type Error = PolarsError;

    fn try_from(builder: PeptideSpectrumMatchCollectionBuilder) -> Result<Self, Self::Error> {
        let len = builder.len();
        let peptide_series = Column::new("peptide".into(), builder.peptide_col);
        let xcorr_series = Column::new("xcorr".into(), builder.xcorr_col);
        let ions_total_series = Column::new("ions_total".into(), builder.ions_total_col);
        let ions_matched_series = Column::new("ions_matched".into(), builder.ions_matched_col);
        let min_theoretical_mass_series =
            Column::new("theoretical_mass".into(), builder.theoretical_mass_col);
        let max_theoretical_mass_series = Column::new(
            "second_theoretical_mass".into(),
            builder.second_theoretical_mass_col,
        );

        let dataframe = DataFrame::new(
            len,
            vec![
                peptide_series,
                xcorr_series,
                ions_total_series,
                ions_matched_series,
                min_theoretical_mass_series,
                max_theoretical_mass_series,
            ],
        )?;

        Ok(PeptideSpectrumMatchCollection {
            scan_id: builder.scan_id,
            precursor: builder.precursor,
            experimental_mass: builder.experimental_mass,
            values: dataframe,
        })
    }
}

impl PointSource<f64> for PeptideSpectrumMatchCollection {
    fn len(&self) -> usize {
        self.values.height()
    }

    fn dim(&self) -> usize {
        LOOP_FEATURES.len()
    }

    fn point(
        &self,
        idx: usize,
    ) -> Result<Cow<'_, [f64]>, local_outlier_probabilities::error::Error> {
        let loop_feature_values = self.values.select(LOOP_FEATURES).map_err(|err| {
            local_outlier_probabilities::error::Error::PointExtraction(format!(
                "Unable to select local outlier probability features {:?}: {}",
                LOOP_FEATURES, err
            ))
        })?;

        let row = loop_feature_values.get_row(idx).map_err(|err| {
            local_outlier_probabilities::error::Error::PointExtraction(err.to_string())
        })?;

        let values = row
            .0
            .iter()
            .map(|value| {
                value.extract::<f64>().ok_or_else(|| {
                    local_outlier_probabilities::error::Error::PointExtraction(format!(
                        "could not cast {value:?} to f64"
                    ))
                })
            })
            .collect::<Result<Vec<f64>, local_outlier_probabilities::error::Error>>()?;

        Ok(Cow::Owned(values))
    }
}
