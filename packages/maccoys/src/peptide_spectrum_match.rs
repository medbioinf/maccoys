use std::{borrow::Cow, io::Write, sync::Arc};

use dihardts_omicstools::mass_spectrometry::unit_conversions::mass_to_charge_to_dalton;
use local_outlier_probabilities::PointSource;
use parquet::{
    column::reader::ColumnReaderImpl,
    data_type::{ByteArray, ByteArrayType, DataType, DoubleType, Int64Type},
    file::{
        metadata::KeyValue,
        properties::WriterProperties,
        reader::{ChunkReader, FileReader, RowGroupReader, SerializedFileReader},
        writer::SerializedFileWriter,
    },
    schema::parser::parse_message_type,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use xcorrrs::scoring_result::ScoringResult;

use crate::{pipeline::tasks::scoring_task::FEATURES as LOOP_FEATURES, precursor::Precursor};

/// Key under which the [`PsmCollectionMetadata`] is stored in the parquet file's
/// key-value metadata (footer), since `scan_id`/`precursor`/`experimental_mass`
/// are not part of the columnar `values` DataFrame.
///
const PSM_METADATA_KEY: &str = "maccoys.psm_collection";

/// Parquet schema for the per-PSM columns of a [`PeptideSpectrumMatchCollection`].
/// Column order here fixes the column index used by both [`PeptideSpectrumMatchCollection::write_parquet`]
/// and [`PeptideSpectrumMatchCollection::read_parquet`].
///
const PSM_PARQUET_SCHEMA: &str = "
    message psm_collection {
        REQUIRED BYTE_ARRAY peptide (UTF8);
        REQUIRED DOUBLE xcorr;
        REQUIRED INT64 ions_total;
        REQUIRED INT64 ions_matched;
        REQUIRED DOUBLE theoretical_mass;
        OPTIONAL DOUBLE second_theoretical_mass;
        OPTIONAL DOUBLE loop_score;
    }
";

/// Errors that can occur while reading/writing a [`PeptideSpectrumMatchCollection`]
/// to/from a parquet file.
///
#[derive(Debug, Error)]
pub enum Error {
    #[error("Failed to read parquet input:\n\t{0}")]
    Io(#[from] std::io::Error),
    #[error("Parquet error:\n\t{0}")]
    Parquet(#[from] parquet::errors::ParquetError),
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
    /// Loop score
    loop_score: Option<f64>,
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

    /// Returns the loop score
    pub fn loop_score(&self) -> Option<f64> {
        self.loop_score
    }

    /// Returns the ratio of matched ions to total ions
    pub fn ions_matched_ratio(&self) -> u64 {
        self.ions_matched.checked_div(self.ions_total).unwrap_or(0)
    }

    pub fn mass_error(&self) -> f64 {
        self.experimental_mass - self.theoretical_mass
    }
}

/// Collection of peptide-spectrum matches (PSMs) for a single spectrum/precursor.
/// Each per-PSM attribute that actually varies between peptide candidates is stored
/// in its own column (Vec). `scan_id`, `precursor` and `experimental_mass` are
/// identical for every PSM in the collection (one collection == one spectrum), so
/// they're stored once instead of once per row.
///
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct PeptideSpectrumMatchCollection {
    scan_id: String,
    precursor: Precursor,
    experimental_mass: f64,
    peptide_col: Vec<String>,
    xcorr_col: Vec<f64>,
    ions_total_col: Vec<u64>,
    ions_matched_col: Vec<u64>,
    theoretical_mass_col: Vec<f64>,
    second_theoretical_mass_col: Vec<Option<f64>>,
    loop_score_col: Option<Vec<f64>>,
}

impl PeptideSpectrumMatchCollection {
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
            loop_score_col: None,
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

    pub fn scan_id(&self) -> &str {
        &self.scan_id
    }

    pub fn precursor(&self) -> &Precursor {
        &self.precursor
    }

    pub fn experimental_mass(&self) -> f64 {
        self.experimental_mass
    }

    pub fn peptide_col(&self) -> &[String] {
        &self.peptide_col
    }

    pub fn xcorr_col(&self) -> &[f64] {
        &self.xcorr_col
    }

    pub fn ions_total_col(&self) -> &[u64] {
        &self.ions_total_col
    }

    pub fn ions_matched_col(&self) -> &[u64] {
        &self.ions_matched_col
    }

    pub fn theoretical_mass_col(&self) -> &[f64] {
        &self.theoretical_mass_col
    }

    pub fn second_theoretical_mass_col(&self) -> &[Option<f64>] {
        &self.second_theoretical_mass_col
    }

    pub fn loop_score_col(&self) -> Option<&[f64]> {
        self.loop_score_col.as_deref()
    }

    pub fn set_loop_score_col(&mut self, loop_score_col: Vec<f64>) {
        self.loop_score_col = Some(loop_score_col);
    }

    pub fn size(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.scan_id.capacity()
            + self.precursor.size()
            + self.peptide_col.capacity() * std::mem::size_of::<String>()
            + self
                .peptide_col
                .iter()
                .map(|peptide| peptide.capacity())
                .sum::<usize>()
            + self.xcorr_col.capacity() * std::mem::size_of::<f64>()
            + self.ions_total_col.capacity() * std::mem::size_of::<u64>()
            + self.ions_matched_col.capacity() * std::mem::size_of::<u64>()
            + self.theoretical_mass_col.capacity() * std::mem::size_of::<f64>()
            + self.second_theoretical_mass_col.capacity() * std::mem::size_of::<Option<f64>>()
            + self
                .loop_score_col
                .as_ref()
                .map_or(0, |col| col.capacity() * std::mem::size_of::<f64>())
    }

    pub async fn write_parquet<W: Write + Send>(&self, writer: W) -> Result<(), Error> {
        let metadata = PsmCollectionMetadata {
            scan_id: self.scan_id.clone(),
            precursor: self.precursor.clone(),
            experimental_mass: self.experimental_mass,
        };

        let schema = Arc::new(parse_message_type(PSM_PARQUET_SCHEMA)?);
        let properties = Arc::new(
            WriterProperties::builder()
                .set_key_value_metadata(Some(vec![KeyValue::new(
                    PSM_METADATA_KEY.to_string(),
                    serde_json::to_string(&metadata)?,
                )]))
                .build(),
        );

        let mut file_writer = SerializedFileWriter::new(writer, schema, properties)?;
        let mut row_group_writer = file_writer.next_row_group()?;

        let mut column_writer = row_group_writer
            .next_column()?
            .expect("psm_collection schema should have a `peptide` column");
        let peptide_values: Vec<ByteArray> = self
            .peptide_col
            .iter()
            .map(|peptide| ByteArray::from(peptide.as_str()))
            .collect();
        column_writer
            .typed::<ByteArrayType>()
            .write_batch(&peptide_values, None, None)?;
        column_writer.close()?;

        let mut column_writer = row_group_writer
            .next_column()?
            .expect("psm_collection schema should have an `xcorr` column");
        column_writer
            .typed::<DoubleType>()
            .write_batch(&self.xcorr_col, None, None)?;
        column_writer.close()?;

        let mut column_writer = row_group_writer
            .next_column()?
            .expect("psm_collection schema should have an `ions_total` column");
        let ions_total_values: Vec<i64> = self
            .ions_total_col
            .iter()
            .map(|value| *value as i64)
            .collect();
        column_writer
            .typed::<Int64Type>()
            .write_batch(&ions_total_values, None, None)?;
        column_writer.close()?;

        let mut column_writer = row_group_writer
            .next_column()?
            .expect("psm_collection schema should have an `ions_matched` column");
        let ions_matched_values: Vec<i64> = self
            .ions_matched_col
            .iter()
            .map(|value| *value as i64)
            .collect();
        column_writer
            .typed::<Int64Type>()
            .write_batch(&ions_matched_values, None, None)?;
        column_writer.close()?;

        let mut column_writer = row_group_writer
            .next_column()?
            .expect("psm_collection schema should have a `theoretical_mass` column");
        column_writer
            .typed::<DoubleType>()
            .write_batch(&self.theoretical_mass_col, None, None)?;
        column_writer.close()?;

        let mut column_writer = row_group_writer
            .next_column()?
            .expect("psm_collection schema should have a `second_theoretical_mass` column");
        let mut second_mass_values = Vec::new();
        let mut second_mass_def_levels = Vec::with_capacity(self.len());
        for value in &self.second_theoretical_mass_col {
            match value {
                Some(value) => {
                    second_mass_values.push(*value);
                    second_mass_def_levels.push(1i16);
                }
                None => second_mass_def_levels.push(0i16),
            }
        }
        column_writer.typed::<DoubleType>().write_batch(
            &second_mass_values,
            Some(&second_mass_def_levels),
            None,
        )?;
        column_writer.close()?;

        let mut column_writer = row_group_writer
            .next_column()?
            .expect("psm_collection schema should have a `loop_score` column");
        match &self.loop_score_col {
            Some(loop_score_col) => {
                let def_levels = vec![1i16; loop_score_col.len()];
                column_writer.typed::<DoubleType>().write_batch(
                    loop_score_col,
                    Some(&def_levels),
                    None,
                )?;
            }
            None => {
                let def_levels = vec![0i16; self.len()];
                column_writer
                    .typed::<DoubleType>()
                    .write_batch(&[], Some(&def_levels), None)?;
            }
        }
        column_writer.close()?;

        row_group_writer.close()?;
        file_writer.close()?;

        Ok(())
    }

    pub async fn read_parquet<R: ChunkReader + 'static>(reader: R) -> Result<Self, Error> {
        let file_reader = SerializedFileReader::new(reader)?;

        let metadata_json = file_reader
            .metadata()
            .file_metadata()
            .key_value_metadata()
            .ok_or(Error::MissingMetadata)?
            .iter()
            .find(|kv| kv.key == PSM_METADATA_KEY)
            .ok_or(Error::MissingPsmMetadata)?
            .value
            .as_ref()
            .ok_or(Error::MissingPsmMetadataValue)?;
        let metadata: PsmCollectionMetadata = serde_json::from_str(metadata_json)?;

        let row_group_reader = file_reader.get_row_group(0)?;
        let num_rows = row_group_reader.metadata().num_rows() as usize;
        let row_group_reader = row_group_reader.as_ref();

        let mut peptide_values = Vec::with_capacity(num_rows);
        Self::typed_column_reader::<ByteArrayType>(row_group_reader, 0, "peptide")?.read_records(
            num_rows,
            None,
            None,
            &mut peptide_values,
        )?;
        let peptide_col = peptide_values
            .into_iter()
            .map(|value| Ok(value.as_utf8()?.to_string()))
            .collect::<Result<Vec<String>, Error>>()?;

        let mut xcorr_col = Vec::with_capacity(num_rows);
        Self::typed_column_reader::<DoubleType>(row_group_reader, 1, "xcorr")?.read_records(
            num_rows,
            None,
            None,
            &mut xcorr_col,
        )?;

        let mut ions_total_raw = Vec::with_capacity(num_rows);
        Self::typed_column_reader::<Int64Type>(row_group_reader, 2, "ions_total")?.read_records(
            num_rows,
            None,
            None,
            &mut ions_total_raw,
        )?;
        let ions_total_col: Vec<u64> = ions_total_raw
            .into_iter()
            .map(|value| value as u64)
            .collect();

        let mut ions_matched_raw = Vec::with_capacity(num_rows);
        Self::typed_column_reader::<Int64Type>(row_group_reader, 3, "ions_matched")?.read_records(
            num_rows,
            None,
            None,
            &mut ions_matched_raw,
        )?;
        let ions_matched_col: Vec<u64> = ions_matched_raw
            .into_iter()
            .map(|value| value as u64)
            .collect();

        let mut theoretical_mass_col = Vec::with_capacity(num_rows);
        Self::typed_column_reader::<DoubleType>(row_group_reader, 4, "theoretical_mass")?
            .read_records(num_rows, None, None, &mut theoretical_mass_col)?;

        let mut second_mass_values = Vec::new();
        let mut second_mass_def_levels = Vec::with_capacity(num_rows);
        Self::typed_column_reader::<DoubleType>(row_group_reader, 5, "second_theoretical_mass")?
            .read_records(
                num_rows,
                Some(&mut second_mass_def_levels),
                None,
                &mut second_mass_values,
            )?;
        let mut second_mass_values = second_mass_values.into_iter();
        let second_theoretical_mass_col: Vec<Option<f64>> = second_mass_def_levels
            .into_iter()
            .map(|def_level| {
                if def_level == 1 {
                    second_mass_values.next()
                } else {
                    None
                }
            })
            .collect();

        let mut loop_score_values = Vec::new();
        let mut loop_score_def_levels = Vec::with_capacity(num_rows);
        Self::typed_column_reader::<DoubleType>(row_group_reader, 6, "loop_score")?.read_records(
            num_rows,
            Some(&mut loop_score_def_levels),
            None,
            &mut loop_score_values,
        )?;
        let loop_score_col = if loop_score_def_levels
            .iter()
            .all(|&def_level| def_level == 0)
        {
            None
        } else {
            let mut loop_score_values = loop_score_values.into_iter();
            Some(
                loop_score_def_levels
                    .into_iter()
                    .map(|_| {
                        loop_score_values
                            .next()
                            .expect("loop_score column value/level count mismatch")
                    })
                    .collect(),
            )
        };

        Ok(Self {
            scan_id: metadata.scan_id,
            precursor: metadata.precursor,
            experimental_mass: metadata.experimental_mass,
            peptide_col,
            xcorr_col,
            ions_total_col,
            ions_matched_col,
            theoretical_mass_col,
            second_theoretical_mass_col,
            loop_score_col,
        })
    }

    /// Returns the column reader for the `index`-th leaf column of the row group as `T`, or
    /// [`Error::InvalidColumn`] if the column is missing or has an unexpected physical type.
    ///
    fn typed_column_reader<T: DataType>(
        row_group_reader: &dyn RowGroupReader,
        index: usize,
        column_name: &'static str,
    ) -> Result<ColumnReaderImpl<T>, Error> {
        T::get_column_reader(row_group_reader.get_column_reader(index)?)
            .ok_or(Error::InvalidColumn(column_name))
    }
}

impl PointSource<f64> for PeptideSpectrumMatchCollection {
    fn len(&self) -> usize {
        self.peptide_col.len()
    }

    fn dim(&self) -> usize {
        LOOP_FEATURES.len()
    }

    fn point(
        &self,
        idx: usize,
    ) -> Result<Cow<'_, [f64]>, local_outlier_probabilities::error::Error> {
        let values = vec![
            self.xcorr_col[idx],
            self.ions_matched_col[idx] as f64 / self.ions_total_col[idx] as f64,
            (self.theoretical_mass_col[idx] - self.experimental_mass).abs(),
        ];

        Ok(Cow::Owned(values))
    }
}

#[cfg(test)]
mod tests {
    use std::{env::temp_dir, fs::File};

    use super::*;

    fn scoring_result(
        score: f64,
        ions_total: usize,
        ions_matched: usize,
        min_theoretical_mass: f64,
        max_theoretical_mass: f64,
    ) -> ScoringResult {
        ScoringResult {
            score,
            ions_total,
            ions_matched,
            min_theoretical_mass,
            max_theoretical_mass,
        }
    }

    #[tokio::test]
    async fn test_psm_collection_parquet_roundtrip() {
        let mut psms =
            PeptideSpectrumMatchCollection::new("scan_1".to_string(), Precursor::new(500.25, 2));
        // Ambiguous mass -> `second_theoretical_mass_col` entry is `Some(_)`
        psms.push(
            "PEPTIDE".to_string(),
            scoring_result(1.23, 10, 7, 999.5, 999.9),
        );
        // Unambiguous mass -> `second_theoretical_mass_col` entry is `None`
        psms.push(
            "PEPTIDEK".to_string(),
            scoring_result(0.98, 12, 9, 1000.1, 1000.1),
        );
        psms.set_loop_score_col(vec![0.1, 0.2]);

        let path = temp_dir()
            .join("maccoys.tests.peptide_spectrum_match.psm_collection_roundtrip.parquet");
        psms.write_parquet(File::create(&path).unwrap())
            .await
            .unwrap();
        let read_back = PeptideSpectrumMatchCollection::read_parquet(File::open(&path).unwrap())
            .await
            .unwrap();
        std::fs::remove_file(&path).ok();

        assert_eq!(psms, read_back);
    }

    #[tokio::test]
    async fn test_psm_collection_parquet_roundtrip_without_loop_score() {
        let mut psms =
            PeptideSpectrumMatchCollection::new("scan_2".to_string(), Precursor::new(600.5, 3));
        psms.push(
            "SEQVENCE".to_string(),
            scoring_result(2.5, 8, 4, 800.0, 800.0),
        );

        let path = temp_dir().join(
            "maccoys.tests.peptide_spectrum_match.psm_collection_roundtrip_without_loop_score.parquet",
        );
        psms.write_parquet(File::create(&path).unwrap())
            .await
            .unwrap();
        let read_back = PeptideSpectrumMatchCollection::read_parquet(File::open(&path).unwrap())
            .await
            .unwrap();
        std::fs::remove_file(&path).ok();

        assert_eq!(read_back.loop_score_col(), None);
        assert_eq!(psms, read_back);
    }
}
