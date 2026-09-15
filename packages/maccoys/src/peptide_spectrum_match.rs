use std::{borrow::Cow, io::Write, sync::Arc};

use dihardts_omicstools::mass_spectrometry::unit_conversions::mass_to_charge_to_dalton;
use local_outlier_probabilities::PointSource;
use parquet::{
    basic::{Compression, ZstdLevel},
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

use crate::{
    peptide_arena::PeptideArena, pipeline::tasks::scoring_task::FEATURES as LOOP_FEATURES,
    precursor::Precursor,
};

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
    #[error("Peptide (incl. ProForma modification annotations) is too long:\n\t{0}")]
    PeptideTooLong(#[from] crate::peptide_arena::SequenceTooLongError),
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
/// Peptides are stored in a [`PeptideArena`] rather than as `Vec<String>`, so at up
/// to ~1M rows this avoids one heap allocation per peptide.
///
/// `second_theoretical_mass_col` uses `f64::NAN` as a sentinel for "no second mass"
/// instead of `Option<f64>`, which has no niche for `f64` and so costs 16 bytes/row
/// instead of 8.
///
#[derive(Debug, Deserialize, Serialize)]
pub struct PeptideSpectrumMatchCollection {
    scan_id: String,
    precursor: Precursor,
    experimental_mass: f64,
    peptides: PeptideArena,
    xcorr_col: Vec<f64>,
    ions_total_col: Vec<u64>,
    ions_matched_col: Vec<u64>,
    theoretical_mass_col: Vec<f64>,
    second_theoretical_mass_col: Vec<f64>,
    loop_score_col: Option<Vec<f64>>,
    /// Flat, row-major cache of the 3 LoOP features per row (row `idx` at
    /// `[idx*3..idx*3+3]`), built once by [`Self::prepare_loop_features`] so
    /// [`PointSource::point`] can borrow instead of allocating a `Vec` per call. Derived
    /// from the other columns, so it's excluded from `PartialEq` and never (de)serialized.
    #[serde(skip)]
    loop_feature_buffer: Option<Vec<f64>>,
}

impl PartialEq for PeptideSpectrumMatchCollection {
    fn eq(&self, other: &Self) -> bool {
        // `xcorr_col`/`theoretical_mass_col`/`second_theoretical_mass_col` are seeded
        // with the `f64::NAN` sentinel (`with_capacity`) for "no result yet"/"no second
        // mass", and a computed `loop_score` could in principle also be `NaN`; `NaN !=
        // NaN` under `==`, so compare bit patterns instead everywhere an `f64` column
        // is involved.
        fn f64_cols_eq(a: &[f64], b: &[f64]) -> bool {
            a.len() == b.len() && a.iter().zip(b).all(|(a, b)| a.to_bits() == b.to_bits())
        }

        self.scan_id == other.scan_id
            && self.precursor == other.precursor
            && self.experimental_mass == other.experimental_mass
            && self.peptides == other.peptides
            && f64_cols_eq(&self.xcorr_col, &other.xcorr_col)
            && self.ions_total_col == other.ions_total_col
            && self.ions_matched_col == other.ions_matched_col
            && f64_cols_eq(&self.theoretical_mass_col, &other.theoretical_mass_col)
            && f64_cols_eq(
                &self.second_theoretical_mass_col,
                &other.second_theoretical_mass_col,
            )
            && match (&self.loop_score_col, &other.loop_score_col) {
                (Some(a), Some(b)) => f64_cols_eq(a, b),
                (None, None) => true,
                _ => false,
            }
    }
}

impl PeptideSpectrumMatchCollection {
    /// Creates a new, empty collection for the given scan/precursor with preallocated capacity.
    ///
    /// # Arguments
    /// * `scan_id` - Scan ID shared by every PSM in this collection
    /// * `precursor` - Precursor shared by every PSM in this collection
    /// * `peptides` - Peptides to preallocate space for
    ///
    pub fn with_capacity(scan_id: String, precursor: Precursor, peptides: PeptideArena) -> Self {
        let experimental_mass = mass_to_charge_to_dalton(precursor.mz(), precursor.charge());
        Self {
            scan_id,
            precursor,
            experimental_mass,
            xcorr_col: vec![f64::NAN; peptides.len()],
            ions_total_col: vec![0; peptides.len()],
            ions_matched_col: vec![0; peptides.len()],
            theoretical_mass_col: vec![f64::NAN; peptides.len()],
            second_theoretical_mass_col: vec![f64::NAN; peptides.len()],
            loop_score_col: None,
            loop_feature_buffer: None,
            peptides,
        }
    }

    /// Returns an iterator over this collection's peptides (O(n) total, via
    /// [`PeptideArena::iter`]) together with mutable slices into every scoring-result
    /// column, for callers that want to fill in scores directly by index (e.g. from a
    /// parallel iterator).
    ///
    #[allow(clippy::type_complexity)]
    pub(crate) fn peptides_and_columns_mut(
        &mut self,
    ) -> (
        impl Iterator<Item = &str>,
        &mut [f64],
        &mut [u64],
        &mut [u64],
        &mut [f64],
        &mut [f64],
    ) {
        (
            self.peptides.iter(),
            &mut self.xcorr_col,
            &mut self.ions_total_col,
            &mut self.ions_matched_col,
            &mut self.theoretical_mass_col,
            &mut self.second_theoretical_mass_col,
        )
    }

    /// Drops every row whose `xcorr_col` entry is still the `f64::NAN` "no result yet"
    /// sentinel (see [`Self::with_capacity`]) — i.e. every candidate this collection
    /// preallocated a row for but that a caller filling it via
    /// [`Self::peptides_and_columns_mut`] never actually wrote a result into, e.g.
    /// because its score was below a threshold or scoring it errored. Keeps the
    /// relative order of the remaining rows.
    ///
    /// Callers that only conditionally write a row's columns (as `run_xcorr_search`
    /// does) must call this before returning/publishing the collection, or every
    /// unwritten candidate ends up in the result as a bogus all-NaN/all-zero row.
    ///
    /// Must be called before [`Self::set_loop_score_col`]; panics (in debug builds)
    /// if `loop_score_col` is already set, since this method doesn't filter it.
    ///
    pub fn retain_scored(&mut self) {
        debug_assert!(
            self.loop_score_col.is_none(),
            "retain_scored must be called before set_loop_score_col"
        );

        let keep: Vec<bool> = self.xcorr_col.iter().map(|x| !x.is_nan()).collect();
        self.retain_by_keep_mask(&keep);
    }

    /// Keeps only the `max_len` rows with the highest `xcorr`, dropping the rest, to bound
    /// how many candidates ever reach LoOP scoring for a single spectrum (defense-in-depth
    /// against a pathologically large search space). No-op if the collection already has
    /// `max_len` rows or fewer.
    ///
    /// Ties at the cut-off `xcorr` value are all kept, so the result can be slightly larger
    /// than `max_len`.
    ///
    /// Must be called before [`Self::set_loop_score_col`], like [`Self::retain_scored`].
    ///
    pub fn truncate_to_top_by_xcorr(&mut self, max_len: usize) {
        debug_assert!(
            self.loop_score_col.is_none(),
            "truncate_to_top_by_xcorr must be called before set_loop_score_col"
        );

        if self.len() <= max_len {
            return;
        }

        if max_len == 0 {
            self.retain_by_keep_mask(&vec![false; self.len()]);
            return;
        }

        let mut sorted_xcorr = self.xcorr_col.clone();
        sorted_xcorr.select_nth_unstable_by(max_len - 1, |a, b| b.total_cmp(a));
        let threshold = sorted_xcorr[max_len - 1];

        let keep: Vec<bool> = self.xcorr_col.iter().map(|x| *x >= threshold).collect();
        self.retain_by_keep_mask(&keep);
    }

    /// Filters `peptides` and every plain-`Vec` column in lockstep with `keep`, keeping the
    /// relative order of the retained rows. Shared by [`Self::retain_scored`] and
    /// [`Self::truncate_to_top_by_xcorr`].
    ///
    fn retain_by_keep_mask(&mut self, keep: &[bool]) {
        self.peptides.retain(keep);

        // Filters a column `Vec` in lockstep with `keep`, using an external index
        // counter into the mask rather than duplicating this loop 5 times.
        fn retain_by_mask<T>(col: &mut Vec<T>, keep: &[bool]) {
            debug_assert_eq!(keep.len(), col.len());
            let mut keep_iter = keep.iter();
            col.retain(|_| *keep_iter.next().unwrap());
        }

        retain_by_mask(&mut self.xcorr_col, keep);
        retain_by_mask(&mut self.ions_total_col, keep);
        retain_by_mask(&mut self.ions_matched_col, keep);
        retain_by_mask(&mut self.theoretical_mass_col, keep);
        retain_by_mask(&mut self.second_theoretical_mass_col, keep);
    }

    /// Builds the flat, row-major LoOP-feature cache consumed by [`PointSource::point`],
    /// from the current (final) row set. Must be called after any row-count-changing
    /// operation (e.g. [`Self::retain_scored`] / [`Self::truncate_to_top_by_xcorr`]) and
    /// before this collection is passed to `local_outlier_probabilities`.
    ///
    pub fn prepare_loop_features(&mut self) {
        let mut buffer = Vec::with_capacity(self.len() * 3);
        for idx in 0..self.len() {
            buffer.push(self.xcorr_col[idx]);
            buffer.push(self.ions_matched_col[idx] as f64 / self.ions_total_col[idx] as f64);
            buffer.push((self.theoretical_mass_col[idx] - self.experimental_mass).abs());
        }
        self.loop_feature_buffer = Some(buffer);
    }

    pub fn len(&self) -> usize {
        self.peptides.len()
    }

    pub fn is_empty(&self) -> bool {
        self.peptides.is_empty()
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

    /// Returns the peptide of the `idx`-th PSM in the collection.
    ///
    /// O(idx); prefer [`Self::peptides`] over calling this in a loop.
    ///
    pub fn peptide(&self, idx: usize) -> &str {
        self.peptides.get(idx)
    }

    /// Returns an iterator over the peptides of every PSM in the collection, in
    /// order. O(n) total, unlike calling [`Self::peptide`] in a loop.
    ///
    pub fn peptides(&self) -> impl Iterator<Item = &str> {
        self.peptides.iter()
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

    /// Returns the raw second-theoretical-mass column, using `f64::NAN` as the
    /// "no second mass" sentinel (see [`Self::second_theoretical_mass`] for the
    /// `Option<f64>` view of a single row).
    pub fn second_theoretical_mass_col(&self) -> &[f64] {
        &self.second_theoretical_mass_col
    }

    /// Returns the second theoretical mass of the `idx`-th PSM, or `None` if the
    /// peptide's mass is unambiguous.
    pub fn second_theoretical_mass(&self, idx: usize) -> Option<f64> {
        let value = self.second_theoretical_mass_col[idx];
        if value.is_nan() {
            None
        } else {
            Some(value)
        }
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
            + self.peptides.size()
            + self.xcorr_col.capacity() * std::mem::size_of::<f64>()
            + self.ions_total_col.capacity() * std::mem::size_of::<u64>()
            + self.ions_matched_col.capacity() * std::mem::size_of::<u64>()
            + self.theoretical_mass_col.capacity() * std::mem::size_of::<f64>()
            + self.second_theoretical_mass_col.capacity() * std::mem::size_of::<f64>()
            + self
                .loop_score_col
                .as_ref()
                .map_or(0, |col| col.capacity() * std::mem::size_of::<f64>())
            + self
                .loop_feature_buffer
                .as_ref()
                .map_or(0, |buffer| buffer.capacity() * std::mem::size_of::<f64>())
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
                .set_compression(Compression::ZSTD(ZstdLevel::default()))
                .build(),
        );

        let mut file_writer = SerializedFileWriter::new(writer, schema, properties)?;
        let mut row_group_writer = file_writer.next_row_group()?;

        let mut column_writer = row_group_writer
            .next_column()?
            .expect("psm_collection schema should have a `peptide` column");
        let peptide_values: Vec<ByteArray> = self.peptides().map(ByteArray::from).collect();
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
        for &value in &self.second_theoretical_mass_col {
            if value.is_nan() {
                second_mass_def_levels.push(0i16);
            } else {
                second_mass_values.push(value);
                second_mass_def_levels.push(1i16);
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
        let mut peptides = PeptideArena::with_capacity(num_rows);
        for value in peptide_values {
            peptides.try_push(value.as_utf8()?)?;
        }

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
        let second_theoretical_mass_col: Vec<f64> = second_mass_def_levels
            .into_iter()
            .map(|def_level| {
                if def_level == 1 {
                    second_mass_values
                        .next()
                        .expect("second_theoretical_mass column value/level count mismatch")
                } else {
                    f64::NAN
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
            peptides,
            xcorr_col,
            ions_total_col,
            ions_matched_col,
            theoretical_mass_col,
            second_theoretical_mass_col,
            loop_score_col,
            loop_feature_buffer: None,
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
        PeptideSpectrumMatchCollection::len(self)
    }

    fn dim(&self) -> usize {
        LOOP_FEATURES.len()
    }

    fn point(
        &self,
        idx: usize,
    ) -> Result<Cow<'_, [f64]>, local_outlier_probabilities::error::Error> {
        let buffer = self
            .loop_feature_buffer
            .as_deref()
            .expect("prepare_loop_features must be called before scoring");

        Ok(Cow::Borrowed(&buffer[idx * 3..idx * 3 + 3]))
    }
}

#[cfg(test)]
mod tests {
    use std::{env::temp_dir, fs::File};

    use xcorrrs::scoring_result::ScoringResult;

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

    /// Test helper mirroring the direct-column-write pattern `run_xcorr_search` uses via
    /// [`PeptideSpectrumMatchCollection::peptides_and_columns_mut`].
    fn set_scoring_result_at_index(
        psms: &mut PeptideSpectrumMatchCollection,
        idx: usize,
        scoring_result: ScoringResult,
    ) {
        let (_, xcorr_col, ions_total_col, ions_matched_col, theoretical_mass_col, second_theoretical_mass_col) =
            psms.peptides_and_columns_mut();
        xcorr_col[idx] = scoring_result.score;
        ions_total_col[idx] = scoring_result.ions_total as u64;
        ions_matched_col[idx] = scoring_result.ions_matched as u64;
        theoretical_mass_col[idx] = scoring_result.min_theoretical_mass;
        second_theoretical_mass_col[idx] =
            if scoring_result.min_theoretical_mass == scoring_result.max_theoretical_mass {
                f64::NAN
            } else {
                scoring_result.max_theoretical_mass
            };
    }

    #[test]
    fn test_retain_scored() {
        let mut peptide_arena = PeptideArena::new();
        peptide_arena.push("PASSES_1");
        peptide_arena.push("BELOW_THRESHOLD"); // never written -> stays NaN placeholder
        peptide_arena.push("PASSES_2");
        peptide_arena.push("ERRORED"); // never written -> stays NaN placeholder

        let mut psms = PeptideSpectrumMatchCollection::with_capacity(
            "scan_1".to_string(),
            Precursor::new(500.25, 2),
            peptide_arena,
        );
        // Mirrors `run_xcorr_search`: only indices that passed the score threshold get
        // written; below-threshold and erroring candidates (indices 1 and 3) are left
        // untouched at their `with_capacity` NaN/0 sentinel.
        set_scoring_result_at_index(&mut psms, 0, scoring_result(1.23, 10, 7, 999.5, 999.9));
        set_scoring_result_at_index(&mut psms, 2, scoring_result(2.5, 8, 4, 800.0, 800.0));

        psms.retain_scored();

        assert_eq!(psms.len(), 2);
        assert_eq!(
            psms.peptides().collect::<Vec<_>>(),
            vec!["PASSES_1", "PASSES_2"]
        );
        assert_eq!(psms.xcorr_col(), &[1.23, 2.5]);
        assert_eq!(psms.ions_total_col(), &[10, 8]);
        assert_eq!(psms.ions_matched_col(), &[7, 4]);
        assert_eq!(psms.theoretical_mass_col(), &[999.5, 800.0]);
        // Row 0 was ambiguous-mass, row 1 (originally index 2) was unambiguous —
        // confirms `second_theoretical_mass_col` stayed aligned after filtering.
        assert_eq!(psms.second_theoretical_mass(0), Some(999.9));
        assert_eq!(psms.second_theoretical_mass(1), None);
    }

    #[test]
    fn test_retain_scored_all_below_threshold() {
        let mut peptide_arena = PeptideArena::new();
        peptide_arena.push("BELOW_THRESHOLD_1");
        peptide_arena.push("BELOW_THRESHOLD_2");

        let mut psms = PeptideSpectrumMatchCollection::with_capacity(
            "scan_2".to_string(),
            Precursor::new(500.25, 2),
            peptide_arena,
        );
        // Nothing written -> every row stays a NaN placeholder.

        psms.retain_scored();

        assert_eq!(psms.len(), 0);
        assert!(psms.is_empty());
    }

    fn psms_with_xcorr(xcorr_values: &[f64]) -> PeptideSpectrumMatchCollection {
        let mut peptide_arena = PeptideArena::new();
        for (idx, _) in xcorr_values.iter().enumerate() {
            peptide_arena.push(format!("PEPTIDE{idx}"));
        }

        let mut psms = PeptideSpectrumMatchCollection::with_capacity(
            "scan_1".to_string(),
            Precursor::new(500.25, 2),
            peptide_arena,
        );
        for (idx, &xcorr) in xcorr_values.iter().enumerate() {
            set_scoring_result_at_index(&mut psms, idx, scoring_result(xcorr, 10, 5, 999.5, 999.5));
        }
        psms
    }

    #[test]
    fn test_prepare_loop_features_matches_point() {
        let mut psms = psms_with_xcorr(&[1.0, 2.0, 3.0]);
        psms.prepare_loop_features();

        for idx in 0..psms.len() {
            let point = PointSource::point(&psms, idx).unwrap();
            assert_eq!(point[0], psms.xcorr_col()[idx]);
            assert_eq!(
                point[1],
                psms.ions_matched_col()[idx] as f64 / psms.ions_total_col()[idx] as f64
            );
            assert_eq!(
                point[2],
                (psms.theoretical_mass_col()[idx] - psms.experimental_mass()).abs()
            );
        }
    }

    #[test]
    #[should_panic(expected = "prepare_loop_features must be called before scoring")]
    fn test_point_without_prepare_loop_features_panics() {
        let psms = psms_with_xcorr(&[1.0]);
        let _ = PointSource::point(&psms, 0);
    }

    #[test]
    fn test_loop_feature_buffer_excluded_from_serde() {
        // Unlike `psms_with_xcorr`, uses distinct min/max theoretical mass so
        // `second_theoretical_mass_col` is `Some(_)` rather than the `f64::NAN` sentinel:
        // serde_json serializes `NaN` as `null`, which the derived `Deserialize` can't read
        // back into a plain `f64` — an unrelated, pre-existing quirk of NaN-sentinel columns
        // under JSON that this test isn't about, so it's avoided here.
        let mut peptide_arena = PeptideArena::new();
        peptide_arena.push("PEPTIDE0");
        peptide_arena.push("PEPTIDE1");
        let mut psms = PeptideSpectrumMatchCollection::with_capacity(
            "scan_1".to_string(),
            Precursor::new(500.25, 2),
            peptide_arena,
        );
        set_scoring_result_at_index(&mut psms, 0, scoring_result(1.0, 10, 5, 999.5, 999.9));
        set_scoring_result_at_index(&mut psms, 1, scoring_result(2.0, 10, 5, 999.5, 999.9));
        psms.prepare_loop_features();
        assert!(psms.loop_feature_buffer.is_some());

        let serialized = serde_json::to_string(&psms).unwrap();
        assert!(!serialized.contains("loop_feature_buffer"));

        let deserialized: PeptideSpectrumMatchCollection =
            serde_json::from_str(&serialized).unwrap();
        assert!(deserialized.loop_feature_buffer.is_none());
    }

    #[test]
    fn test_partial_eq_ignores_loop_feature_buffer() {
        let mut with_buffer = psms_with_xcorr(&[1.0, 2.0]);
        with_buffer.prepare_loop_features();
        let without_buffer = psms_with_xcorr(&[1.0, 2.0]);

        assert!(with_buffer.loop_feature_buffer.is_some());
        assert!(without_buffer.loop_feature_buffer.is_none());
        assert_eq!(with_buffer, without_buffer);
    }

    #[test]
    fn test_truncate_to_top_by_xcorr_noop_under_cap() {
        let mut psms = psms_with_xcorr(&[3.0, 1.0, 2.0]);
        psms.truncate_to_top_by_xcorr(10);
        assert_eq!(psms.len(), 3);
        assert_eq!(psms.xcorr_col(), &[3.0, 1.0, 2.0]);
    }

    #[test]
    fn test_truncate_to_top_by_xcorr_keeps_highest() {
        let mut psms = psms_with_xcorr(&[3.0, 1.0, 4.0, 1.5, 5.0]);
        psms.truncate_to_top_by_xcorr(2);

        assert_eq!(psms.len(), 2);
        assert_eq!(psms.xcorr_col(), &[4.0, 5.0]);
        assert_eq!(
            psms.peptides().collect::<Vec<_>>(),
            vec!["PEPTIDE2", "PEPTIDE4"]
        );
        // Every column stayed aligned with the kept rows, not just xcorr.
        assert_eq!(psms.ions_total_col(), &[10, 10]);
        assert_eq!(psms.ions_matched_col(), &[5, 5]);
    }

    #[test]
    fn test_truncate_to_top_by_xcorr_ties_at_threshold_are_all_kept() {
        let mut psms = psms_with_xcorr(&[1.0, 2.0, 2.0, 2.0]);
        psms.truncate_to_top_by_xcorr(2);

        // All three rows tied at the cut-off value (2.0) are kept, so the result can
        // exceed max_len; the below-threshold row (1.0) must not be.
        assert_eq!(psms.len(), 3);
        assert!(psms.xcorr_col().iter().all(|&x| x == 2.0));
    }

    #[test]
    fn test_truncate_to_top_by_xcorr_zero_keeps_nothing() {
        let mut psms = psms_with_xcorr(&[1.0, 2.0, 3.0]);
        psms.truncate_to_top_by_xcorr(0);

        assert_eq!(psms.len(), 0);
        assert!(psms.is_empty());
    }

    #[tokio::test]
    async fn test_psm_collection_parquet_roundtrip() {
        let mut peptide_arena = PeptideArena::new();
        peptide_arena.push("PEPTIDE");
        peptide_arena.push("PEPTIDEK");

        let mut psms = PeptideSpectrumMatchCollection::with_capacity(
            "scan_1".to_string(),
            Precursor::new(500.25, 2),
            peptide_arena,
        );
        // Ambiguous mass -> `second_theoretical_mass_col` entry is `Some(_)`
        set_scoring_result_at_index(&mut psms, 0, scoring_result(1.23, 10, 7, 999.5, 999.9));
        // Unambiguous mass -> `second_theoretical_mass_col` entry is `None`
        set_scoring_result_at_index(&mut psms, 1, scoring_result(0.98, 12, 9, 1000.1, 1000.1));
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
        let mut peptide_arena = PeptideArena::new();
        peptide_arena.push("SEQVENCE");
        peptide_arena.push("PEPTIDEK");
        let mut psms = PeptideSpectrumMatchCollection::with_capacity(
            "scan_2".to_string(),
            Precursor::new(600.5, 3),
            peptide_arena,
        );
        set_scoring_result_at_index(&mut psms, 0, scoring_result(2.5, 8, 4, 800.0, 800.0));

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
