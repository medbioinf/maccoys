use std::path::PathBuf;

use anyhow::Result;
use axum::{body::Bytes, BoxError};
use futures::{pin_mut, Stream, TryStreamExt};
use tokio::fs::File;
use tokio::io::{copy, BufWriter, Error as IoError};
use tokio_util::io::StreamReader;

/// Writes a streamed file to disk.
///
/// # Arguments
/// * `path` - The path to write the file to.
/// * `stream` - The stream of bytes to write to the file.
///
pub async fn write_streamed_file<S, E>(path: &PathBuf, stream: S) -> Result<()>
where
    S: Stream<Item = Result<Bytes, E>>,
    E: Into<BoxError>,
{
    async {
        // Convert the stream into an `AsyncRead`.
        let body_with_io_error = stream.map_err(IoError::other);
        let body_reader = StreamReader::new(body_with_io_error);
        pin_mut!(body_reader);
        let mut file = BufWriter::new(File::create(path).await?);

        // Copy the body into the file.
        copy(&mut body_reader, &mut file).await?;

        Ok(())
    }
    .await
}
