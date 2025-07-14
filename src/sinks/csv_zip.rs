use crate::core::{Processable, Sink};
use crate::error::{AppError, Result};
use crossbeam_channel::{Receiver, bounded};
use log::{debug, info, warn};
use rayon::prelude::*;
use std::collections::HashMap;
use std::fs::File;
use std::io::{Cursor, Write};
use std::path::Path;
use std::thread;
use std::time::Instant;
use tokio::task;
use zip::ZipArchive;
use zip::{CompressionMethod, ZipWriter, write::FileOptions};

/// Trait for writing records to a CSV writer using dynamic headers.
pub trait CsvWritable {
    /// Return the attribute keys used for CSV headers.
    fn headers(&self) -> Vec<String>;

    /// Write the record using the provided header ordering.
    fn write<W: Write>(&self, writer: &mut csv::Writer<W>, headers: &[String]) -> csv::Result<()>;
}

pub struct CsvZipSink;

#[async_trait::async_trait]
impl<T> Sink<T> for CsvZipSink
where
    T: Processable + CsvWritable + Send + Sync + 'static,
{
    async fn load(
        &self,
        grouped_records: HashMap<String, Vec<T>>,
        output_path: &Path,
    ) -> Result<()> {
        let out = output_path.to_owned();
        task::spawn_blocking(move || Self::load_sync(grouped_records, &out))
            .await
            .unwrap()
    }
}

impl CsvZipSink {
    fn load_sync<T>(grouped_records: HashMap<String, Vec<T>>, output_path: &Path) -> Result<()>
    where
        T: Processable + CsvWritable + Send + Sync + 'static,
    {
        let start = Instant::now();

        let entries = filter_entries(grouped_records);
        let total_files = entries.len();
        let total_recs: usize = entries.iter().map(|(_, v)| v.len()).sum();
        info!(
            "Exporting {} CSVs, {} total records",
            total_files, total_recs
        );

        // 2. Parallel CSV serialization into byte buffers and streaming merge into the final ZIP
        let (tx, rx) = bounded::<(String, Cursor<Vec<u8>>)>(1);

        let merge_handle = spawn_merger(output_path, rx, start);

        // 3. Produce mini-zips in parallel and stream into the merge channel
        entries
            .into_par_iter()
            .try_for_each(|(name, mut recs)| -> Result<()> {
                let cursor = create_mini_zip(&name, &mut recs)?;
                tx.send((name, cursor))
                    .map_err(|e| AppError::Unknown(e.to_string()))?;
                Ok(())
            })?;

        // drop sender and wait for merging to complete
        drop(tx);
        merge_handle.join().expect("merge thread panicked")
    }
}

fn filter_entries<T>(grouped_records: HashMap<String, Vec<T>>) -> Vec<(String, Vec<T>)>
where
    T: Processable + CsvWritable,
{
    let mut entries: Vec<(String, Vec<T>)> = grouped_records
        .into_iter()
        .filter_map(|(k, v)| {
            if v.is_empty() {
                warn!("Skipping empty group '{}'", k);
                None
            } else {
                Some((k, v))
            }
        })
        .collect();
    entries.sort_by(|a, b| a.0.cmp(&b.0));
    entries
}

fn spawn_merger(
    output_path: &Path,
    rx: Receiver<(String, Cursor<Vec<u8>>)>,
    start: Instant,
) -> thread::JoinHandle<Result<()>> {
    let output_path = output_path.to_owned();
    thread::spawn(move || -> Result<()> {
        let mut out = File::create(&output_path)?;
        let mut zip = ZipWriter::new(&mut out);
        for (name, mut mini) in rx {
            let src = ZipArchive::new(&mut mini)?;
            zip.merge_archive(src)?;
            debug!("Merged '{}.csv' from mini-zip", name);
        }
        zip.finish()?;
        log::info!("Done in {:.2}s", start.elapsed().as_secs_f64());
        Ok(())
    })
}

fn create_mini_zip<T>(name: &str, recs: &mut [T]) -> Result<Cursor<Vec<u8>>>
where
    T: Processable + CsvWritable,
{
    use std::collections::BTreeSet;

    recs.sort_by_key(|r| r.sort_key().unwrap_or_default());

    // build CSV in memory
    let mut buf = Vec::with_capacity(recs.len() * 100);
    {
        let mut header_set = BTreeSet::new();
        for r in &*recs {
            header_set.extend(r.headers());
        }
        let headers: Vec<String> = header_set.into_iter().collect();

        let mut w = csv::WriterBuilder::new()
            .has_headers(true)
            .from_writer(&mut buf);
        w.write_record(&headers)?;
        for r in &*recs {
            r.write(&mut w, &headers)?;
        }
        w.flush()?;
    }
    debug!("CSV for '{}' is {} bytes", name, buf.len());

    // wrap in a 1-entry ZIP
    let mut cursor = Cursor::new(Vec::with_capacity(buf.len() / 3));
    {
        let mut mini = ZipWriter::new(&mut cursor);
        let opts = FileOptions::<()>::default()
            .compression_method(CompressionMethod::Deflated)
            .compression_level(Some(2))
            .unix_permissions(0o644);
        mini.start_file(format!("{}.csv", name), opts)?;
        mini.write_all(&buf)?;
        mini.finish()?;
    }
    cursor.set_position(0);
    Ok(cursor)
}
