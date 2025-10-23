use crate::core::{Processable, Sink};
use crate::error::{AppError, Result};
use ahash::{AHashMap, AHashSet};
use crossbeam_channel::{Receiver, bounded};
use log::{debug, info, warn};
use rayon::prelude::*;
use std::fs::File;
use std::io::{Cursor, Write};
use std::mem::MaybeUninit;
use std::path::Path;
use std::thread;
use std::time::Instant;
use tokio::task;
use zip::ZipArchive;
use zip::{CompressionMethod, ZipWriter, write::FileOptions};

const STORE_THRESHOLD: usize = 8 * 1024;

/// Trait for writing records to a CSV writer using dynamic headers.
pub trait CsvWritable {
    /// Return the attribute keys used for CSV headers.
    fn header_keys(&self) -> impl Iterator<Item = &str>;

    /// Write the record using the provided header ordering.
    fn write<W: Write>(&self, writer: &mut csv::Writer<W>, headers: &[&str]) -> csv::Result<()>;
}

pub struct CsvZipSink;

#[async_trait::async_trait]
impl<T> Sink<T> for CsvZipSink
where
    T: Processable + CsvWritable + Send + Sync + 'static,
{
    async fn load(
        &self,
        grouped_records: AHashMap<String, Vec<T>>,
        output_path: &Path,
    ) -> Result<()> {
        let out = output_path.to_owned();
        task::spawn_blocking(move || Self::load_sync(grouped_records, &out))
            .await
            .unwrap()
    }
}

impl CsvZipSink {
    fn load_sync<T>(grouped_records: AHashMap<String, Vec<T>>, output_path: &Path) -> Result<()>
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
        //    Benchmarks with `tests/fixtures/sample_export.xml` showed a small win from
        //    buffering four mini-zips at a time (~0.28s vs. 0.33s for capacity 1).
        //    If memory usage allows in the future, we could stream CSV data directly into the
        //    final archive and remove this channel entirely.
        let queue_capacity = (rayon::current_num_threads().saturating_mul(2)).max(4);
        let (tx, rx) = bounded::<(String, Cursor<Vec<u8>>)>(queue_capacity);

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

fn filter_entries<T>(grouped_records: AHashMap<String, Vec<T>>) -> Vec<(String, Vec<T>)>
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
    let mut has_sort_keys = false;
    let sort_keys: Vec<Option<&str>> = recs
        .iter()
        .map(|r| {
            let key = r.sort_key();
            if key.is_some() {
                has_sort_keys = true;
            }
            key
        })
        .collect();
    if has_sort_keys {
        let mut indices: Vec<usize> = (0..recs.len()).collect();
        indices.sort_unstable_by_key(|&idx| sort_keys[idx]);
        drop(sort_keys);
        reorder_by_indices(recs, &indices);
    }

    // Determine dynamic headers once per file
    let mut header_set: AHashSet<&str> = AHashSet::new();
    for r in &*recs {
        header_set.extend(r.header_keys());
    }
    let mut headers: Vec<&str> = header_set.into_iter().collect();
    headers.sort_unstable();

    let mut csv_buf = Vec::with_capacity(recs.len().saturating_mul(headers.len().max(1) * 8));
    {
        let mut w = csv::WriterBuilder::new()
            .has_headers(true)
            .buffer_capacity(128 * 1024)
            .from_writer(&mut csv_buf);
        w.write_record(&headers)?;
        for r in &*recs {
            r.write(&mut w, &headers)?;
        }
        w.flush()?;
    }
    debug!("CSV for '{}' is {} bytes", name, csv_buf.len());

    let mut cursor = Cursor::new(Vec::with_capacity(csv_buf.len() / 3 + 256));
    {
        let mut mini = ZipWriter::new(&mut cursor);
        let (method, level) = if csv_buf.len() < STORE_THRESHOLD {
            (CompressionMethod::Stored, None)
        } else {
            (CompressionMethod::Deflated, Some(1))
        };
        let mut opts = FileOptions::<()>::default()
            .compression_method(method)
            .unix_permissions(0o644);
        if let Some(level) = level {
            opts = opts.compression_level(Some(level));
        }
        mini.start_file(format!("{}.csv", name), opts)?;
        mini.write_all(&csv_buf)?;
        mini.finish()?;
    }
    debug!(
        "Compressed CSV for '{}' is {} bytes",
        name,
        cursor.get_ref().len()
    );
    cursor.set_position(0);
    Ok(cursor)
}

fn reorder_by_indices<T>(items: &mut [T], order: &[usize]) {
    debug_assert_eq!(items.len(), order.len());
    if items.len() <= 1 {
        return;
    }

    let len = items.len();
    let mut tmp: Vec<MaybeUninit<T>> = Vec::with_capacity(len);
    unsafe {
        tmp.set_len(len);
    }

    let base_ptr = items.as_mut_ptr();
    for (slot, &src_index) in tmp.iter_mut().zip(order.iter()) {
        unsafe {
            slot.as_mut_ptr().write(base_ptr.add(src_index).read());
        }
    }

    for (index, slot) in tmp.into_iter().enumerate() {
        unsafe {
            base_ptr.add(index).write(slot.assume_init());
        }
    }
}
