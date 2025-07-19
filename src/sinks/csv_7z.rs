use crate::core::{Processable, Sink};
use crate::error::{AppError, Result};
use crate::sinks::csv_zip::CsvWritable;
use crossbeam_channel::{Receiver, bounded};
use log::{debug, info, warn};
use rayon::prelude::*;
use sevenz_rust::{SevenZArchiveEntry, SevenZWriter};
use std::collections::HashMap;
use std::fs::File;
use std::io::Cursor;
use std::path::Path;
use std::thread;
use std::time::Instant;
use tokio::task;

pub struct Csv7zSink;

#[async_trait::async_trait]
impl<T> Sink<T> for Csv7zSink
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

impl Csv7zSink {
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

        let (tx, rx) = bounded::<(String, Cursor<Vec<u8>>)>(1);
        let merge_handle = spawn_writer(output_path, rx, start);

        entries
            .into_par_iter()
            .try_for_each(|(name, mut recs)| -> Result<()> {
                let buf = create_csv_buffer(&name, &mut recs)?;
                tx.send((name, buf))
                    .map_err(|e| AppError::Unknown(e.to_string()))?;
                Ok(())
            })?;

        drop(tx);
        merge_handle.join().expect("writer thread panicked")
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

fn spawn_writer(
    output_path: &Path,
    rx: Receiver<(String, Cursor<Vec<u8>>)>,
    start: Instant,
) -> thread::JoinHandle<Result<()>> {
    let output_path = output_path.to_owned();
    thread::spawn(move || -> Result<()> {
        let out = File::create(&output_path)?;
        let mut writer = SevenZWriter::new(out)?;
        for (name, mut cursor) in rx {
            let mut entry = SevenZArchiveEntry::new();
            entry.name = format!("{}.csv", name);
            writer.push_archive_entry(entry, Some(&mut cursor))?;
            debug!("Added '{}.csv' to 7z archive", name);
        }
        writer.finish()?;
        info!("Done in {:.2}s", start.elapsed().as_secs_f64());
        Ok(())
    })
}

fn create_csv_buffer<T>(name: &str, recs: &mut [T]) -> Result<Cursor<Vec<u8>>>
where
    T: Processable + CsvWritable,
{
    use std::collections::BTreeSet;

    recs.sort_by_key(|r| r.sort_key().unwrap_or_default());

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

    let mut cursor = Cursor::new(buf);
    cursor.set_position(0);
    Ok(cursor)
}
