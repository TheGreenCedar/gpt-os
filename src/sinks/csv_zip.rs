use crate::core::{Processable, Sink};
use crate::error::Result;
use dashmap::DashMap;
use log::{debug, info, warn};
use std::fs::{self, File};
use std::io;
use std::path::Path;
use std::time::Instant;
use zip::{ZipWriter, write::SimpleFileOptions};

pub struct CsvZipSink;

impl<T: Processable> Sink<T> for CsvZipSink {
    fn load(&self, grouped_records: DashMap<String, Vec<T>>, output_path: &Path) -> Result<()> {
        let start_time = Instant::now();
        let total_record_types = grouped_records.len();
        let total_records: usize = grouped_records
            .iter()
            .map(|entry| entry.value().len())
            .sum();

        info!(
            "Starting CSV export for {} record types, {} total records",
            total_record_types, total_records
        );

        let temp_dir = tempfile::Builder::new().prefix("csv-").tempdir()?;
        debug!("Created temporary directory: {}", temp_dir.path().display());

        let csv_start = Instant::now();
        let mut csv_files_created = 0;
        let mut total_records_written = 0;

        for entry in grouped_records.iter() {
            let (record_type, records) = entry.pair();
            if records.is_empty() {
                warn!("Record type '{}' has no records, skipping", record_type);
                continue;
            }

            let record_count = records.len();
            debug!(
                "Processing {} records for type '{}'",
                record_count, record_type
            );

            let file_path = temp_dir.path().join(format!("{}.csv", record_type));
            let mut wtr = csv::Writer::from_path(&file_path)?;

            for record in records.iter() {
                wtr.serialize(record.as_serializable())?;
            }
            wtr.flush()?;

            csv_files_created += 1;
            total_records_written += record_count;
            debug!("Wrote {} records to {}.csv", record_count, record_type);
        }

        let csv_duration = csv_start.elapsed();
        info!(
            "Created {} CSV files with {} records in {:.2}s",
            csv_files_created,
            total_records_written,
            csv_duration.as_secs_f64()
        );

        let zip_start = Instant::now();
        info!("Creating ZIP archive: {}", output_path.display());
        create_zip(output_path, temp_dir.path())?;
        let zip_duration = zip_start.elapsed();
        info!("ZIP archive created in {:.2}s", zip_duration.as_secs_f64());

        temp_dir.close()?;
        debug!("Cleaned up temporary directory");

        let total_duration = start_time.elapsed();
        info!(
            "CSV export completed in {:.2}s (CSV: {:.2}s, ZIP: {:.2}s)",
            total_duration.as_secs_f64(),
            csv_duration.as_secs_f64(),
            zip_duration.as_secs_f64()
        );

        Ok(())
    }
}

fn create_zip(output_zip: &Path, source_dir: &Path) -> Result<()> {
    let zip_file = File::create(output_zip)?;
    let mut zip = ZipWriter::new(zip_file);
    let mut files_added = 0;
    let mut total_bytes = 0u64;

    for entry in fs::read_dir(source_dir)? {
        let entry = entry?;
        let path = entry.path();

        if path.is_file() {
            let file_size = entry.metadata()?.len();
            let file_name = path.file_name().unwrap();

            debug!(
                "Adding {} ({} bytes) to ZIP",
                file_name.to_string_lossy(),
                file_size
            );

            zip.start_file(
                file_name.to_string_lossy().as_ref(),
                SimpleFileOptions::default(),
            )?;
            let mut f = File::open(&path)?;
            let bytes_copied = io::copy(&mut f, &mut zip)?;

            files_added += 1;
            total_bytes += bytes_copied;
        }
    }

    zip.finish()?;
    debug!(
        "ZIP archive completed: {} files, {} bytes",
        files_added, total_bytes
    );
    Ok(())
}
