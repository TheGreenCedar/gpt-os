use crate::core::{Processable, Sink};
use crate::error::Result;
use dashmap::DashMap;
use log::{debug, info, warn};
use std::fs::File;
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

        let zip_file = File::create(output_path)?;
        let mut zip_writer = ZipWriter::new(zip_file);

        // Convert DashMap entries to vector and filter empty records
        let entries: Vec<(String, Vec<T>)> = grouped_records
            .into_iter()
            .filter_map(|(record_type, records)| {
                if records.is_empty() {
                    warn!("Record type '{}' has no records, skipping", record_type);
                    None
                } else {
                    Some((record_type, records))
                }
            })
            .collect();

        // Process each record type and write directly to ZIP
        for (record_type, records) in entries {
            let record_count = records.len();
            debug!(
                "Processing {} records for type '{}'",
                record_count, record_type
            );

            // Start a new file in the ZIP archive
            let file_name = format!("{}.csv", record_type);
            zip_writer.start_file(&file_name, SimpleFileOptions::default())?;

            // Create CSV writer that writes directly to the ZIP entry
            let mut csv_writer = csv::Writer::from_writer(&mut zip_writer);

            // Write all records for this type
            for record in &records {
                csv_writer.serialize(record.as_serializable())?;
            }

            // Flush the CSV writer to ensure all data is written
            csv_writer.flush()?;

            debug!("Wrote {} records to {}", record_count, file_name);
        }

        // Finalize the ZIP archive
        zip_writer.finish()?;

        let total_duration = start_time.elapsed();
        info!(
            "CSV export completed in {:.2}s",
            total_duration.as_secs_f64()
        );

        Ok(())
    }
}
