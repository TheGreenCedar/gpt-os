use crate::error::Result;
use crate::types::RecordRow;
use serde::Serialize;
use std::fs::{self, File};
use std::io;
use std::path::Path;
use zip::{ZipWriter, write::SimpleFileOptions};

/// Write serializable records to a CSV file
pub fn write_csv<T: Serialize>(records: &[T], output_path: &Path) -> Result<()> {
    let file = File::create(output_path)?;
    let mut wtr = csv::Writer::from_writer(file);

    for record in records {
        wtr.serialize(record)?;
    }

    wtr.flush()?;
    Ok(())
}

/// Create a ZIP archive from all CSV files in a directory
pub fn create_zip(output_zip: &Path, temp_dir: &Path) -> Result<()> {
    let zip_file = File::create(output_zip)?;
    let mut zip = ZipWriter::new(zip_file);

    for entry in fs::read_dir(temp_dir)? {
        let entry = entry?;
        let path = entry.path();

        if path.is_file() {
            let file_name = path.file_name().unwrap();
            zip.start_file(
                file_name.to_string_lossy().as_ref(),
                SimpleFileOptions::default(),
            )?;
            let mut f = File::open(&path)?;
            io::copy(&mut f, &mut zip)?;
        }
    }

    zip.finish()?;
    Ok(())
}

/// Write records to CSV file with proper serialization
pub fn write_records_to_csv(records: &[RecordRow], output_path: &Path) -> Result<()> {
    if records.is_empty() {
        return Ok(());
    }

    // Group records by their actual type and write them using serde
    match &records[0] {
        RecordRow::Record(_) => {
            let typed_records: Vec<_> = records
                .iter()
                .filter_map(|r| match r {
                    RecordRow::Record(record) => Some(record),
                    _ => None,
                })
                .collect();
            write_csv(&typed_records, output_path)?;
        }
        RecordRow::Workout(_) => {
            let typed_records: Vec<_> = records
                .iter()
                .filter_map(|r| match r {
                    RecordRow::Workout(workout) => Some(workout),
                    _ => None,
                })
                .collect();
            write_csv(&typed_records, output_path)?;
        }
        RecordRow::ActivitySummary(_) => {
            let typed_records: Vec<_> = records
                .iter()
                .filter_map(|r| match r {
                    RecordRow::ActivitySummary(summary) => Some(summary),
                    _ => None,
                })
                .collect();
            write_csv(&typed_records, output_path)?;
        }
    }

    Ok(())
}
