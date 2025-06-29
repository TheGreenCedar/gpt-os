use crate::types::RecordRow;
use serde::Serialize;
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use zip::{ZipWriter, write::SimpleFileOptions};

pub fn write_csv<T: Serialize>(records: &[T], output_path: &Path) -> io::Result<()> {
    let file = File::create(output_path)?;
    let mut wtr = csv::Writer::from_writer(file);

    for record in records {
        wtr.serialize(record)?;
    }

    wtr.flush()?;
    Ok(())
}

pub fn create_zip(output_zip: &Path, temp_dir: &Path) -> io::Result<()> {
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

pub fn write_records_to_csv(records: &[RecordRow], output_path: &Path) -> io::Result<()> {
    let file = File::create(output_path)?;
    let mut wtr = csv::Writer::from_writer(file);

    // Write header based on record type
    if let Some(first_record) = records.first() {
        match first_record {
            RecordRow::Record(_) => {
                wtr.write_record(&[
                    "type",
                    "value",
                    "unit",
                    "creationDate",
                    "startDate",
                    "endDate",
                    "sourceName",
                    "sourceVersion",
                    "device",
                ])?;
            }
            RecordRow::Workout(_) => {
                wtr.write_record(&[
                    "workoutActivityType",
                    "duration",
                    "totalDistance",
                    "totalEnergyBurned",
                    "sourceName",
                    "device",
                    "startDate",
                    "endDate",
                ])?;
            }
            RecordRow::ActivitySummary(_) => {
                wtr.write_record(&[
                    "dateComponents",
                    "activeEnergyBurned",
                    "activeEnergyBurnedGoal",
                    "appleExerciseTime",
                    "appleStandHours",
                ])?;
            }
        }
    }

    // Write data rows
    for record in records {
        match record {
            RecordRow::Record(r) => {
                wtr.write_record(&[
                    &r.record_type,
                    &r.value,
                    r.unit.as_deref().unwrap_or(""),
                    &r.creation_date,
                    &r.start_date,
                    &r.end_date,
                    &r.source_name,
                    r.source_version.as_deref().unwrap_or(""),
                    r.device.as_deref().unwrap_or(""),
                ])?;
            }
            RecordRow::Workout(w) => {
                wtr.write_record(&[
                    &w.activity_type,
                    &w.duration.to_string(),
                    &w.total_distance.map(|d| d.to_string()).unwrap_or_default(),
                    &w.total_energy_burned
                        .map(|e| e.to_string())
                        .unwrap_or_default(),
                    &w.source_name,
                    w.device.as_deref().unwrap_or(""),
                    &w.start_date,
                    &w.end_date,
                ])?;
            }
            RecordRow::ActivitySummary(s) => {
                wtr.write_record(&[
                    &s.date_components,
                    &s.active_energy_burned
                        .map(|e| e.to_string())
                        .unwrap_or_default(),
                    &s.active_energy_burned_goal
                        .map(|e| e.to_string())
                        .unwrap_or_default(),
                    &s.apple_exercise_time
                        .map(|e| e.to_string())
                        .unwrap_or_default(),
                    &s.apple_stand_hours
                        .map(|e| e.to_string())
                        .unwrap_or_default(),
                ])?;
            }
        }
    }

    wtr.flush()?;
    Ok(())
}
