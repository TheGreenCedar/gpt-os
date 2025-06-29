use clap::Parser;
use log::error;
use std::process;

mod config;
mod error;
mod parser;
mod processor;
mod types;
mod util;
mod writer;

use crate::error::Result;

fn main() {
    // Parse command-line arguments
    let config = config::Config::parse();

    // Initialize logging
    initialize_logging(config.verbose);

    // Run the application
    if let Err(e) = run(&config) {
        error!("Application error: {}", e);
        process::exit(1);
    }
}

/// Initialize logging based on verbosity level
fn initialize_logging(verbose: bool) {
    let level = if verbose {
        log::LevelFilter::Debug
    } else {
        log::LevelFilter::Info
    };

    env_logger::Builder::from_default_env()
        .filter_level(level)
        .init();
}

/// Main application logic using streaming mode
fn run(config: &config::Config) -> Result<()> {
    use std::time::Instant;

    let start_time = Instant::now();

    // Get number of worker threads
    let num_workers = config.threads.unwrap_or_else(|| {
        std::thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(1)
    });

    log::info!("Starting Apple Health data transformation...");
    log::info!("Input file: {}", config.input_file);
    log::info!("Output file: {}", config.output_zip);
    log::info!("Using {} worker threads", num_workers);

    // Step 1: Start streaming parser
    let (receiver, parser_handle) = start_streaming_parser(&config.input_file)?;

    // Step 2: Process records with worker threads
    let (grouped_records, total_records) = process_records_streaming(receiver, num_workers)?;

    // Step 3: Wait for parser to complete
    match parser_handle.join() {
        Ok(result) => result?,
        Err(_) => {
            return Err(error::AppError::Unknown(
                "Parser thread panicked".to_string(),
            ));
        }
    }

    // Step 4: Write CSV files and create ZIP
    write_output_files(&grouped_records, &config.output_zip, config.verbose)?;

    // Step 5: Report completion
    report_completion(config, total_records, start_time.elapsed());

    Ok(())
}

/// Start the streaming parser and return channel receiver and thread handle
fn start_streaming_parser(
    input_file: &str,
) -> Result<(
    std::sync::mpsc::Receiver<types::RecordRow>,
    std::thread::JoinHandle<Result<()>>,
)> {
    use std::fs::File;
    use std::io::{BufReader, Read};

    let input_reader: Box<dyn std::io::BufRead + Send> = if input_file.ends_with(".zip") {
        log::info!("Opening ZIP archive...");
        let file = File::open(input_file)?;
        let mut archive = zip::ZipArchive::new(file)?;

        // Find export.xml in the archive
        let mut export_xml_index = None;
        for i in 0..archive.len() {
            let file = archive.by_index(i)?;
            if file.name() == "export.xml" || file.name().ends_with("/export.xml") {
                export_xml_index = Some(i);
                break;
            }
        }

        match export_xml_index {
            Some(index) => {
                let mut export_file = archive.by_index(index)?;
                let mut contents = Vec::new();
                export_file.read_to_end(&mut contents)?;
                Box::new(BufReader::new(std::io::Cursor::new(contents)))
            }
            None => {
                return Err(error::AppError::Unknown(
                    "export.xml not found in ZIP archive".to_string(),
                ));
            }
        }
    } else {
        log::info!("Opening XML file...");
        let file = File::open(input_file)?;
        Box::new(BufReader::new(file))
    };

    log::info!("Starting streaming XML parser...");
    let (receiver, handle) = parser::parse_health_export_streaming(input_reader);
    Ok((receiver, handle))
}

/// Process records from channel using worker threads
fn process_records_streaming(
    receiver: std::sync::mpsc::Receiver<types::RecordRow>,
    num_workers: usize,
) -> Result<(
    std::sync::Arc<dashmap::DashMap<String, std::sync::Mutex<Vec<types::RecordRow>>>>,
    usize,
)> {
    use std::time::Instant;

    log::info!(
        "Starting {} worker threads for record processing...",
        num_workers
    );
    let process_start = Instant::now();

    let processor = processor::Processor::new();
    let worker_handles = processor.process_records_from_channel(receiver, num_workers);

    // Wait for all workers to complete
    for (i, handle) in worker_handles.into_iter().enumerate() {
        match handle.join() {
            Ok(()) => log::debug!("Worker {} completed successfully", i),
            Err(_) => return Err(error::AppError::Unknown(format!("Worker {} panicked", i))),
        }
    }

    let total_records = processor.get_total_record_count();
    let grouped_records = processor.get_records();
    let process_duration = process_start.elapsed();

    log::info!(
        "Processed {} records in {:.3}s using {} workers",
        total_records,
        process_duration.as_secs_f64(),
        num_workers
    );

    Ok((grouped_records, total_records))
}

/// Write CSV files and create ZIP archive
fn write_output_files(
    grouped_records: &dashmap::DashMap<String, std::sync::Mutex<Vec<types::RecordRow>>>,
    output_zip: &str,
    verbose: bool,
) -> Result<()> {
    use std::path::Path;
    use std::time::Instant;

    // Create temporary directory
    let temp_dir =
        std::env::temp_dir().join(format!("apple-health-{}", util::generate_random_id()));
    std::fs::create_dir_all(&temp_dir)?;
    log::info!("Created temporary directory: {}", temp_dir.display());

    log::info!("Writing CSV files...");
    let write_start = Instant::now();
    let mut csv_files_created = 0;

    // Write CSV files for each record type
    for entry in grouped_records.iter() {
        let record_type = entry.key();
        let records_guard = entry.value().lock().unwrap();
        let records_count = records_guard.len();

        if records_count > 0 {
            let csv_filename = format!("{}.csv", util::sanitize_filename(record_type));
            let csv_path = temp_dir.join(&csv_filename);

            writer::write_records_to_csv(&*records_guard, &csv_path)?;
            csv_files_created += 1;

            if verbose {
                log::debug!("Wrote {} records to {}", records_count, csv_filename);
            }
        }
    }

    let write_duration = write_start.elapsed();
    log::info!(
        "Wrote {} CSV files in {:.2}s",
        csv_files_created,
        write_duration.as_secs_f64()
    );

    // Create output ZIP archive
    log::info!("Creating output ZIP archive...");
    let zip_start = Instant::now();
    writer::create_zip(Path::new(output_zip), &temp_dir)?;
    let zip_duration = zip_start.elapsed();
    log::info!("Created ZIP archive in {:.2}s", zip_duration.as_secs_f64());

    // Clean up temporary directory
    std::fs::remove_dir_all(&temp_dir)?;
    log::info!("Cleaned up temporary directory");

    Ok(())
}

/// Report completion metrics to the user
fn report_completion(config: &config::Config, total_records: usize, elapsed: std::time::Duration) {
    if !config.no_metrics {
        println!("✅ Transformation completed successfully!");
        println!(
            "📊 Processed {} records in {:.2} seconds",
            total_records,
            elapsed.as_secs_f64()
        );
        println!("📁 Created CSV files in {}", config.output_zip);

        if total_records > 0 {
            let records_per_second = total_records as f64 / elapsed.as_secs_f64();
            println!("⚡ Throughput: {:.0} records/second", records_per_second);
        }
    }
}
