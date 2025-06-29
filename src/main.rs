use clap::Parser;
use env_logger;
use log::error;
use std::process;

mod config;
mod error;
mod parser;
mod processor;
mod types;
mod util;
mod writer;

fn main() {
    // Parse command-line arguments
    let config = config::Config::parse();

    // Initialize logging based on verbose flag
    if config.verbose {
        env_logger::Builder::from_default_env()
            .filter_level(log::LevelFilter::Debug)
            .init();
    } else {
        env_logger::Builder::from_default_env()
            .filter_level(log::LevelFilter::Info)
            .init();
    }

    // Initialize the processing pipeline
    if let Err(e) = run(&config) {
        error!("Application error: {}", e);
        process::exit(1);
    }
}

fn run(config: &config::Config) -> Result<(), Box<dyn std::error::Error>> {
    use std::fs::File;
    use std::io::{BufReader, copy};
    use std::time::Instant;

    let start_time = Instant::now();

    // Initialize thread pool
    let num_threads = config.threads.unwrap_or_else(|| {
        // Use all available CPU cores by default
        std::thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(1)
    });

    if let Some(thread_count) = config.threads {
        log::info!("Using {} threads (user specified)", thread_count);
        rayon::ThreadPoolBuilder::new()
            .num_threads(thread_count)
            .build_global()?;
    } else {
        log::info!("Using {} threads (all available CPU cores)", num_threads);
        // Rayon uses all available cores by default, so no need to explicitly set
    }

    log::info!("Starting Apple Health data transformation...");
    log::info!("Input file: {}", config.input_file);
    log::info!("Output file: {}", config.output_zip);

    // Step 1: Open and prepare input stream
    let input_reader: Box<dyn std::io::BufRead> = if config.input_file.ends_with(".zip") {
        // Handle ZIP file
        log::info!("Opening ZIP archive...");
        let file = File::open(&config.input_file)?;
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
                std::io::copy(&mut export_file, &mut contents)?;
                Box::new(BufReader::new(std::io::Cursor::new(contents)))
            }
            None => return Err("export.xml not found in ZIP archive".into()),
        }
    } else {
        // Handle direct XML file
        log::info!("Opening XML file...");
        let file = File::open(&config.input_file)?;
        Box::new(BufReader::new(file))
    };

    // Step 2: Parse XML and extract records
    log::info!("Parsing XML data...");
    let parse_start = Instant::now();
    let records = parser::parse_health_export(input_reader)?;
    let parse_duration = parse_start.elapsed();
    log::info!("Parsed {} total records in {:.2}s", records.len(), parse_duration.as_secs_f64());

    // Step 3: Process records in parallel and group by type
    log::info!("Processing records in parallel...");
    let process_start = Instant::now();
    let processor = processor::Processor::new();
    processor.process_records(records);
    let grouped_records = processor.get_records();
    let process_duration = process_start.elapsed();
    log::info!("Processed records in {:.3}s", process_duration.as_secs_f64());

    // Step 4: Create temporary directory for CSV files
    let temp_dir =
        std::env::temp_dir().join(format!("apple-health-{}", util::generate_random_id()));
    std::fs::create_dir_all(&temp_dir)?;
    log::info!("Created temporary directory: {}", temp_dir.display());

    // Step 5: Write CSV files for each record type
    log::info!("Writing CSV files...");
    let write_start = Instant::now();
    let mut total_records_written = 0;
    let mut csv_files_created = 0;

    for entry in grouped_records.iter() {
        let record_type = entry.key();
        let records_guard = entry.value().lock().unwrap();
        let records_count = records_guard.len();

        if records_count > 0 {
            let csv_filename = format!("{}.csv", util::sanitize_filename(record_type));
            let csv_path = temp_dir.join(&csv_filename);

            writer::write_records_to_csv(&*records_guard, &csv_path)?;

            total_records_written += records_count;
            csv_files_created += 1;

            if config.verbose {
                log::debug!("Wrote {} records to {}", records_count, csv_filename);
            }
        }
    }

    let write_duration = write_start.elapsed();
    log::info!("Wrote {} CSV files in {:.2}s", csv_files_created, write_duration.as_secs_f64());

    // Step 6: Create output ZIP archive
    log::info!("Creating output ZIP archive...");
    let zip_start = Instant::now();
    writer::create_zip(&std::path::Path::new(&config.output_zip), &temp_dir)?;
    let zip_duration = zip_start.elapsed();
    log::info!("Created ZIP archive in {:.2}s", zip_duration.as_secs_f64());

    // Step 7: Clean up temporary directory
    std::fs::remove_dir_all(&temp_dir)?;
    log::info!("Cleaned up temporary directory");

    // Step 8: Report completion metrics
    let elapsed = start_time.elapsed();
    if !config.no_metrics {
        println!("✅ Transformation completed successfully!");
        println!(
            "📊 Processed {} records in {:.2} seconds",
            total_records_written,
            elapsed.as_secs_f64()
        );
        println!(
            "📁 Created {} CSV files in {}",
            csv_files_created, config.output_zip
        );

        if total_records_written > 0 {
            let records_per_second = total_records_written as f64 / elapsed.as_secs_f64();
            println!("⚡ Throughput: {:.0} records/second", records_per_second);
        }
    }

    Ok(())
}
