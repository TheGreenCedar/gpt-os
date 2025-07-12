mod apple_health;
mod config;
mod core;
mod error;
mod sinks;
mod xml_utils;

use clap::Parser;
use log::{LevelFilter, error, info};
use std::path::Path;
use std::process;

fn main() {
    let start_time = std::time::Instant::now();
    let config = config::Config::parse();

    // Initialize logging
    env_logger::Builder::from_default_env()
        .filter_level(if config.verbose {
            LevelFilter::Debug
        } else {
            LevelFilter::Info
        })
        .init();

    info!("🚀 Starting Apple Health Transformer");
    info!("📁 Input: {}", config.input_file);
    info!("📦 Output: {}", config.output_zip);

    let extractor = apple_health::extractor::AppleHealthExtractor;
    let sink = sinks::csv_zip::CsvZipSink;

    let engine = core::Engine::new(extractor, sink);

    let input_path = Path::new(&config.input_file);
    let output_path = Path::new(&config.output_zip);

    let avail = std::thread::available_parallelism().map_or(1, |p| p.get());
    let default_threads = avail / 2 + 1;
    let extract_threads = config.extract_threads.unwrap_or(default_threads);
    let transform_threads = config.transform_threads.unwrap_or(default_threads);
    let load_threads = config.load_threads.unwrap_or(default_threads);

    if let Err(e) = engine.run(
        input_path,
        output_path,
        extract_threads,
        transform_threads,
        load_threads,
    ) {
        error!("❌ Application error: {}", e);
        process::exit(1);
    }

    let total_time = start_time.elapsed();
    info!(
        "✅ Transformation completed successfully in {:.2}s!",
        total_time.as_secs_f64()
    );

    if !config.no_metrics {
        println!("\n🎉 Apple Health transformation completed!");
        println!(
            "📊 Total execution time: {:.2} seconds",
            total_time.as_secs_f64()
        );
        println!("📁 Output saved to: {}", config.output_zip);
    }
}
