use clap::Parser;

/// Configuration for the Apple Health transformer application
#[derive(Debug, Parser)]
#[command(name = "gpt-os")]
#[command(about = "Convert Apple Health export data to structured CSV files")]
pub struct Config {
    /// Path to the Apple Health export (export.zip or export.xml)
    pub input_file: String,

    /// Path for the output ZIP archive containing CSV files
    pub output_zip: String,

    /// Enable verbose logging
    #[arg(short, long)]
    pub verbose: bool,

    /// Disable printing of end-of-run metrics
    #[arg(long)]
    pub no_metrics: bool,
}
