use clap::{Parser, ValueEnum};

/// Configuration for the Apple Health transformer application
#[derive(Debug, Parser)]
#[command(name = "gpt-os")]
#[command(about = "Convert Apple Health export data to structured CSV files")]
pub struct Config {
    /// Path to the Apple Health export (export.zip or export.xml)
    pub input_file: String,

    /// Path for the output archive containing CSV files
    pub output_archive: String,

    /// Archive format for the output: zip or 7z
    #[arg(long, value_enum, default_value = "zip")]
    pub format: ArchiveFormat,

    /// Enable verbose logging
    #[arg(short, long)]
    pub verbose: bool,

    /// Disable printing of end-of-run metrics
    #[arg(long)]
    pub no_metrics: bool,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum ArchiveFormat {
    Zip,
    #[value(name = "7z")]
    SevenZ,
}
