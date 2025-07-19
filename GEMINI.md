# Gemini Instructions for gpt-os

This document provides essential information about the `gpt-os` repository to ensure efficient and consistent development.

## 1. Project Overview

`gpt-os` is a generic, high-performance ETL (Extract-Transform-Load) framework written in Rust. Its primary goal is to process and transform personal data from various sources into structured formats. The first implementation handles Apple Health XML exports, converting them into zipped CSV files. The system is designed for extensibility, low memory usage, and high throughput via streaming and parallel processing.

## 2. Core Architecture

The application follows a classic ETL pattern orchestrated by the `Engine` in `src/core.rs`.

-   **`Extractor<T>` trait**: Responsible for reading data from a source (e.g., a file) and streaming it as `Processable` records. The primary implementation is `AppleHealthExtractor`.
-   **`Processable` trait**: Represents a single data record. It requires a `grouping_key()` (e.g., "HeartRate", "Steps") which the engine uses to group records. It can also have an optional `sort_key()`.
-   **`Engine`**: The core orchestrator. It takes an `Extractor` and a `Sink`, receives the stream of records from the extractor, groups them by the `grouping_key` in a concurrent map, and then passes the grouped data to the `Sink`.
-   **`Sink<T>` trait**: Responsible for loading the grouped records into a destination. The primary implementation is `CsvZipSink`, which writes data into compressed CSV files within a ZIP archive.

The entire process is asynchronous, leveraging `tokio` for concurrency and `crossbeam-channel` for communication between components.

## 3. Key Files & Modules

-   `src/main.rs`: Entry point. Parses CLI arguments, sets up logging, initializes the `Engine`, `Extractor`, and `Sink`, and runs the ETL process.
-   `src/core.rs`: Defines the core architectural traits (`Processable`, `Extractor`, `Sink`) and the `Engine` struct that drives the ETL pipeline.
-   `src/config.rs`: Defines the command-line interface (CLI) using `clap`.
-   `src/error.rs`: Defines custom error types using `thiserror`.
-   `src/apple_health/`: Contains the implementation for the Apple Health data source.
    -   `extractor.rs`: Implements the `Extractor` trait for Apple Health data (from XML or ZIP).
    -   `types.rs`: Defines `GenericRecord`, which implements the `Processable` and `CsvWritable` traits.
-   `src/sinks/`: Contains implementations for data sinks.
    -   `csv_zip.rs`: Implements the `Sink` trait to write records to zipped, compressed CSV files. It also defines the `CsvWritable` trait.
-   `src/xml_utils.rs`: Provides streaming XML parsing utilities built on `quick-xml`.

## 4. Development Workflow

Always follow these steps to maintain code quality.

1.  **Format code**: `cargo fmt`
2.  **Check for errors/warnings**: `cargo check`
3.  **Run tests**: `cargo test`
4.  **Run the application**: `cargo run -- [OPTIONS] <INPUT_FILE> <OUTPUT_ZIP>`
    -   Example: `cargo run -- -v tests/fixtures/sample_export.xml test_output.zip`

## 5. How to Extend the Application

### Adding a New Data Source

1.  **Create a new module**: e.g., `src/my_data_source/`.
2.  **Define your record type**: Create a struct for your data and implement `Processable` for it. You will also need to implement `CsvWritable` if you intend to use the existing `CsvZipSink`.
3.  **Implement the `Extractor` trait**: Create a struct (e.g., `MyDataSourceExtractor`) and implement `Extractor<YourRecordType>` for it. This will involve reading from the source and sending records through a channel.
4.  **Wire it into `main.rs`**: Update `main.rs` to use your new extractor, potentially based on a new CLI argument.

### Adding a New Data Sink

1.  **Create a new module**: e.g., `src/sinks/my_sink.rs`.
2.  **Implement the `Sink` trait**: Create a struct (e.g., `MySink`) and implement `Sink<T>` for it. The `load` method will receive the grouped records and write them to the desired output.
3.  **Wire it into `main.rs`**: Update `main.rs` to use your new sink.

## 6. Key Dependencies

-   `tokio`: Asynchronous runtime.
-   `clap`: Command-line argument parsing.
-   `quick-xml`: High-performance, streaming XML parser.
-   `crossbeam-channel`: Efficient MPMC channels for inter-thread communication.
-   `rayon`: For parallel data processing (used in the sink).
-   `zip`: For reading and writing ZIP archives.
-   `thiserror`: For creating custom error types.
