# gpt-os

`gpt-os` is a personal general data transformer built with extensibility and reusability in mind. It's called gpt-os because I'm vibe coding it with Codex from my phone for the heck of it, and I'm using it to transform data that I want to add inside my chatgpt projects or want to do a deep research on.

Written in Rust, the project aims to provide a framework for transforming various personal data sources. Its first transformer focuses on Apple Health exports, converting them into structured CSV files. Processing is done in parallel to ensure efficient handling of large datasets while maintaining low memory usage.

## Features

- Multithreaded XML parsing using `quick-xml` and `Rayon` for optimal performance.
- Memory-efficient processing with streaming and chunked buffering.
- Dedicated Rayon thread pools for each phase are preloaded at startup to avoid latency.
- Outputs structured CSV files for various health record types, all compressed into a single ZIP archive.
- Robust error handling and logging capabilities.
- Cross-platform compatibility (Linux, macOS, Windows).

## Installation

To build the project, ensure you have Rust installed on your machine. Clone the repository and run the following commands:

```bash
cargo build --release
```

## Usage

The tool can be executed from the command line as follows:

```bash
gpt-os [OPTIONS] <INPUT_FILE> <OUTPUT_ZIP>
```

### Arguments

- `<INPUT_FILE>`: Path to the Apple Health export (either the `export.zip` file or an already-unzipped `export.xml` file).
- `<OUTPUT_ZIP>`: Path for the resulting ZIP archive containing the CSV files.

### Options

- `-v, --verbose`: Enable verbose logging.
- `--extract-threads <N>`: Threads for extraction phase (default: available/2 + 1).
- `--transform-threads <N>`: Threads for transformation phase (default: available/2 + 1).
- `--load-threads <N>`: Threads for load phase (default: available/2 + 1).
- `--no-metrics`: Disable printing of end-of-run metrics.
- `-h, --help`: Show usage information.

### Example

To process an Apple Health export and generate a ZIP file with CSV outputs:

```bash
gpt-os -v export.zip my_health_data.zip
```

## Project Structure

The repository contains the Rust source code under `src/`, tests in `tests/`, and
additional documentation in `docs/`. Key modules include `core.rs` for the
transformation engine, `apple_health/` for domain-specific extractors and data
types, and `sinks/` for output implementations. A full breakdown of files and
their roles is available in [docs/PROJECT_STRUCTURE.md](docs/PROJECT_STRUCTURE.md).

## Testing

Integration tests are included in the `tests` directory. You can run the tests using:

```bash
cargo test
```

## Performance

The tool is designed to handle large Apple Health exports efficiently, targeting a throughput of at least 700,000 records per second (~6 to 12 months worth of data per second) on an 8-core, hyperthreaded CPU with an ssd.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for more details.
