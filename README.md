# gpt-os

`gpt-os` is a personal general data transformer built with extensibility and reusability in mind. Written in Rust, the project aims to provide a framework for transforming various personal data sources. Its first transformer focuses on Apple Health exports, converting them into structured CSV files. Processing is done in parallel to ensure efficient handling of large datasets while maintaining low memory usage.

## Features

- Multithreaded XML parsing using `quick-xml` and `Rayon` for optimal performance.
- Memory-efficient processing with streaming and chunked buffering.
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
- `-t, --threads <N>`: Limit the number of worker threads (default is all available CPU cores).
- `--no-metrics`: Disable printing of end-of-run metrics.
- `-h, --help`: Show usage information.

### Example

To process an Apple Health export and generate a ZIP file with CSV outputs:

```bash
gpt-os -v export.zip my_health_data.zip
```

## Testing

Integration tests are included in the `tests` directory. You can run the tests using:

```bash
cargo test
```

## Performance

The tool is designed to handle large Apple Health exports efficiently, targeting a throughput of at least 200,000 records per second on modern hardware.

## Future Work

Potential enhancements include:

- Direct streaming of CSV output to ZIP to reduce I/O overhead.
- Implementation of a custom memory allocator for improved performance.
- Additional optimizations for parsing and data handling.
- Support for additional personal data sources through new transformer modules.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for more details.
