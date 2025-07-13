# Project Structure

This document provides an overview of the repository layout and explains the purpose of the major directories and files.

```text
.
├── src/                # Application and library code
│   ├── main.rs         # Command-line entry point
│   ├── lib.rs          # Library module declarations
│   ├── config.rs       # CLI configuration and argument parsing
│   ├── core.rs         # Core traits and the transformation engine
│   ├── error.rs        # Centralized error definitions
│   ├── xml_utils.rs    # Helpers for streaming XML processing
│   ├── apple_health/   # Apple Health specific implementation
│   │   ├── extractor.rs  # Extractor reading Apple Health exports
│   │   ├── types.rs      # Data models representing XML records
│   │   └── mod.rs        # Module declarations
│   └── sinks/          # Output sinks for processed data
│       ├── csv_zip.rs    # Sink writing grouped records to zipped CSV
│       └── mod.rs
├── tests/              # Unit and integration tests
│   ├── fixtures/       # Sample XML exports used by tests
│   ├── integration_tests.rs
│   └── unit.rs
├── Cargo.toml          # Package configuration
├── README.md           # Project overview and usage instructions
├── AGENTS.md           # Coding and contribution guidelines for Codex
└── docs/               # Additional documentation (this folder)
    └── PROJECT_STRUCTURE.md (this file)
```

## Architectural Overview

The project is built around a generic transformation engine defined in `src/core.rs`. The engine orchestrates the extraction of `Processable` records from an input source and loads the grouped records into a configurable sink. The first implementation focuses on Apple Health data:

- **Extractor**: `apple_health::extractor::AppleHealthExtractor` reads zipped or plain XML exports and streams `GenericRecord` values.
- **Processable types**: Defined in `apple_health::types`, these models represent the XML elements found in the export.
- **Sink**: `sinks::csv_zip::CsvZipSink` groups the records and writes them to compressed CSV files inside a ZIP archive.

The command-line interface in `src/main.rs` wires these pieces together using `Config` from `src/config.rs`. Logging and error handling are provided by `env_logger` and the custom `error` module.

Concurrency is managed by the Tokio async runtime. CPU intensive work is executed using blocking tasks when necessary.

```
Flow: Extractor -> Engine -> Sink
```

Future transformers or sinks can implement the `Extractor` and `Sink` traits to extend the tool for new data sources or output formats.
