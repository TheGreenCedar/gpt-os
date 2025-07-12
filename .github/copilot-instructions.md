# Copilot Instructions for gpt-os

## Architecture Overview

This is a **generic ETL framework** built in Rust with Apple Health as the first implementation. The core design follows an Extract-Transform-Load pattern:

- **Extract**: `Extractor<T>` trait streams records from data sources
- **Transform**: Multi-threaded worker pool groups records by type using `Processable::grouping_key()`
- **Load**: `Sink<T>` trait outputs grouped records to destinations

Key architectural files:

- `src/core.rs`: Defines the ETL engine and core traits (`Processable`, `Extractor`, `Sink`)
- `src/apple_health/`: First domain implementation (extractor, types, XML parsing)
- `src/sinks/`: Output implementations (currently CSV ZIP archives)

## Development Workflows

### Essential Commands

```bash
cargo fmt              # Required before commits
cargo check            # Check compilation and warnings
cargo test             # Run full test suite (include results in PRs)
cargo build --release  # Production builds
cargo run -- -v input.zip output.zip  # Test with verbose logging
```

### Performance Testing

Target: 700k+ records/second. Use `--no-metrics` to disable throughput logging during development.

## Code Patterns

### Adding New Data Sources

1. Implement `Processable` for your record type with:
   - `grouping_key()`: How records are categorized (e.g., "HeartRate", "Steps")
   - `sort_key()`: Optional ordering within groups (typically timestamps)
2. Implement `Extractor<YourRecord>` using streaming patterns from `xml_utils.rs`
3. Wire into `Engine` in `main.rs`

### Memory Management

- Use streaming XML parsing (`quick-xml` + chunked processing)
- Memory-mapped files for large inputs (`memmap2`)
- Channel-based producer-consumer for backpressure
- `DashMap` for concurrent record grouping

### Error Handling

- Custom errors in `src/error.rs` using `thiserror`
- Channel closure signals worker completion (no explicit error passing)
- Graceful degradation: continue processing on individual record failures

## Testing Strategy

- Integration tests in `tests/` verify end-to-end CLI behavior
- Use `assert_cmd` for CLI testing with `NamedTempFile` for outputs
- Test both XML and ZIP inputs produce identical outputs
- Fixtures in `tests/fixtures/` for sample data

## Key Dependencies

- `quick-xml`: Streaming XML parser (performance critical)
- `crossbeam-channel`: Multi-producer/single-consumer channels
- `dashmap`: Concurrent HashMap for record grouping
- `rayon`: Parallel iteration (used selectively)
- `memmap2`: Memory-mapped file I/O

## Important Notes

- Update `README.md` and `docs/PROJECT_STRUCTURE.md` when changing architecture
- Performance optimizations in `Cargo.toml`: dependencies built with `opt-level = 3` even in debug
- Generic design supports future data sources beyond Apple Health
