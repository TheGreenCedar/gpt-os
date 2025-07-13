use criterion::{Criterion, criterion_group, criterion_main};
use std::process::Command;
use std::time::Duration;
use tempfile::NamedTempFile;

fn bench_sample(c: &mut Criterion) {
    c.bench_function("process_sample_export", |b| {
        b.iter(|| {
            // Create a temporary output file for each iteration
            let output = NamedTempFile::with_suffix(".zip").expect("temp file");
            // Invoke the CLI binary to measure full execution
            let status = Command::new(env!("CARGO_BIN_EXE_gpt-os"))
                .arg("AppleHealth2025-06-28.zip")
                .arg(output.path())
                .status()
                .expect("failed to execute process");
            assert!(status.success());
        });
    });
}

// Replace default group to adjust measurement time and sample size
criterion_group! {
    name = benches;
    config = Criterion::default()
        .measurement_time(Duration::from_secs(15))
        .sample_size(10);
    targets = bench_sample
}
criterion_main!(benches);
