use criterion::{Criterion, criterion_group, criterion_main};
use gpt_os::apple_health::extractor::AppleHealthExtractor;
use gpt_os::core::Engine;
use gpt_os::sinks::csv_zip::CsvZipSink;
use std::path::Path;
use tempfile::NamedTempFile;
use tokio::runtime::Runtime;

fn bench_sample(c: &mut Criterion) {
    let rt = Runtime::new().expect("runtime");
    c.bench_function("process_sample_export", |b| {
        b.iter(|| {
            let extractor = AppleHealthExtractor;
            let sink = CsvZipSink;
            let engine = Engine::new(extractor, sink);
            let input = Path::new("tests/fixtures/sample_export.xml");
            let output = NamedTempFile::new().expect("temp file");
            rt.block_on(async {
                engine.run(input, output.path()).await.expect("run engine");
            });
        });
    });
}

criterion_group!(benches, bench_sample);
criterion_main!(benches);
