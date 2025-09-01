use criterion::{Criterion, criterion_group, criterion_main};
use gpt_os::apple_health::types::GenericRecord;
use quick_xml::Reader;
use quick_xml::events::Event;

fn parse_record(data: &[u8]) {
    let mut reader = Reader::from_reader(data);
    reader.config_mut().trim_text(true);
    let mut buf = Vec::new();
    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) | Ok(Event::Empty(e)) => {
                let _ = GenericRecord::from_xml(&e).unwrap();
                break;
            }
            Ok(Event::Eof) => break,
            _ => {}
        }
        buf.clear();
    }
}

fn benchmark_generic_record(c: &mut Criterion) {
    let data = br#"<Record type="Heart" value="60" creationDate="2020" startDate="2020" endDate="2020" sourceName="watch"/>"#;
    c.bench_function("parse_generic_record", |b| {
        b.iter(|| parse_record(std::hint::black_box(data)))
    });
}

criterion_group!(benches, benchmark_generic_record);
criterion_main!(benches);
