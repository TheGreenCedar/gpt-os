use dashmap::DashMap;
use gpt_os::apple_health::types::GenericRecord;
use gpt_os::core::{Processable, Sink};
use gpt_os::sinks::csv_zip::CsvZipSink;
use gpt_os::xml_utils;
use quick_xml::Reader;
use quick_xml::events::Event;
use std::fs::File;
use std::io::Read;
use tempfile::NamedTempFile;
use zip::ZipArchive;

#[test]
fn record_from_xml_optional_fields() {
    let xml = r#"<Record type="Heart" value="60" creationDate="2020" startDate="2020" endDate="2020" sourceName="watch"/>"#;
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);
    let mut buf = Vec::new();
    match reader.read_event_into(&mut buf).unwrap() {
        Event::Empty(e) => {
            let rec = GenericRecord::from_xml(&e).unwrap();
            assert_eq!(rec.element_name, "Record");
            assert_eq!(rec.attributes.get("type").unwrap(), "Heart");
            assert_eq!(rec.attributes.get("value").unwrap(), "60");
            assert_eq!(rec.attributes.get("unit"), None);
            assert_eq!(rec.attributes.get("sourceVersion"), None);
            assert_eq!(rec.attributes.get("device"), None);
        }
        _ => panic!("Expected empty Record event"),
    }
}

#[test]
fn workout_from_xml_numeric_fields() {
    let xml = r#"<Workout workoutActivityType="Run" duration="42.5" totalDistance="5.2" totalEnergyBurned="300" sourceName="watch" startDate="2020" endDate="2020"/>"#;
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);
    let mut buf = Vec::new();
    match reader.read_event_into(&mut buf).unwrap() {
        Event::Empty(e) => {
            let workout = GenericRecord::from_xml(&e).unwrap();
            assert_eq!(workout.element_name, "Workout");
            assert_eq!(
                workout.attributes.get("workoutActivityType").unwrap(),
                "Run"
            );
            assert_eq!(workout.attributes.get("duration").unwrap(), "42.5");
            assert_eq!(workout.attributes.get("totalDistance").unwrap(), "5.2");
            assert_eq!(workout.attributes.get("totalEnergyBurned").unwrap(), "300");
            assert_eq!(workout.attributes.get("device"), None);
        }
        _ => panic!("Expected empty Workout event"),
    }
}

#[test]
fn activity_summary_from_xml_numeric_fields() {
    let xml = r#"<ActivitySummary dateComponents="2023-01-01" activeEnergyBurned="300" activeEnergyBurnedGoal="500" appleExerciseTime="30" appleStandHours="12"/>"#;
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);
    let mut buf = Vec::new();
    match reader.read_event_into(&mut buf).unwrap() {
        Event::Empty(e) => {
            let summary = GenericRecord::from_xml(&e).unwrap();
            assert_eq!(summary.element_name, "ActivitySummary");
            assert_eq!(
                summary.attributes.get("dateComponents").unwrap(),
                "2023-01-01"
            );
            assert_eq!(summary.attributes.get("activeEnergyBurned").unwrap(), "300");
            assert_eq!(
                summary.attributes.get("activeEnergyBurnedGoal").unwrap(),
                "500"
            );
            assert_eq!(summary.attributes.get("appleExerciseTime").unwrap(), "30");
            assert_eq!(summary.attributes.get("appleStandHours").unwrap(), "12");
        }
        _ => panic!("Expected empty ActivitySummary event"),
    }
}

#[test]
fn generic_record_from_xml() {
    let xml = r#"<Correlation type="Blood" startDate="2020" endDate="2020"/>"#;
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);
    let mut buf = Vec::new();
    match reader.read_event_into(&mut buf).unwrap() {
        Event::Empty(e) => {
            let g = GenericRecord::from_xml(&e).unwrap();
            assert_eq!(g.element_name, "Correlation");
            assert_eq!(g.attributes.get("type").unwrap(), "Blood");
            assert_eq!(g.attributes.get("startDate").unwrap(), "2020");
        }
        _ => panic!("Expected empty event"),
    }
}

#[test]
fn generic_record_grouping_key_for_record() {
    let xml = r#"<Record type="HKQuantityTypeIdentifierBodyMass" value="70" startDate="2020" endDate="2020" creationDate="2020" sourceName="watch"/>"#;
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);
    let mut buf = Vec::new();
    match reader.read_event_into(&mut buf).unwrap() {
        Event::Empty(e) => {
            let g = GenericRecord::from_xml(&e).unwrap();
            assert_eq!(g.grouping_key(), "HKQuantityTypeIdentifierBodyMass");
        }
        _ => panic!("Expected empty Record event"),
    }
}

#[test]
fn find_chunk_boundaries_basic() {
    let xml = b"<Record/><Workout/><ActivitySummary/>";
    let boundaries = xml_utils::find_chunk_boundaries(xml);
    assert_eq!(boundaries, vec![0, xml.len()]);
}

#[test]
fn find_chunk_boundaries_multiple() {
    let element = b"<Record/>";
    let repeat = (2 * 1024 * 1024 / element.len()) + 10;
    let data = element.repeat(repeat);
    let boundaries = xml_utils::find_chunk_boundaries(&data);
    assert!(boundaries.len() >= 2); // at least start and end
    assert_eq!(*boundaries.first().unwrap(), 0);
    assert_eq!(*boundaries.last().unwrap(), data.len());
}

#[test]
fn process_chunk_slice_detects_types() {
    let chunk = br#"<Record type="Steps" value="1" creationDate="2020" startDate="2020" endDate="2020" sourceName="watch"/><Workout workoutActivityType="Run" duration="10" sourceName="watch" startDate="2020" endDate="2020"/><ActivitySummary dateComponents="2023-01-01"/><Correlation type="BP"/>"#;
    let parse_fn = |e: &quick_xml::events::BytesStart| GenericRecord::from_xml(e).ok();
    let records = xml_utils::process_chunk_slice(chunk, &parse_fn).unwrap();
    assert_eq!(records.len(), 4);
    assert_eq!(records[0].element_name, "Record");
    assert_eq!(records[1].element_name, "Workout");
    assert_eq!(records[2].element_name, "ActivitySummary");
    assert_eq!(records[3].element_name, "Correlation");
}

#[test]
fn csv_sink_sorts_records_by_date() {
    let xml1 =
        r#"<Record type="Steps" startDate="2023-01-02T00:00:00Z" endDate="2023-01-02T00:00:00Z"/>"#;
    let xml2 =
        r#"<Record type="Steps" startDate="2023-01-01T00:00:00Z" endDate="2023-01-01T00:00:00Z"/>"#;

    let parse = |xml: &str| {
        let mut reader = Reader::from_str(xml);
        reader.config_mut().trim_text(true);
        let mut buf = Vec::new();
        match reader.read_event_into(&mut buf).unwrap() {
            Event::Empty(e) => GenericRecord::from_xml(&e).unwrap(),
            _ => panic!("expected empty"),
        }
    };

    let r1 = parse(xml1);
    let r2 = parse(xml2);

    let map: DashMap<String, Vec<GenericRecord>> = DashMap::new();
    map.entry("Steps".to_string()).or_default().extend([r1, r2]);

    let tmp = NamedTempFile::new().unwrap();
    CsvZipSink.load(map, tmp.path()).unwrap();

    let file = File::open(tmp.path()).unwrap();
    let mut archive = ZipArchive::new(file).unwrap();
    let mut f = archive.by_index(0).unwrap();
    let mut csv_data = String::new();
    f.read_to_string(&mut csv_data).unwrap();
    let lines: Vec<&str> = csv_data.lines().collect();
    assert!(lines[1].contains("2023-01-01T00:00:00Z"));
    assert!(lines[2].contains("2023-01-02T00:00:00Z"));
}
