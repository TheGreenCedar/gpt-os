use gpt_os::apple_health::types::GenericRecord;
use gpt_os::core::{Processable, Sink};
use gpt_os::sinks::csv_zip::CsvZipSink;
use quick_xml::Reader;
use quick_xml::events::Event;
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use tempfile::NamedTempFile;
use tokio_test::block_on;
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

    let mut map: HashMap<String, Vec<GenericRecord>> = HashMap::new();
    map.entry("Steps".to_string()).or_default().extend([r1, r2]);

    let tmp = NamedTempFile::new().unwrap();
    block_on(CsvZipSink.load(map, tmp.path())).unwrap();

    let file = File::open(tmp.path()).unwrap();
    let mut archive = ZipArchive::new(file).unwrap();
    let mut f = archive.by_index(0).unwrap();
    let mut csv_data = String::new();
    f.read_to_string(&mut csv_data).unwrap();
    let lines: Vec<&str> = csv_data.lines().collect();
    assert!(lines[1].contains("2023-01-01T00:00:00Z"));
    assert!(lines[2].contains("2023-01-02T00:00:00Z"));
}

#[test]
fn csv_7z_sink_sorts_records_by_date() {
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

    let mut map: HashMap<String, Vec<GenericRecord>> = HashMap::new();
    map.entry("Steps".to_string()).or_default().extend([r1, r2]);

    let tmp = NamedTempFile::new().unwrap();
    block_on(gpt_os::sinks::csv_7z::Csv7zSink.load(map, tmp.path())).unwrap();

    let mut reader =
        sevenz_rust::SevenZReader::open(tmp.path(), sevenz_rust::Password::empty()).unwrap();
    let mut csv_data = String::new();
    reader
        .for_each_entries(|_entry, mut r| {
            std::io::Read::read_to_string(&mut r, &mut csv_data).unwrap();
            Ok(true)
        })
        .unwrap();
    let lines: Vec<&str> = csv_data.lines().collect();
    assert!(lines[1].contains("2023-01-01T00:00:00Z"));
    assert!(lines[2].contains("2023-01-02T00:00:00Z"));
}
