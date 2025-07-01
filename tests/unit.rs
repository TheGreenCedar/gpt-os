use gpt_os::apple_health::extractor::AppleHealthExtractor;
use gpt_os::apple_health::types::{ActivitySummary, Record, RecordRow, Workout};
use quick_xml::Reader;
use quick_xml::events::Event;

#[test]
fn record_from_xml_optional_fields() {
    let xml = r#"<Record type="Heart" value="60" creationDate="2020" startDate="2020" endDate="2020" sourceName="watch"/>"#;
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);
    let mut buf = Vec::new();
    match reader.read_event_into(&mut buf).unwrap() {
        Event::Empty(e) => {
            let rec = Record::from_xml(&e).unwrap();
            assert_eq!(rec.record_type, "Heart");
            assert_eq!(rec.value, "60");
            assert_eq!(rec.unit, None);
            assert_eq!(rec.source_version, None);
            assert_eq!(rec.device, None);
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
            let workout = Workout::from_xml(&e).unwrap();
            assert_eq!(workout.activity_type, "Run");
            assert_eq!(workout.duration, 42.5);
            assert_eq!(workout.total_distance, Some(5.2));
            assert_eq!(workout.total_energy_burned, Some(300.0));
            assert_eq!(workout.device, None);
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
            let summary = ActivitySummary::from_xml(&e).unwrap();
            assert_eq!(summary.date_components, "2023-01-01");
            assert_eq!(summary.active_energy_burned, Some(300.0));
            assert_eq!(summary.active_energy_burned_goal, Some(500.0));
            assert_eq!(summary.apple_exercise_time, Some(30.0));
            assert_eq!(summary.apple_stand_hours, Some(12.0));
        }
        _ => panic!("Expected empty ActivitySummary event"),
    }
}

#[test]
fn find_chunk_boundaries_basic() {
    let xml = b"<Record/><Workout/><ActivitySummary/>";
    let boundaries = AppleHealthExtractor::find_chunk_boundaries(xml);
    assert_eq!(boundaries, vec![0, xml.len()]);
}

#[test]
fn find_chunk_boundaries_multiple() {
    let element = b"<Record/>";
    let repeat = (2 * 1024 * 1024 / element.len()) + 10;
    let data = element.repeat(repeat);
    let boundaries = AppleHealthExtractor::find_chunk_boundaries(&data);
    assert!(boundaries.len() >= 2); // at least start and end
    assert_eq!(*boundaries.first().unwrap(), 0);
    assert_eq!(*boundaries.last().unwrap(), data.len());
}

#[test]
fn process_chunk_slice_detects_types() {
    let chunk = br#"<Record type="Steps" value="1" creationDate="2020" startDate="2020" endDate="2020" sourceName="watch"/><Workout workoutActivityType="Run" duration="10" sourceName="watch" startDate="2020" endDate="2020"/><ActivitySummary dateComponents="2023-01-01"/>"#;
    let records = AppleHealthExtractor::process_chunk_slice(chunk).unwrap();
    assert_eq!(records.len(), 3);
    assert!(matches!(records[0], RecordRow::Record(_)));
    assert!(matches!(records[1], RecordRow::Workout(_)));
    assert!(matches!(records[2], RecordRow::ActivitySummary(_)));
}
