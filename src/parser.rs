use crate::error::{AppError, Result};
use crate::types::{ActivitySummary, Record, RecordRow, Workout};
use quick_xml::Reader;
use quick_xml::events::Event;
use quick_xml::name::QName;
use std::io::BufRead;

pub fn parse_health_export<R: BufRead>(reader: R) -> Result<Vec<RecordRow>> {
    let mut records = Vec::new();
    let mut xml_reader = Reader::from_reader(reader);
    xml_reader.config_mut().trim_text(true);

    let mut buf = Vec::new();
    let mut current_record: Option<Record> = None;

    loop {
        match xml_reader.read_event_into(&mut buf) {
            Ok(Event::Start(ref e)) => match e.name() {
                QName(b"Record") => {
                    current_record = Some(Record::from_xml(e)?);
                }
                QName(b"Workout") => {
                    let workout = Workout::from_xml(e)?;
                    records.push(RecordRow::Workout(workout));
                }
                QName(b"ActivitySummary") => {
                    let summary = ActivitySummary::from_xml(e)?;
                    records.push(RecordRow::ActivitySummary(summary));
                }
                _ => {}
            },
            Ok(Event::Empty(ref e)) => match e.name() {
                QName(b"Record") => {
                    let record = Record::from_xml(e)?;
                    records.push(RecordRow::Record(record));
                }
                QName(b"Workout") => {
                    let workout = Workout::from_xml(e)?;
                    records.push(RecordRow::Workout(workout));
                }
                QName(b"ActivitySummary") => {
                    let summary = ActivitySummary::from_xml(e)?;
                    records.push(RecordRow::ActivitySummary(summary));
                }
                _ => {}
            },
            Ok(Event::End(ref e)) => match e.name() {
                QName(b"Record") => {
                    if let Some(record) = current_record.take() {
                        records.push(RecordRow::Record(record));
                    }
                }
                _ => {}
            },
            Ok(Event::Eof) => break,
            Err(e) => {
                return Err(AppError::ParseError(format!(
                    "Error at position {}: {:?}",
                    xml_reader.buffer_position(),
                    e
                )));
            }
            _ => {}
        }
        buf.clear();
    }

    Ok(records)
}
