use crate::error::{AppError, Result};
use crate::types::{ActivitySummary, Record, RecordRow, Workout};
use quick_xml::Reader;
use quick_xml::events::Event;
use quick_xml::name::QName;
use std::io::BufRead;
use std::sync::mpsc;
use std::thread;

/// Parse Apple Health export XML and emit records to a channel for streaming processing
pub fn parse_health_export_streaming<R: BufRead + Send + 'static>(
    reader: R,
) -> (mpsc::Receiver<RecordRow>, thread::JoinHandle<Result<()>>) {
    let (sender, receiver) = mpsc::channel();

    let handle = thread::spawn(move || {
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
                        if sender.send(RecordRow::Workout(workout)).is_err() {
                            // Receiver dropped, stop parsing
                            break;
                        }
                    }
                    QName(b"ActivitySummary") => {
                        let summary = ActivitySummary::from_xml(e)?;
                        if sender.send(RecordRow::ActivitySummary(summary)).is_err() {
                            // Receiver dropped, stop parsing
                            break;
                        }
                    }
                    _ => {}
                },
                Ok(Event::Empty(ref e)) => match e.name() {
                    QName(b"Record") => {
                        let record = Record::from_xml(e)?;
                        if sender.send(RecordRow::Record(record)).is_err() {
                            // Receiver dropped, stop parsing
                            break;
                        }
                    }
                    QName(b"Workout") => {
                        let workout = Workout::from_xml(e)?;
                        if sender.send(RecordRow::Workout(workout)).is_err() {
                            // Receiver dropped, stop parsing
                            break;
                        }
                    }
                    QName(b"ActivitySummary") => {
                        let summary = ActivitySummary::from_xml(e)?;
                        if sender.send(RecordRow::ActivitySummary(summary)).is_err() {
                            // Receiver dropped, stop parsing
                            break;
                        }
                    }
                    _ => {}
                },
                Ok(Event::End(ref e)) => match e.name() {
                    QName(b"Record") => {
                        if let Some(record) = current_record.take() {
                            if sender.send(RecordRow::Record(record)).is_err() {
                                // Receiver dropped, stop parsing
                                break;
                            }
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

        // Drop sender to signal completion
        drop(sender);
        Ok(())
    });

    (receiver, handle)
}
