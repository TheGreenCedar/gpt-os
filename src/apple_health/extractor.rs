use quick_xml::{Reader, events::Event, name::QName};

use crate::apple_health::types::{ActivitySummary, Record, RecordRow, Workout};
use crate::core::Extractor;
use crate::error::{AppError, Result};
use std::{fs::File, path::Path, sync::mpsc, thread};

pub struct AppleHealthExtractor;

impl Extractor<RecordRow> for AppleHealthExtractor {
    fn extract(&self, input_path: &Path) -> Result<mpsc::Receiver<RecordRow>> {
        let (sender, receiver) = mpsc::channel();
        let input_path = input_path.to_owned();

        thread::spawn(move || {
            let result: Result<()> = (|| {
                if input_path.extension().and_then(|s| s.to_str()) == Some("zip") {
                    // Handle ZIP file
                    let file = File::open(&input_path)?;
                    let mut archive = zip::ZipArchive::new(file)?;
                    let export_file_name = archive
                        .file_names()
                        .find(|name| name.ends_with("export.xml"))
                        .map(|s| s.to_string());

                    if let Some(name) = export_file_name {
                        let export_file = archive.by_name(&name)?;
                        let buf_reader = std::io::BufReader::new(export_file);
                        let mut xml_reader = Reader::from_reader(buf_reader);
                        xml_reader.config_mut().trim_text(true);

                        let mut buf = Vec::new();
                        loop {
                            match xml_reader.read_event_into(&mut buf) {
                                Ok(Event::Start(ref e)) | Ok(Event::Empty(ref e)) => match e.name()
                                {
                                    QName(b"Record") => {
                                        if let Ok(record) = Record::from_xml(e) {
                                            if sender.send(RecordRow::Record(record)).is_err() {
                                                break;
                                            }
                                        }
                                    }
                                    QName(b"Workout") => {
                                        if let Ok(workout) = Workout::from_xml(e) {
                                            if sender.send(RecordRow::Workout(workout)).is_err() {
                                                break;
                                            }
                                        }
                                    }
                                    QName(b"ActivitySummary") => {
                                        if let Ok(summary) = ActivitySummary::from_xml(e) {
                                            if sender
                                                .send(RecordRow::ActivitySummary(summary))
                                                .is_err()
                                            {
                                                break;
                                            }
                                        }
                                    }
                                    _ => {}
                                },
                                Ok(Event::Eof) => break,
                                Err(_) => break,
                                _ => {}
                            }
                            buf.clear();
                        }
                    } else {
                        return Err(AppError::ParseError(
                            "Could not find export.xml in the zip archive".to_string(),
                        ));
                    }
                } else {
                    // Handle plain XML file
                    let file = File::open(&input_path)?;
                    let reader = std::io::BufReader::new(file);
                    let mut xml_reader = Reader::from_reader(reader);
                    xml_reader.config_mut().trim_text(true);

                    let mut buf = Vec::new();
                    loop {
                        match xml_reader.read_event_into(&mut buf) {
                            Ok(Event::Start(ref e)) | Ok(Event::Empty(ref e)) => match e.name() {
                                QName(b"Record") => {
                                    if let Ok(record) = Record::from_xml(e) {
                                        if sender.send(RecordRow::Record(record)).is_err() {
                                            break;
                                        }
                                    }
                                }
                                QName(b"Workout") => {
                                    if let Ok(workout) = Workout::from_xml(e) {
                                        if sender.send(RecordRow::Workout(workout)).is_err() {
                                            break;
                                        }
                                    }
                                }
                                QName(b"ActivitySummary") => {
                                    if let Ok(summary) = ActivitySummary::from_xml(e) {
                                        if sender.send(RecordRow::ActivitySummary(summary)).is_err()
                                        {
                                            break;
                                        }
                                    }
                                }
                                _ => {}
                            },
                            Ok(Event::Eof) => break,
                            Err(_) => break,
                            _ => {}
                        }
                        buf.clear();
                    }
                }
                Ok(())
            })();

            if let Err(_) = result {
                // Error occurred, but we can't send it through the channel
                // The receiver will detect the channel is closed
            }
        });

        Ok(receiver)
    }
}
