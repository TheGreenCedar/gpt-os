use crossbeam_channel as channel;
use quick_xml::events::{BytesStart, Event};
use std::{path::PathBuf, sync::Arc};
use tokio::task;

use crate::error::{AppError, Result};

pub const BUFFER_SIZE: usize = 1024 * 128; // 128 KB for L2 cache optimization

type ParseFn<T> = fn(&BytesStart) -> Option<T>;

/// Core XML processing logic shared between different input sources
fn process_xml_reader<T, R>(
    reader: R,
    sender: channel::Sender<T>,
    parse_fn: ParseFn<T>,
) -> Result<()>
where
    T: Send + 'static,
    R: std::io::Read,
{
    let buf_reader = std::io::BufReader::new(reader);
    let mut xml_reader = quick_xml::reader::Reader::from_reader(buf_reader);
    xml_reader.config_mut().trim_text(true);
    let mut buf = Vec::with_capacity(BUFFER_SIZE);

    loop {
        match xml_reader.read_event_into(&mut buf) {
            Ok(Event::Start(ref e)) | Ok(Event::Empty(ref e)) => {
                if let Some(record) = parse_fn(e) {
                    if sender.send(record).is_err() {
                        // Receiver has been dropped
                        break;
                    }
                }
            }
            Ok(Event::Eof) => break,
            Err(e) => return Err(AppError::ParseError(e.to_string())),
            _ => {}
        }
        buf.clear();
    }
    Ok(())
}

pub async fn process_stream<T, R>(
    reader: R,
    sender: channel::Sender<T>,
    parse_fn: ParseFn<T>,
) -> Result<()>
where
    T: Send + 'static,
    R: std::io::Read + Send + 'static,
{
    task::spawn_blocking(move || process_xml_reader(reader, sender, parse_fn))
        .await
        .map_err(|e| AppError::Unknown(e.to_string()))?
}

/// Stream and process `export.xml` directly from a ZIP file
pub async fn process_zip_stream<T>(
    input_path: Arc<PathBuf>,
    sender: channel::Sender<T>,
    parse_fn: ParseFn<T>,
) -> Result<()>
where
    T: Send + 'static,
{
    let file = std::fs::File::open(input_path.as_ref())?;
    let mut archive = zip::ZipArchive::new(file)?;
    let export_file_name = archive
        .file_names()
        .find(|name| name.ends_with("export.xml"))
        .map(|s| s.to_string());

    if let Some(name) = export_file_name {
        task::spawn_blocking(move || -> Result<()> {
            let export_file = archive.by_name(&name)?;
            process_xml_reader(export_file, sender, parse_fn)
        })
        .await
        .map_err(|e| AppError::Unknown(e.to_string()))?
    } else {
        Err(AppError::ParseError(
            "Could not find export.xml in the zip archive".to_string(),
        ))
    }
}
