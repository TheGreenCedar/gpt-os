use crossbeam_channel as channel;
use quick_xml::events::{BytesStart, Event};
use std::{path::PathBuf, sync::Arc};
use tokio::task;

use crate::error::{AppError, Result};

pub const BUFFER_SIZE: usize = 1024 * 128; // 128 KB for L2 cache optimization
const BATCH_SIZE: usize = 10000; // Number of records to batch for parallel processing

pub type ParseFn<T> = fn(&BytesStart) -> Option<T>;

/// Parallel XML processing logic using a batched streaming approach.
fn process_xml_reader_parallel<T, R>(
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
    let mut batch = Vec::with_capacity(BATCH_SIZE);

    let pool = rayon::ThreadPoolBuilder::new().build().unwrap();

    loop {
        match xml_reader.read_event_into(&mut buf) {
            Ok(Event::Start(ref e)) | Ok(Event::Empty(ref e)) => {
                // Skip the root element to avoid processing it
                if e.name().as_ref() == b"HealthData" {
                    continue;
                }

                batch.push(e.to_owned());
                if batch.len() < BATCH_SIZE {
                    continue;
                }

                let current_batch = std::mem::take(&mut batch);
                let sender_clone = sender.clone();
                pool.spawn(move || {
                    let records: Vec<T> =
                        current_batch.iter().filter_map(|e| parse_fn(e)).collect();
                    for record in records {
                        if sender_clone.send(record).is_err() {
                            break;
                        }
                    }
                });
            }
            Ok(Event::Eof) => break,
            Err(e) => return Err(AppError::ParseError(e.to_string())),
            _ => {}
        }
        buf.clear();
    }

    // Process the final partial batch
    if !batch.is_empty() {
        let sender_clone = sender.clone();
        pool.spawn(move || {
            let records: Vec<T> = batch.iter().filter_map(|e| parse_fn(e)).collect();
            for record in records {
                if sender_clone.send(record).is_err() {
                    break;
                }
            }
        });
    }

    Ok(())
}

pub async fn process_stream_parallel<T, R>(
    reader: R,
    sender: channel::Sender<T>,
    parse_fn: ParseFn<T>,
) -> Result<()>
where
    T: Send + 'static,
    R: std::io::Read + Send + 'static,
{
    task::spawn_blocking(move || process_xml_reader_parallel(reader, sender, parse_fn))
        .await
        .map_err(|e| AppError::Unknown(e.to_string()))?
}

/// Stream and process `export.xml` directly from a ZIP file in parallel
pub async fn process_zip_stream_parallel<T>(
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
            process_xml_reader_parallel(export_file, sender, parse_fn)
        })
        .await
        .map_err(|e| AppError::Unknown(e.to_string()))?
    } else {
        Err(AppError::ParseError(
            "Could not find export.xml in the zip archive".to_string(),
        ))
    }
}
