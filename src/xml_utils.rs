use crossbeam_channel as channel;
use memmap2::Mmap;
use quick_xml::{
    Reader,
    events::{BytesStart, Event},
};
use rayon::prelude::*;
use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use crate::error::{AppError, Result};

type ParseFn<T> = Arc<dyn Fn(&BytesStart) -> Option<T> + Send + Sync + 'static>;

/// Target chunk size in bytes
pub const CHUNK_SIZE: usize = 2 * 1024 * 1024;

/// Advance index past any whitespace characters and return the new position.
#[allow(dead_code)]
fn skip_whitespace(data: &[u8], mut idx: usize) -> usize {
    while idx < data.len() && data[idx].is_ascii_whitespace() {
        idx += 1;
    }
    idx
}

/// Return the position of the next element start if it immediately follows a
/// closing tag. `start` should point just after a `>` character.
#[allow(dead_code)]
fn advance_to_next_element(data: &[u8], start: usize) -> Option<usize> {
    let idx = skip_whitespace(data, start);
    if idx < data.len() && is_element_start(&data[idx..]) {
        Some(idx)
    } else {
        None
    }
}

/// Scan forward from `start` looking for a safe boundary which is the beginning
/// of the next XML element. Returns `None` if no such boundary exists.
#[allow(dead_code)]
fn find_boundary_after(data: &[u8], mut start: usize) -> Option<usize> {
    let len = data.len();
    while start < len {
        if data[start] == b'>' {
            if let Some(pos) = advance_to_next_element(data, start + 1) {
                return Some(pos);
            }
        }
        start += 1;
    }
    None
}

/// Find safe chunk boundaries by looking for complete XML elements.
///
/// The function walks through the input in roughly `CHUNK_SIZE` steps. From
/// each tentative boundary it scans forward until it finds a closing `>` that is
/// followed (ignoring whitespace) by the start of a new element. Splitting on
/// these positions guarantees that no chunk begins in the middle of an XML
/// element.
#[allow(dead_code)]
pub fn find_chunk_boundaries(content: &[u8]) -> Vec<usize> {
    let mut boundaries = vec![0];
    let mut pos = 0;
    let content_len = content.len();

    while pos < content_len {
        // Jump ahead approximately `CHUNK_SIZE` bytes from the last boundary
        // and try to align the next chunk with the start of an element.
        let target_pos = (pos + CHUNK_SIZE).min(content_len);

        // Search for the next `>` followed by an element start. This ensures we
        // never split inside an element.
        if let Some(boundary_pos) = find_boundary_after(content, target_pos) {
            if boundary_pos < content_len {
                boundaries.push(boundary_pos);
                pos = boundary_pos;
                continue;
            }
        }
        break;
    }

    // Always include the end of the data as the final boundary.
    if boundaries.last() != Some(&content_len) {
        boundaries.push(content_len);
    }
    boundaries
}

#[allow(dead_code)]
fn is_element_start(data: &[u8]) -> bool {
    if data.len() < 2 || data[0] != b'<' {
        return false;
    }
    data[1].is_ascii_alphabetic()
}

/// Process a slice of XML data using a provided parser callback
#[allow(dead_code)]
pub fn process_chunk_slice<T>(
    chunk: &[u8],
    parse_fn: &dyn Fn(&BytesStart) -> Option<T>,
) -> Result<Vec<T>> {
    let mut results = Vec::new();
    let mut reader = Reader::from_reader(chunk);
    reader.config_mut().trim_text(true);
    let mut buf = Vec::new();
    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(ref e)) | Ok(Event::Empty(ref e)) => {
                if let Some(r) = parse_fn(e) {
                    results.push(r);
                }
            }
            Ok(Event::Eof) => break,
            Err(_) => break,
            _ => {}
        }
        buf.clear();
    }
    Ok(results)
}

/// Process pre-split chunks and send parsed rows
#[allow(dead_code)]
pub fn process_chunks<T>(data: &[u8], sender: &channel::Sender<T>, parse_fn: ParseFn<T>) -> usize
where
    T: Send + 'static,
{
    let boundaries = find_chunk_boundaries(data);
    let processed = Arc::new(AtomicUsize::new(0));
    boundaries
        .windows(2)
        .collect::<Vec<_>>()
        .par_iter()
        .for_each_with(
            (
                sender.clone(),
                Arc::clone(&processed),
                Arc::clone(&parse_fn),
            ),
            |(s, count, pf), window| {
                let chunk = &data[window[0]..window[1]];
                count.fetch_add(1, Ordering::Relaxed);
                if let Ok(records) = process_chunk_slice(chunk, &**pf) {
                    for r in records {
                        let _ = s.send(r);
                    }
                }
            },
        );
    processed.load(Ordering::Relaxed)
}

/// Process XML file using memory-mapped I/O
#[allow(dead_code)]
pub fn process_xml_file_mmap<T>(
    input_path: &Path,
    sender: &channel::Sender<T>,
    parse_fn: ParseFn<T>,
) -> Result<()>
where
    T: Send + 'static,
{
    let file = File::open(input_path)?;
    let mmap = unsafe { Mmap::map(&file)? };
    let processed = process_chunks(&mmap[..], sender, parse_fn);
    log::info!("Mmap chunks processed: {}", processed);
    Ok(())
}

/// Process chunks from memory (for ZIP files)
#[allow(dead_code)]
pub fn process_memory_chunks<T>(
    content: &[u8],
    sender: &channel::Sender<T>,
    parse_fn: ParseFn<T>,
) -> Result<()>
where
    T: Send + 'static,
{
    let processed = process_chunks(content, sender, parse_fn);
    log::info!("Memory chunks processed: {}", processed);
    Ok(())
}

/// Process XML from any `Read` stream in parallel chunks without loading the
/// entire file into memory.
pub fn process_stream<T, R>(
    mut reader: R,
    sender: &channel::Sender<T>,
    parse_fn: ParseFn<T>,
) -> Result<()>
where
    T: Send + 'static,
    R: Read + Send,
{
    let mut read_buf = vec![0u8; CHUNK_SIZE];
    let mut buffer = Vec::with_capacity(CHUNK_SIZE * 2);
    let mut result: Result<()> = Ok(());

    rayon::scope(|scope| {
        while result.is_ok() {
            match reader.read(&mut read_buf) {
                Ok(0) => break,
                Ok(n) => buffer.extend_from_slice(&read_buf[..n]),
                Err(e) => {
                    result = Err(AppError::ParseError(format!("Read error: {}", e)));
                    break;
                }
            }

            loop {
                if buffer.len() < CHUNK_SIZE {
                    break;
                }

                if let Some(boundary) = find_boundary_after(&buffer, CHUNK_SIZE) {
                    let chunk: Vec<u8> = buffer.drain(..boundary).collect();
                    let s = sender.clone();
                    let pf = Arc::clone(&parse_fn);
                    scope.spawn(move |_| {
                        if let Ok(records) = process_chunk_slice(&chunk, &*pf) {
                            for r in records {
                                let _ = s.send(r);
                            }
                        }
                    });
                } else {
                    break;
                }
            }
        }

        if result.is_ok() && !buffer.is_empty() {
            let chunk: Vec<u8> = buffer.drain(..).collect();
            let s = sender.clone();
            let pf = Arc::clone(&parse_fn);
            scope.spawn(move |_| {
                if let Ok(records) = process_chunk_slice(&chunk, &*pf) {
                    for r in records {
                        let _ = s.send(r);
                    }
                }
            });
        }
    });

    result
}

/// Extract XML content from ZIP file and return as bytes
#[allow(dead_code)]
pub fn extract_xml_from_zip(input_path: &Path) -> Result<Vec<u8>> {
    use std::io::Read;
    let file = File::open(input_path)?;
    let mut archive = zip::ZipArchive::new(file)?;
    let export_file_name = archive
        .file_names()
        .find(|name| name.ends_with("export.xml"))
        .map(|s| s.to_string());
    if let Some(name) = export_file_name {
        let mut export_file = archive.by_name(&name)?;
        let mut content = Vec::new();
        export_file.read_to_end(&mut content)?;
        Ok(content)
    } else {
        Err(AppError::ParseError(
            "Could not find export.xml in the zip archive".to_string(),
        ))
    }
}

/// Stream and process `export.xml` directly from a ZIP file
pub fn process_zip_stream<T>(
    input_path: &Path,
    sender: &channel::Sender<T>,
    parse_fn: ParseFn<T>,
) -> Result<()>
where
    T: Send + 'static,
{
    let file = File::open(input_path)?;
    let mut archive = zip::ZipArchive::new(file)?;
    let export_file_name = archive
        .file_names()
        .find(|name| name.ends_with("export.xml"))
        .map(|s| s.to_string());
    if let Some(name) = export_file_name {
        let export_file = archive.by_name(&name)?;
        process_stream(export_file, sender, parse_fn)
    } else {
        Err(AppError::ParseError(
            "Could not find export.xml in the zip archive".to_string(),
        ))
    }
}
