use crossbeam_channel as channel;
use quick_xml::{
    Reader,
    events::{BytesStart, Event},
};
use std::fs::File;
use std::io::Read;
use std::path::Path;

use crate::error::{AppError, Result};

type ParseFn<T> = fn(&BytesStart) -> Option<T>;

/// Target chunk size in bytes - optimized for L2 cache efficiency
pub const CHUNK_SIZE: usize = 128 * 1024; // 128KB

/// Advance index past any whitespace characters and return the new position.
fn skip_whitespace(data: &[u8], mut idx: usize) -> usize {
    while idx < data.len() && data[idx].is_ascii_whitespace() {
        idx += 1;
    }
    idx
}

/// Return the position of the next element start if it immediately follows a
/// closing tag. `start` should point just after a `>` character.
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

fn is_element_start(data: &[u8]) -> bool {
    if data.len() < 2 || data[0] != b'<' {
        return false;
    }
    data[1].is_ascii_alphabetic()
}

/// Process a slice of XML data using a provided parser callback
pub fn process_chunk_slice<T>(chunk: &[u8], parse_fn: ParseFn<T>) -> Result<Vec<T>> {
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

/// Read data from `reader` into `buffer` using a temporary slice.
///
/// Returns `Ok(true)` if bytes were read, `Ok(false)` on EOF.
fn read_to_buffer<R: Read>(reader: &mut R, buffer: &mut Vec<u8>, tmp: &mut [u8]) -> Result<bool> {
    match reader.read(tmp) {
        Ok(0) => Ok(false),
        Ok(n) => {
            buffer.extend_from_slice(&tmp[..n]);
            Ok(true)
        }
        Err(e) => Err(AppError::ParseError(format!("Read error: {}", e))),
    }
}

/// Spawn a task to parse `chunk` and send all parsed records on `sender`.
fn dispatch_chunk<T>(
    scope: &rayon::Scope<'_>,
    chunk: Vec<u8>,
    sender: channel::Sender<T>,
    parse_fn: ParseFn<T>,
) where
    T: Send + 'static,
{
    scope.spawn(move |_| {
        if let Ok(records) = process_chunk_slice(&chunk, parse_fn) {
            for r in records {
                let _ = sender.send(r);
            }
        }
    });
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
    let mut buffer = Vec::with_capacity(CHUNK_SIZE);
    let mut result: Result<()> = Ok(());

    rayon::scope(|scope| {
        while result.is_ok() {
            match read_to_buffer(&mut reader, &mut buffer, &mut read_buf) {
                Ok(true) => {}
                Ok(false) => break,
                Err(e) => {
                    result = Err(e);
                    break;
                }
            }

            loop {
                if buffer.len() < CHUNK_SIZE {
                    break;
                }

                if let Some(boundary) = find_boundary_after(&buffer, CHUNK_SIZE) {
                    let chunk: Vec<u8> = buffer.drain(..boundary).collect();
                    dispatch_chunk(scope, chunk, sender.clone(), parse_fn);
                } else {
                    break;
                }
            }
        }

        if result.is_ok() && !buffer.is_empty() {
            let chunk: Vec<u8> = buffer.drain(..).collect();
            dispatch_chunk(scope, chunk, sender.clone(), parse_fn);
        }
    });

    result
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
