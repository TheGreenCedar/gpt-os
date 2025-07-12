use crossbeam_channel as channel;
use memmap2::Mmap;
use quick_xml::{
    Reader,
    events::{BytesStart, Event},
};
use rayon::prelude::*;
use std::fs::File;
use std::path::Path;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use crate::error::{AppError, Result};

/// Target chunk size in bytes
pub const CHUNK_SIZE: usize = 2 * 1024 * 1024;

/// Find safe chunk boundaries by looking for complete XML elements
pub fn find_chunk_boundaries(content: &[u8]) -> Vec<usize> {
    let mut boundaries = vec![0];
    let mut pos = 0;
    let content_len = content.len();

    while pos < content_len {
        let target_pos = (pos + CHUNK_SIZE).min(content_len);
        let mut boundary_pos = target_pos;
        while boundary_pos > pos && boundary_pos < content_len {
            if content[boundary_pos] == b'>' {
                let mut next_pos = boundary_pos + 1;
                while next_pos < content_len && content[next_pos].is_ascii_whitespace() {
                    next_pos += 1;
                }
                if next_pos < content_len && is_element_start(&content[next_pos..]) {
                    boundary_pos = next_pos;
                    break;
                }
            }
            boundary_pos += 1;
        }
        if boundary_pos > pos && boundary_pos < content_len {
            boundaries.push(boundary_pos);
            pos = boundary_pos;
        } else {
            break;
        }
    }
    if boundaries.last() != Some(&content_len) {
        boundaries.push(content_len);
    }
    boundaries
}

fn is_element_start(data: &[u8]) -> bool {
    if data.len() < 2 || data[0] != b'<' {
        return false;
    }
    data[1].is_ascii_alphabetic()
}

/// Process a slice of XML data using a provided parser callback
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
pub fn process_chunks<T>(
    data: &[u8],
    sender: &channel::Sender<T>,
    parse_fn: Arc<dyn Fn(&BytesStart) -> Option<T> + Send + Sync + 'static>,
) -> usize
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
pub fn process_xml_file_mmap<T>(
    input_path: &Path,
    sender: &channel::Sender<T>,
    parse_fn: Arc<dyn Fn(&BytesStart) -> Option<T> + Send + Sync + 'static>,
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
pub fn process_memory_chunks<T>(
    content: &[u8],
    sender: &channel::Sender<T>,
    parse_fn: Arc<dyn Fn(&BytesStart) -> Option<T> + Send + Sync + 'static>,
) -> Result<()>
where
    T: Send + 'static,
{
    let processed = process_chunks(content, sender, parse_fn);
    log::info!("Memory chunks processed: {}", processed);
    Ok(())
}

/// Extract XML content from ZIP file and return as bytes
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
