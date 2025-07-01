use memmap2::Mmap;
use quick_xml::{Reader, events::Event};
use rayon::prelude::*;

use crate::apple_health::types::GenericRecord;
use crate::core::Extractor;
use crate::error::{AppError, Result};
use crossbeam_channel as channel;
use std::{fs::File, path::Path, thread};

// Target chunk size in bytes
const CHUNK_SIZE: usize = 2 * 1024 * 1024;

pub struct AppleHealthExtractor;

impl Extractor<GenericRecord> for AppleHealthExtractor {
    fn extract(&self, input_path: &Path) -> Result<channel::Receiver<GenericRecord>> {
        let (sender, receiver) = channel::unbounded();
        let input_path = input_path.to_owned();

        thread::spawn(move || {
            let result: Result<()> = (|| {
                if input_path.extension().and_then(|s| s.to_str()) == Some("zip") {
                    // For ZIP files, we still need to extract to memory first
                    let content = Self::extract_xml_from_zip(&input_path)?;
                    Self::process_memory_chunks(&content, sender)?;
                } else {
                    // For regular XML files, use memory-mapped processing
                    Self::process_xml_file_mmap(&input_path, sender)?;
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

impl AppleHealthExtractor {
    /// Find safe chunk boundaries by looking for complete XML elements
    pub fn find_chunk_boundaries(content: &[u8]) -> Vec<usize> {
        let mut boundaries = vec![0];
        let mut pos = 0;
        let content_len = content.len();

        while pos < content_len {
            // Target position for next chunk
            let target_pos = (pos + CHUNK_SIZE).min(content_len);

            // Look backwards from target position to find a safe boundary
            let mut boundary_pos = target_pos;

            // Find the end of the current element by looking for ">" followed by "<"
            while boundary_pos > pos && boundary_pos < content_len {
                if content[boundary_pos] == b'>' {
                    // Look for the start of next element
                    let mut next_pos = boundary_pos + 1;
                    while next_pos < content_len && content[next_pos].is_ascii_whitespace() {
                        next_pos += 1;
                    }

                    if next_pos < content_len && Self::is_element_start(&content[next_pos..]) {
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

    /// Check if the given slice starts with an XML element
    fn is_element_start(data: &[u8]) -> bool {
        if data.len() < 2 || data[0] != b'<' {
            return false;
        }
        let c = data[1];
        c.is_ascii_alphabetic()
    }

    /// Process a slice of XML data containing complete elements
    pub fn process_chunk_slice(chunk: &[u8]) -> Result<Vec<GenericRecord>> {
        let mut results = Vec::new();
        let mut reader = Reader::from_reader(chunk);
        reader.config_mut().trim_text(true);

        let mut buf = Vec::new();
        loop {
            match reader.read_event_into(&mut buf) {
                Ok(Event::Start(ref e)) | Ok(Event::Empty(ref e)) => {
                    if let Ok(generic_rec) = GenericRecord::from_xml(e) {
                        results.push(generic_rec);
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

    /// Extract XML content from ZIP file and return as bytes
    fn extract_xml_from_zip(input_path: &Path) -> Result<Vec<u8>> {
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

    /// Process pre-split chunks and send parsed rows
    fn process_chunks(data: &[u8], sender: &channel::Sender<GenericRecord>) -> usize {
        let boundaries = Self::find_chunk_boundaries(data);

        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };

        let processed = Arc::new(AtomicUsize::new(0));

        boundaries
            .windows(2)
            .collect::<Vec<_>>()
            .par_iter()
            .for_each_with(
                (sender.clone(), Arc::clone(&processed)),
                |(s, count), window| {
                    let chunk = &data[window[0]..window[1]];
                    count.fetch_add(1, Ordering::Relaxed);
                    if let Ok(records) = Self::process_chunk_slice(chunk) {
                        for r in records {
                            let _ = s.send(r);
                        }
                    }
                },
            );

        processed.load(Ordering::Relaxed)
    }

    /// Process XML file using memory-mapped I/O
    fn process_xml_file_mmap(
        input_path: &Path,
        sender: channel::Sender<GenericRecord>,
    ) -> Result<()> {
        let file = File::open(input_path)?;
        let mmap = unsafe { Mmap::map(&file)? };

        let processed = Self::process_chunks(&mmap[..], &sender);

        log::info!("Mmap chunks processed: {}", processed);

        Ok(())
    }

    /// Process chunks from memory (for ZIP files)
    fn process_memory_chunks(content: &[u8], sender: channel::Sender<GenericRecord>) -> Result<()> {
        let processed = Self::process_chunks(content, &sender);

        log::info!("Memory chunks processed: {}", processed);

        Ok(())
    }
}
