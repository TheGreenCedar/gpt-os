use quick_xml::{Reader, events::Event, name::QName};
use rayon::prelude::*;

use crate::apple_health::types::{ActivitySummary, Record, RecordRow, Workout};
use crate::core::Extractor;
use crate::error::{AppError, Result};
use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
    path::Path,
    sync::mpsc,
    thread,
};

// Target chunk size in bytes (1MB chunks)
const CHUNK_SIZE: usize = 1024 * 1024;
// Buffer size for finding boundaries (should be larger than max XML element size)
const BOUNDARY_BUFFER_SIZE: usize = 64 * 1024; // 64KB

// XML elements we're interested in
const TARGET_ELEMENTS: &[&[u8]] = &[b"Record", b"Workout", b"ActivitySummary"];

#[derive(Debug, Clone)]
struct XmlChunk {
    data: Vec<u8>,
}

#[derive(Debug, Clone)]
struct ChunkMetrics {
    chunk_index: usize,
    chunk_size: usize,
    record_count: usize,
}

pub struct AppleHealthExtractor;

impl Extractor<RecordRow> for AppleHealthExtractor {
    fn extract(&self, input_path: &Path) -> Result<mpsc::Receiver<RecordRow>> {
        let (sender, receiver) = mpsc::channel();
        let input_path = input_path.to_owned();

        thread::spawn(move || {
            let result: Result<()> = (|| {
                if input_path.extension().and_then(|s| s.to_str()) == Some("zip") {
                    // For ZIP files, we still need to extract to memory first
                    let content = Self::extract_xml_from_zip(&input_path)?;
                    Self::process_memory_chunks(&content, sender)?;
                } else {
                    // For regular XML files, use streaming approach
                    Self::process_xml_file_streaming(&input_path, sender)?;
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
    fn find_chunk_boundaries(content: &[u8]) -> Vec<usize> {
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

                    if next_pos < content_len && content[next_pos] == b'<' {
                        // Check if this is one of our target elements
                        if Self::is_target_element_start(&content[next_pos..]) {
                            boundary_pos = next_pos;
                            break;
                        }
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

    /// Check if the given slice starts with one of our target XML elements
    fn is_target_element_start(data: &[u8]) -> bool {
        for &element in TARGET_ELEMENTS {
            let pattern = [b"<", element].concat();
            if data.len() >= pattern.len() + 1 {
                if data.starts_with(&pattern)
                    && (data[pattern.len()].is_ascii_whitespace() || data[pattern.len()] == b'>')
                {
                    return true;
                }
            }
        }
        false
    }

    /// Process a single XML chunk in parallel
    fn process_chunk(chunk: &XmlChunk) -> Result<Vec<RecordRow>> {
        let mut results = Vec::new();
        let mut reader = Reader::from_reader(chunk.data.as_slice());
        reader.config_mut().trim_text(true);

        let mut buf = Vec::new();
        loop {
            match reader.read_event_into(&mut buf) {
                Ok(Event::Start(ref e)) | Ok(Event::Empty(ref e)) => match e.name() {
                    QName(b"Record") => {
                        if let Ok(record) = Record::from_xml(e) {
                            results.push(RecordRow::Record(record));
                        }
                    }
                    QName(b"Workout") => {
                        if let Ok(workout) = Workout::from_xml(e) {
                            results.push(RecordRow::Workout(workout));
                        }
                    }
                    QName(b"ActivitySummary") => {
                        if let Ok(summary) = ActivitySummary::from_xml(e) {
                            results.push(RecordRow::ActivitySummary(summary));
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

    /// Read XML file content and return as bytes
    /// Find chunk boundaries by streaming through the file
    fn find_streaming_chunk_boundaries<R: Read + Seek>(reader: &mut R) -> Result<Vec<(u64, u64)>> {
        let mut boundaries = Vec::new();
        let mut current_pos = 0u64;
        let file_size = {
            let pos = reader.seek(SeekFrom::Current(0))?;
            let size = reader.seek(SeekFrom::End(0))?;
            reader.seek(SeekFrom::Start(pos))?;
            size
        };

        reader.seek(SeekFrom::Start(0))?;

        while current_pos < file_size {
            let target_pos = (current_pos + CHUNK_SIZE as u64).min(file_size);

            if target_pos >= file_size {
                // Last chunk
                boundaries.push((current_pos, file_size));
                break;
            }

            // Seek to target position
            reader.seek(SeekFrom::Start(target_pos))?;

            // Read buffer to find a safe boundary
            let mut buffer = vec![0u8; BOUNDARY_BUFFER_SIZE];
            let bytes_read = reader.read(&mut buffer)?;

            if bytes_read == 0 {
                boundaries.push((current_pos, file_size));
                break;
            }

            // Find safe boundary within the buffer
            let boundary_offset = Self::find_safe_boundary_in_buffer(&buffer[..bytes_read]);
            let chunk_end = target_pos + boundary_offset as u64;

            boundaries.push((current_pos, chunk_end));
            current_pos = chunk_end;
        }

        Ok(boundaries)
    }

    /// Find a safe XML element boundary within a buffer
    fn find_safe_boundary_in_buffer(buffer: &[u8]) -> usize {
        for i in 0..buffer.len() {
            if buffer[i] == b'>' {
                // Look for the start of next element
                let mut next_pos = i + 1;
                while next_pos < buffer.len() && buffer[next_pos].is_ascii_whitespace() {
                    next_pos += 1;
                }

                if next_pos < buffer.len() && buffer[next_pos] == b'<' {
                    // Check if this is one of our target elements
                    if Self::is_target_element_start(&buffer[next_pos..]) {
                        return next_pos;
                    }
                }
            }
        }
        // If no safe boundary found, return end of buffer
        buffer.len()
    }

    /// Read a specific chunk from the file
    fn read_chunk_from_file<R: Read + Seek>(
        reader: &mut R,
        start: u64,
        end: u64,
    ) -> Result<Vec<u8>> {
        let chunk_size = (end - start) as usize;
        let mut chunk_data = vec![0u8; chunk_size];

        reader.seek(SeekFrom::Start(start))?;
        reader.read_exact(&mut chunk_data)?;

        Ok(chunk_data)
    }

    /// Process file using streaming chunks (for regular XML files)
    fn process_xml_file_streaming(
        input_path: &Path,
        sender: mpsc::Sender<RecordRow>,
    ) -> Result<()> {
        let mut file = File::open(input_path)?;
        let boundaries = Self::find_streaming_chunk_boundaries(&mut file)?;

        log::info!("[extractor] Found {} streaming chunks", boundaries.len());

        // Process chunks in parallel
        let chunk_metrics_and_results: Vec<(ChunkMetrics, Vec<RecordRow>)> = boundaries
            .par_iter()
            .enumerate()
            .map(|(i, &(start, end))| {
                // Each thread needs its own file handle
                let mut thread_file = File::open(input_path).unwrap();
                let chunk_data =
                    Self::read_chunk_from_file(&mut thread_file, start, end).unwrap_or_default();

                let chunk = XmlChunk { data: chunk_data };
                let records = Self::process_chunk(&chunk).unwrap_or_default();
                let metrics = ChunkMetrics {
                    chunk_index: i,
                    chunk_size: chunk.data.len(),
                    record_count: records.len(),
                };
                (metrics, records)
            })
            .collect();

        // Log metrics
        let total_chunks = chunk_metrics_and_results.len();
        let total_records: usize = chunk_metrics_and_results
            .iter()
            .map(|(m, _)| m.record_count)
            .sum();
        let total_bytes: usize = chunk_metrics_and_results
            .iter()
            .map(|(m, _)| m.chunk_size)
            .sum();

        log::info!(
            "[extractor] Streaming chunks: {}, Total bytes: {}, Total records: {}",
            total_chunks,
            total_bytes,
            total_records
        );

        for (metrics, _) in &chunk_metrics_and_results {
            log::debug!(
                "[extractor] Chunk {}: size={} bytes, records={}",
                metrics.chunk_index,
                metrics.chunk_size,
                metrics.record_count
            );
        }

        // Send all results
        for (_, chunk_results) in chunk_metrics_and_results {
            for record_row in chunk_results {
                if sender.send(record_row).is_err() {
                    return Ok(()); // Channel closed, stop sending
                }
            }
        }

        Ok(())
    }

    /// Process chunks from memory (for ZIP files)
    fn process_memory_chunks(content: &[u8], sender: mpsc::Sender<RecordRow>) -> Result<()> {
        // Find chunk boundaries
        let boundaries = Self::find_chunk_boundaries(content);

        // Create chunks
        let chunks: Vec<XmlChunk> = boundaries
            .windows(2)
            .map(|window| XmlChunk {
                data: content[window[0]..window[1]].to_vec(),
            })
            .collect();

        // Process chunks in parallel and collect metrics
        let chunk_metrics_and_results: Vec<(ChunkMetrics, Vec<RecordRow>)> = chunks
            .par_iter()
            .enumerate()
            .map(|(i, chunk)| {
                let records = Self::process_chunk(chunk).unwrap_or_default();
                let metrics = ChunkMetrics {
                    chunk_index: i,
                    chunk_size: chunk.data.len(),
                    record_count: records.len(),
                };
                (metrics, records)
            })
            .collect();

        // Log metrics
        let total_chunks = chunk_metrics_and_results.len();
        let total_records: usize = chunk_metrics_and_results
            .iter()
            .map(|(m, _)| m.record_count)
            .sum();
        let total_bytes: usize = chunk_metrics_and_results
            .iter()
            .map(|(m, _)| m.chunk_size)
            .sum();

        log::info!(
            "[extractor] Memory chunks: {}, Total bytes: {}, Total records: {}",
            total_chunks,
            total_bytes,
            total_records
        );

        for (metrics, _) in &chunk_metrics_and_results {
            log::debug!(
                "[extractor] Chunk {}: size={} bytes, records={}",
                metrics.chunk_index,
                metrics.chunk_size,
                metrics.record_count
            );
        }

        // Send all results
        for (_, chunk_results) in chunk_metrics_and_results {
            for record_row in chunk_results {
                if sender.send(record_row).is_err() {
                    return Ok(()); // Channel closed, stop sending
                }
            }
        }

        Ok(())
    }
}
