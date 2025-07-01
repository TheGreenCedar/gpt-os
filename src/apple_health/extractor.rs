use crate::xml_utils;
use quick_xml::events::BytesStart;

use crate::apple_health::types::GenericRecord;
use crate::core::Extractor;
use crate::error::Result;
use crossbeam_channel as channel;
use std::{path::Path, sync::Arc, thread};

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
    fn parse_generic(e: &BytesStart) -> Option<GenericRecord> {
        GenericRecord::from_xml(e).ok()
    }

    fn process_xml_file_mmap(
        input_path: &Path,
        sender: channel::Sender<GenericRecord>,
    ) -> Result<()> {
        xml_utils::process_xml_file_mmap(input_path, sender, Arc::new(Self::parse_generic))
    }

    fn process_memory_chunks(content: &[u8], sender: channel::Sender<GenericRecord>) -> Result<()> {
        xml_utils::process_memory_chunks(content, sender, Arc::new(Self::parse_generic))
    }

    fn extract_xml_from_zip(input_path: &Path) -> Result<Vec<u8>> {
        xml_utils::extract_xml_from_zip(input_path)
    }
}
