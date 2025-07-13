use crate::xml_utils;
use quick_xml::events::BytesStart;

use crate::apple_health::types::GenericRecord;
use crate::core::Extractor;
use crate::error::Result;
use crossbeam_channel as channel;
use log::error;
use rayon::ThreadPool;
use std::{path::Path, sync::Arc, thread};

pub struct AppleHealthExtractor;

impl Extractor<GenericRecord> for AppleHealthExtractor {
    fn extract(
        &self,
        input_path: &Path,
        pool: Arc<ThreadPool>,
    ) -> Result<channel::Receiver<GenericRecord>> {
        let (sender, receiver) = channel::unbounded();
        let input_path = input_path.to_owned();

        thread::spawn(move || {
            let result: Result<()> = pool.install(|| {
                if input_path.extension().and_then(|s| s.to_str()) == Some("zip") {
                    let content = xml_utils::extract_xml_from_zip(&input_path)?;
                    xml_utils::process_memory_chunks(
                        &content,
                        &sender,
                        Arc::new(Self::parse_generic),
                    )?;
                } else {
                    xml_utils::process_xml_file_mmap(
                        &input_path,
                        &sender,
                        Arc::new(Self::parse_generic),
                    )?;
                }
                Ok(())
            });

            drop(sender);

            if let Err(e) = result {
                error!("Extractor thread failed: {}", e);
            }
        });

        Ok(receiver)
    }
}

impl AppleHealthExtractor {
    fn parse_generic(e: &BytesStart) -> Option<GenericRecord> {
        GenericRecord::from_xml(e).ok()
    }
}
