use crate::xml_utils;
use quick_xml::events::BytesStart;

use crate::apple_health::types::GenericRecord;
use crate::core::Extractor;
use crate::error::Result;
use async_trait::async_trait;
use crossbeam_channel as channel;
use log::error;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task;

pub struct AppleHealthExtractor;

#[async_trait]
impl Extractor<GenericRecord> for AppleHealthExtractor {
    async fn extract(&self, input_path: &Path) -> Result<mpsc::Receiver<GenericRecord>> {
        let (tx, rx) = mpsc::channel(100);
        let (cb_tx, cb_rx) = channel::unbounded();
        let path = input_path.to_owned();

        task::spawn_blocking(move || {
            let result: Result<()> = (|| {
                if path.extension().and_then(|s| s.to_str()) == Some("zip") {
                    let content = xml_utils::extract_xml_from_zip(&path)?;
                    xml_utils::process_memory_chunks(
                        &content,
                        &cb_tx,
                        Arc::new(Self::parse_generic),
                    )?;
                } else {
                    xml_utils::process_xml_file_mmap(&path, &cb_tx, Arc::new(Self::parse_generic))?;
                }
                Ok(())
            })();
            drop(cb_tx);
            if let Err(e) = result {
                error!("Extractor thread failed: {}", e);
            }
        });

        tokio::spawn(async move {
            for record in cb_rx {
                if tx.send(record).await.is_err() {
                    break;
                }
            }
        });

        Ok(rx)
    }
}

impl AppleHealthExtractor {
    fn parse_generic(e: &BytesStart) -> Option<GenericRecord> {
        GenericRecord::from_xml(e).ok()
    }
}
