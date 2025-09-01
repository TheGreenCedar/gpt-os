use crate::xml_utils::{self, BUFFER_SIZE};
use quick_xml::events::BytesStart;

use crate::apple_health::types::GenericRecord;
use crate::core::Extractor;
use crate::error::{AppError, Result};
use async_trait::async_trait;
use crossbeam_channel as channel;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::mpsc;

pub struct AppleHealthExtractor;

#[async_trait]
impl Extractor<GenericRecord> for AppleHealthExtractor {
    async fn extract(&self, input_path: &Path) -> Result<mpsc::Receiver<Result<GenericRecord>>> {
        let (tx, rx) = mpsc::channel(BUFFER_SIZE);
        let (cb_tx, cb_rx) = channel::bounded(BUFFER_SIZE);
        let path = Arc::new(input_path.to_path_buf());

        let handle = if path.extension().and_then(|s| s.to_str()) == Some("zip") {
            tokio::spawn(xml_utils::process_zip_stream_parallel(
                path.clone(),
                cb_tx.clone(),
                Self::parse_generic,
            ))
        } else {
            let file = File::open(path.as_ref())?;
            tokio::spawn(xml_utils::process_stream_parallel(
                file,
                cb_tx,
                Self::parse_generic,
            ))
        };

        let error_tx = tx.clone();
        tokio::spawn(async move {
            match handle.await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    let _ = error_tx.send(Err(e)).await;
                }
                Err(e) => {
                    let _ = error_tx.send(Err(AppError::Unknown(e.to_string()))).await;
                }
            }
        });

        tokio::spawn(async move {
            for record in cb_rx {
                if tx.send(Ok(record)).await.is_err() {
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
