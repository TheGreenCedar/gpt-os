use crate::core::{Processable, Sink};
use crate::error::Result;
use dashmap::DashMap;
use log::{debug, info, warn};
use rayon::prelude::*;
use std::fs::File;
use std::io::{Cursor, Write};
use std::path::Path;
use std::time::Instant;
use zip::ZipArchive;
use zip::{CompressionMethod, ZipWriter, write::FileOptions};

pub struct CsvZipSink;

impl<T> Sink<T> for CsvZipSink
where
    T: Processable + Send + Sync + 'static,
{
    fn load(&self, grouped_records: DashMap<String, Vec<T>>, output_path: &Path) -> Result<()> {
        let start = Instant::now();

        // 1. Drain and filter empty groups into a Vec
        let mut entries: Vec<(String, Vec<T>)> = grouped_records
            .into_iter()
            .filter_map(|(k, v)| {
                if v.is_empty() {
                    warn!("Skipping empty group '{}'", k);
                    None
                } else {
                    Some((k, v))
                }
            })
            .collect();
        entries.sort_by(|a, b| a.0.cmp(&b.0));

        let total_files = entries.len();
        let total_recs: usize = entries.iter().map(|(_, v)| v.len()).sum();
        info!(
            "Exporting {} CSVs, {} total records",
            total_files, total_recs
        );

        // 2. Parallel CSV serialization into byte buffers
        let mini_zips: Vec<_> = entries
            .into_par_iter()
            .map(|(name, mut recs)| -> Result<_> {
                recs.sort_by_key(|r| r.sort_key().unwrap_or_default());
                // a) build CSV in memory
                let mut buf = Vec::with_capacity(recs.len() * 100);
                {
                    let mut w = csv::WriterBuilder::new()
                        .has_headers(true)
                        .from_writer(&mut buf);
                    for r in &recs {
                        w.serialize(r.as_serializable())?;
                    }
                    w.flush()?;
                }
                debug!("CSV for '{}' is {} bytes", name, buf.len());

                // b) wrap in a 1-entry ZIP
                let mut cursor = Cursor::new(Vec::with_capacity(buf.len() / 3));
                {
                    let mut mini = ZipWriter::new(&mut cursor);
                    let opts = FileOptions::<()>::default()
                        .compression_method(CompressionMethod::Deflated)
                        .unix_permissions(0o644);
                    mini.start_file(format!("{}.csv", &name), opts)?;
                    mini.write_all(&buf)?;
                    mini.finish()?;
                }
                cursor.set_position(0);
                Ok((name, cursor))
            })
            .collect::<Result<_>>()?;

        // 3. Merge them into the final ZIP
        let mut out = File::create(output_path)?;
        let mut zip = ZipWriter::new(&mut out);
        for (name, mut mini) in mini_zips {
            let src = ZipArchive::new(&mut mini)?;
            zip.merge_archive(src)?;
            debug!("Merged '{}.csv' from mini-zip", name);
        }
        zip.finish()?;
        log::info!("Done in {:.2}s", start.elapsed().as_secs_f64());
        Ok(())
    }
}
