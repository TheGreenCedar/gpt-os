use crate::error::Result;
use async_trait::async_trait;
use log::{debug, info};
use std::collections::HashMap;
use std::fmt::Debug;
use std::path::Path;
use std::time::Instant;
use tokio::sync::mpsc;

/// Represents a single, processable data record.
pub trait Processable: Send + Sync + Debug + 'static {
    /// A key for grouping records, e.g., by data type.
    fn grouping_key(&self) -> String;

    /// Optional key used for ordering records within groups.
    fn sort_key(&self) -> Option<String> {
        None
    }
}

/// Extracts records from a data source into a channel.
#[async_trait]
pub trait Extractor<T: Processable> {
    async fn extract(&self, input_path: &Path) -> Result<mpsc::Receiver<T>>;
}

/// Loads grouped records into a data sink.
#[async_trait]
pub trait Sink<T: Processable> {
    async fn load(
        &self,
        grouped_records: HashMap<String, Vec<T>>,
        output_path: &Path,
    ) -> Result<()>;
}

pub struct Engine<T, E, S>
where
    T: Processable,
    E: Extractor<T> + Sync,
    S: Sink<T> + Sync,
{
    extractor: E,
    sink: S,
    _marker: std::marker::PhantomData<T>,
}

impl<T, E, S> Engine<T, E, S>
where
    T: Processable,
    E: Extractor<T> + Sync,
    S: Sink<T> + Sync,
{
    pub fn new(extractor: E, sink: S) -> Self {
        Self {
            extractor,
            sink,
            _marker: std::marker::PhantomData,
        }
    }

    pub async fn run(&self, input_path: &Path, output_path: &Path) -> Result<()> {
        let start_time = Instant::now();
        info!("Starting ETL pipeline");
        info!("Input: {}", input_path.display());
        info!("Output: {}", output_path.display());

        // Extract phase
        let extract_start = Instant::now();
        info!("Starting extraction phase...");
        let receiver = self.extractor.extract(input_path).await?;
        let extract_duration = extract_start.elapsed();
        debug!(
            "Extraction phase setup completed in {:.3}s",
            extract_duration.as_secs_f64()
        );

        // Transform phase
        let transform_start = Instant::now();
        info!("Starting transformation phase...");
        let grouped_records = transformer::transform(receiver).await;
        let transform_duration = transform_start.elapsed();

        let total_records: usize = grouped_records.iter().map(|(_, v)| v.len()).sum();
        let record_types = grouped_records.len();
        info!(
            "Transformation completed in {:.3}s: {} records grouped into {} types",
            transform_duration.as_secs_f64(),
            total_records,
            record_types
        );

        // Load phase
        let load_start = Instant::now();
        info!("Starting load phase...");
        self.sink.load(grouped_records, output_path).await?;
        let load_duration = load_start.elapsed();
        info!(
            "Load phase completed in {:.3}s",
            load_duration.as_secs_f64()
        );

        let total_duration = start_time.elapsed();
        info!(
            "ETL pipeline completed successfully in {:.3}s",
            total_duration.as_secs_f64()
        );
        info!(
            "Performance breakdown - Extract: {:.3}s, Transform: {:.3}s, Load: {:.3}s",
            extract_duration.as_secs_f64(),
            transform_duration.as_secs_f64(),
            load_duration.as_secs_f64()
        );

        if total_records > 0 {
            let throughput = total_records as f64 / total_duration.as_secs_f64();
            info!("Throughput: {:.0} records/second", throughput);
        }

        Ok(())
    }
}

mod transformer {
    use super::Processable;
    use log::{debug, info};
    use std::collections::HashMap;
    use std::time::Instant;
    use tokio::sync::mpsc::Receiver;

    pub async fn transform<T: Processable>(mut receiver: Receiver<T>) -> HashMap<String, Vec<T>> {
        let start_time = Instant::now();
        let mut grouped_records: HashMap<String, Vec<T>> = HashMap::new();
        let mut total_processed = 0usize;

        while let Some(record) = receiver.recv().await {
            grouped_records
                .entry(record.grouping_key())
                .or_default()
                .push(record);
            total_processed += 1;
        }

        let duration = start_time.elapsed();
        info!(
            "Transformation completed: {} records processed in {:.3}s",
            total_processed,
            duration.as_secs_f64()
        );

        if total_processed > 0 {
            let records_per_sec = total_processed as f64 / duration.as_secs_f64();
            debug!(
                "Transformation throughput: {:.0} records/second",
                records_per_sec
            );
        }

        grouped_records
    }
}
