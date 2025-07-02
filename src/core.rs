use crate::error::Result;
use crossbeam_channel as channel;
use dashmap::DashMap;
use erased_serde::Serialize;
use log::{debug, info};
use std::fmt::Debug;
use std::path::Path;
use std::time::Instant;

/// Represents a single, processable data record.
pub trait Processable: Send + Sync + Debug + 'static {
    /// A key for grouping records, e.g., by data type.
    fn grouping_key(&self) -> String;

    /// Returns a reference to a serializable version of the record.
    fn as_serializable(&self) -> &dyn Serialize;

    /// Optional key used for ordering records within groups.
    fn sort_key(&self) -> Option<String> {
        None
    }
}

/// Extracts records from a data source into a channel.
pub trait Extractor<T: Processable> {
    fn extract(&self, input_path: &Path) -> Result<channel::Receiver<T>>;
}

/// Loads grouped records into a data sink.
pub trait Sink<T: Processable> {
    fn load(&self, grouped_records: DashMap<String, Vec<T>>, output_path: &Path) -> Result<()>;
}

pub struct Engine<T, E, S>
where
    T: Processable,
    E: Extractor<T>,
    S: Sink<T>,
{
    extractor: E,
    sink: S,
    num_workers: usize,
    _marker: std::marker::PhantomData<T>,
}

impl<T, E, S> Engine<T, E, S>
where
    T: Processable,
    E: Extractor<T>,
    S: Sink<T>,
{
    pub fn new(extractor: E, sink: S, num_workers: usize) -> Self {
        Self {
            extractor,
            sink,
            num_workers,
            _marker: std::marker::PhantomData,
        }
    }

    pub fn run(&self, input_path: &Path, output_path: &Path) -> Result<()> {
        let start_time = Instant::now();
        info!("Starting ETL pipeline");
        info!("Input: {}", input_path.display());
        info!("Output: {}", output_path.display());
        info!("Workers: {}", self.num_workers);

        // Extract phase
        let extract_start = Instant::now();
        info!("Starting extraction phase...");
        let receiver = self.extractor.extract(input_path)?;
        let extract_duration = extract_start.elapsed();
        debug!(
            "Extraction phase setup completed in {:.3}s",
            extract_duration.as_secs_f64()
        );

        // Transform phase
        let transform_start = Instant::now();
        info!(
            "Starting transformation phase with {} workers...",
            self.num_workers
        );
        let grouped_records = transformer::transform(receiver, self.num_workers);
        let transform_duration = transform_start.elapsed();

        let total_records: usize = grouped_records
            .iter()
            .map(|entry| entry.value().len())
            .sum();
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
        self.sink.load(grouped_records, output_path)?;
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
    use crossbeam_channel::Receiver;
    use dashmap::DashMap;
    use log::{debug, info};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use std::thread;
    use std::time::Instant;

    pub fn transform<T: Processable>(
        receiver: Receiver<T>,
        num_workers: usize,
    ) -> DashMap<String, Vec<T>> {
        let start_time = Instant::now();
        let grouped_records: Arc<DashMap<String, Vec<T>>> = Arc::new(DashMap::new());
        let records_processed = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::with_capacity(num_workers);

        debug!("Spawning {} worker threads for transformation", num_workers);

        for i in 0..num_workers {
            let rx = receiver.clone();
            let grouped_records = Arc::clone(&grouped_records);
            let records_processed = Arc::clone(&records_processed);
            handles.push(thread::spawn(move || {
                let mut local_count = 0;
                for record in rx.iter() {
                    grouped_records
                        .entry(record.grouping_key())
                        .or_default()
                        .push(record);
                    local_count += 1;

                    if local_count % 1000 == 0 {
                        debug!("Worker {} processed {} records", i, local_count);
                    }
                }
                records_processed.fetch_add(local_count, Ordering::Relaxed);
                debug!("Worker {} finished processing {} records", i, local_count);
            }));
        }

        for (i, handle) in handles.into_iter().enumerate() {
            match handle.join() {
                Ok(()) => debug!("Worker {} completed successfully", i),
                Err(_) => panic!("Worker {} panicked", i),
            }
        }

        let total_processed = records_processed.load(Ordering::Relaxed);
        let duration = start_time.elapsed();
        info!(
            "Transformation workers completed: {} records processed in {:.3}s",
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

        Arc::try_unwrap(grouped_records).unwrap()
    }
}
