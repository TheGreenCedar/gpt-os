use crate::types::RecordRow;
use dashmap::DashMap;
use std::sync::{Arc, Mutex, mpsc};
use std::thread;

/// Parallel processor for grouping health records by type
pub struct Processor {
    global_map: Arc<DashMap<String, Mutex<Vec<RecordRow>>>>,
}

impl Processor {
    /// Create a new processor instance
    pub fn new() -> Self {
        Processor {
            global_map: Arc::new(DashMap::new()),
        }
    }

    /// Process records from a channel with worker threads
    pub fn process_records_from_channel(
        &self,
        receiver: mpsc::Receiver<RecordRow>,
        num_workers: usize,
    ) -> Vec<thread::JoinHandle<()>> {
        let global_map = Arc::clone(&self.global_map);
        let receiver = Arc::new(Mutex::new(receiver));

        let mut handles = Vec::new();

        for worker_id in 0..num_workers {
            let receiver = Arc::clone(&receiver);
            let global_map = Arc::clone(&global_map);

            let handle = thread::spawn(move || {
                log::debug!("Worker {} started", worker_id);
                let mut records_processed = 0;

                loop {
                    let record = {
                        let receiver = receiver.lock().unwrap();
                        receiver.recv()
                    };

                    match record {
                        Ok(record) => {
                            let record_type = record.record_type();
                            let entry = global_map
                                .entry(record_type)
                                .or_insert_with(|| Mutex::new(Vec::new()));
                            let mut vec = entry.lock().unwrap();
                            vec.push(record);
                            records_processed += 1;
                        }
                        Err(_) => {
                            // Channel closed, no more records
                            break;
                        }
                    }
                }

                log::debug!(
                    "Worker {} finished, processed {} records",
                    worker_id,
                    records_processed
                );
            });

            handles.push(handle);
        }

        handles
    }

    /// Get the grouped records
    pub fn get_records(&self) -> Arc<DashMap<String, Mutex<Vec<RecordRow>>>> {
        Arc::clone(&self.global_map)
    }

    /// Get the total number of records processed
    pub fn get_total_record_count(&self) -> usize {
        self.global_map
            .iter()
            .map(|entry| entry.value().lock().unwrap().len())
            .sum()
    }
}

impl Default for Processor {
    fn default() -> Self {
        Self::new()
    }
}
