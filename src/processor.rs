use crate::types::{ActivitySummary, Record, RecordRow, Workout};
use dashmap::DashMap;
use rayon::prelude::*;
use std::sync::{Arc, Mutex};

pub struct Processor {
    global_map: Arc<DashMap<String, Mutex<Vec<RecordRow>>>>,
}

impl Processor {
    pub fn new() -> Self {
        Processor {
            global_map: Arc::new(DashMap::new()),
        }
    }

    pub fn process_records(&self, records: Vec<RecordRow>) {
        let global_map = Arc::clone(&self.global_map);

        records.into_par_iter().for_each(|record| {
            let record_type = match &record {
                RecordRow::Record(r) => r.record_type.clone(),
                RecordRow::Workout(_) => "Workout".to_string(),
                RecordRow::ActivitySummary(_) => "ActivitySummary".to_string(),
            };

            let entry = global_map
                .entry(record_type)
                .or_insert_with(|| Mutex::new(Vec::new()));
            let mut vec = entry.lock().unwrap();
            vec.push(record);
        });
    }

    pub fn get_records(&self) -> Arc<DashMap<String, Mutex<Vec<RecordRow>>>> {
        Arc::clone(&self.global_map)
    }
}
