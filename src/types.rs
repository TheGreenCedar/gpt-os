#[derive(Debug, Clone)]
pub struct Record {
    pub record_type: String,
    pub value: String,
    pub unit: Option<String>,
    pub creation_date: String,
    pub start_date: String,
    pub end_date: String,
    pub source_name: String,
    pub source_version: Option<String>,
    pub device: Option<String>,
}

impl Record {
    pub fn from_xml(element: &quick_xml::events::BytesStart) -> crate::error::Result<Self> {
        let mut record = Record {
            record_type: String::new(),
            value: String::new(),
            unit: None,
            creation_date: String::new(),
            start_date: String::new(),
            end_date: String::new(),
            source_name: String::new(),
            source_version: None,
            device: None,
        };

        for attr in element.attributes() {
            let attr = attr.map_err(|e| {
                crate::error::AppError::ParseError(format!("Failed to parse attribute: {}", e))
            })?;
            match attr.key {
                quick_xml::name::QName(b"type") => {
                    record.record_type = String::from_utf8_lossy(&attr.value).to_string();
                }
                quick_xml::name::QName(b"value") => {
                    record.value = String::from_utf8_lossy(&attr.value).to_string();
                }
                quick_xml::name::QName(b"unit") => {
                    record.unit = Some(String::from_utf8_lossy(&attr.value).to_string());
                }
                quick_xml::name::QName(b"creationDate") => {
                    record.creation_date = String::from_utf8_lossy(&attr.value).to_string();
                }
                quick_xml::name::QName(b"startDate") => {
                    record.start_date = String::from_utf8_lossy(&attr.value).to_string();
                }
                quick_xml::name::QName(b"endDate") => {
                    record.end_date = String::from_utf8_lossy(&attr.value).to_string();
                }
                quick_xml::name::QName(b"sourceName") => {
                    record.source_name = String::from_utf8_lossy(&attr.value).to_string();
                }
                quick_xml::name::QName(b"sourceVersion") => {
                    record.source_version = Some(String::from_utf8_lossy(&attr.value).to_string());
                }
                quick_xml::name::QName(b"device") => {
                    record.device = Some(String::from_utf8_lossy(&attr.value).to_string());
                }
                _ => {} // Ignore unknown attributes
            }
        }

        Ok(record)
    }
}

#[derive(Debug, Clone)]
pub struct Workout {
    pub activity_type: String,
    pub duration: f64,
    pub total_distance: Option<f64>,
    pub total_energy_burned: Option<f64>,
    pub source_name: String,
    pub device: Option<String>,
    pub start_date: String,
    pub end_date: String,
}

impl Workout {
    pub fn from_xml(element: &quick_xml::events::BytesStart) -> crate::error::Result<Self> {
        let mut workout = Workout {
            activity_type: String::new(),
            duration: 0.0,
            total_distance: None,
            total_energy_burned: None,
            source_name: String::new(),
            device: None,
            start_date: String::new(),
            end_date: String::new(),
        };

        for attr in element.attributes() {
            let attr = attr.map_err(|e| {
                crate::error::AppError::ParseError(format!("Failed to parse attribute: {}", e))
            })?;
            match attr.key {
                quick_xml::name::QName(b"workoutActivityType") => {
                    workout.activity_type = String::from_utf8_lossy(&attr.value).to_string();
                }
                quick_xml::name::QName(b"duration") => {
                    workout.duration =
                        String::from_utf8_lossy(&attr.value).parse().map_err(|e| {
                            crate::error::AppError::ParseError(format!(
                                "Failed to parse duration: {}",
                                e
                            ))
                        })?;
                }
                quick_xml::name::QName(b"totalDistance") => {
                    workout.total_distance =
                        Some(String::from_utf8_lossy(&attr.value).parse().map_err(|e| {
                            crate::error::AppError::ParseError(format!(
                                "Failed to parse totalDistance: {}",
                                e
                            ))
                        })?);
                }
                quick_xml::name::QName(b"totalEnergyBurned") => {
                    workout.total_energy_burned =
                        Some(String::from_utf8_lossy(&attr.value).parse().map_err(|e| {
                            crate::error::AppError::ParseError(format!(
                                "Failed to parse totalEnergyBurned: {}",
                                e
                            ))
                        })?);
                }
                quick_xml::name::QName(b"sourceName") => {
                    workout.source_name = String::from_utf8_lossy(&attr.value).to_string();
                }
                quick_xml::name::QName(b"device") => {
                    workout.device = Some(String::from_utf8_lossy(&attr.value).to_string());
                }
                quick_xml::name::QName(b"startDate") => {
                    workout.start_date = String::from_utf8_lossy(&attr.value).to_string();
                }
                quick_xml::name::QName(b"endDate") => {
                    workout.end_date = String::from_utf8_lossy(&attr.value).to_string();
                }
                _ => {} // Ignore unknown attributes
            }
        }

        Ok(workout)
    }
}

#[derive(Debug, Clone)]
pub struct ActivitySummary {
    pub date_components: String,
    pub active_energy_burned: Option<f64>,
    pub active_energy_burned_goal: Option<f64>,
    pub apple_exercise_time: Option<f64>,
    pub apple_stand_hours: Option<f64>,
}

impl ActivitySummary {
    pub fn from_xml(element: &quick_xml::events::BytesStart) -> crate::error::Result<Self> {
        let mut summary = ActivitySummary {
            date_components: String::new(),
            active_energy_burned: None,
            active_energy_burned_goal: None,
            apple_exercise_time: None,
            apple_stand_hours: None,
        };

        for attr in element.attributes() {
            let attr = attr.map_err(|e| {
                crate::error::AppError::ParseError(format!("Failed to parse attribute: {}", e))
            })?;
            match attr.key {
                quick_xml::name::QName(b"dateComponents") => {
                    summary.date_components = String::from_utf8_lossy(&attr.value).to_string();
                }
                quick_xml::name::QName(b"activeEnergyBurned") => {
                    summary.active_energy_burned =
                        Some(String::from_utf8_lossy(&attr.value).parse().map_err(|e| {
                            crate::error::AppError::ParseError(format!(
                                "Failed to parse activeEnergyBurned: {}",
                                e
                            ))
                        })?);
                }
                quick_xml::name::QName(b"activeEnergyBurnedGoal") => {
                    summary.active_energy_burned_goal =
                        Some(String::from_utf8_lossy(&attr.value).parse().map_err(|e| {
                            crate::error::AppError::ParseError(format!(
                                "Failed to parse activeEnergyBurnedGoal: {}",
                                e
                            ))
                        })?);
                }
                quick_xml::name::QName(b"appleExerciseTime") => {
                    summary.apple_exercise_time =
                        Some(String::from_utf8_lossy(&attr.value).parse().map_err(|e| {
                            crate::error::AppError::ParseError(format!(
                                "Failed to parse appleExerciseTime: {}",
                                e
                            ))
                        })?);
                }
                quick_xml::name::QName(b"appleStandHours") => {
                    summary.apple_stand_hours =
                        Some(String::from_utf8_lossy(&attr.value).parse().map_err(|e| {
                            crate::error::AppError::ParseError(format!(
                                "Failed to parse appleStandHours: {}",
                                e
                            ))
                        })?);
                }
                _ => {} // Ignore unknown attributes
            }
        }

        Ok(summary)
    }
}

// Generic type to represent any record row for CSV output
#[derive(Debug, Clone)]
pub enum RecordRow {
    Record(Record),
    Workout(Workout),
    ActivitySummary(ActivitySummary),
}
