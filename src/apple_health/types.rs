use crate::core::Processable;
use crate::error::{AppError, Result};
use erased_serde::Serialize as ErasedSerialize;
use quick_xml::events::BytesStart;
use quick_xml::name::QName;
use serde::{Serialize, Serializer};

// Record attributes
const ATTR_TYPE: &[u8] = b"type";
const ATTR_VALUE: &[u8] = b"value";
const ATTR_UNIT: &[u8] = b"unit";
const ATTR_CREATION_DATE: &[u8] = b"creationDate";
const ATTR_START_DATE: &[u8] = b"startDate";
const ATTR_END_DATE: &[u8] = b"endDate";
const ATTR_SOURCE_NAME: &[u8] = b"sourceName";
const ATTR_SOURCE_VERSION: &[u8] = b"sourceVersion";
const ATTR_DEVICE: &[u8] = b"device";

// Workout attributes
const ATTR_WORKOUT_ACTIVITY_TYPE: &[u8] = b"workoutActivityType";
const ATTR_DURATION: &[u8] = b"duration";
const ATTR_TOTAL_DISTANCE: &[u8] = b"totalDistance";
const ATTR_TOTAL_ENERGY_BURNED: &[u8] = b"totalEnergyBurned";

// Activity summary attributes
const ATTR_DATE_COMPONENTS: &[u8] = b"dateComponents";
const ATTR_ACTIVE_ENERGY_BURNED: &[u8] = b"activeEnergyBurned";
const ATTR_ACTIVE_ENERGY_BURNED_GOAL: &[u8] = b"activeEnergyBurnedGoal";
const ATTR_APPLE_EXERCISE_TIME: &[u8] = b"appleExerciseTime";
const ATTR_APPLE_STAND_HOURS: &[u8] = b"appleStandHours";

// Helper function to extract string from attribute value
fn extract_string_value(value: &[u8]) -> String {
    String::from_utf8_lossy(value).to_string()
}

// Helper function to parse numeric values
fn parse_numeric_value<T: std::str::FromStr>(value: &[u8], field_name: &str) -> Result<T>
where
    T::Err: std::fmt::Display,
{
    extract_string_value(value)
        .parse()
        .map_err(|e| AppError::ParseError(format!("Failed to parse {}: {}", field_name, e)))
}

// Helper function to serialize Option<String> as empty string when None
fn serialize_option_as_string<S>(
    value: &Option<String>,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match value {
        Some(s) => serializer.serialize_str(s),
        None => serializer.serialize_str(""),
    }
}

// Helper function to serialize Option<f64> as empty string when None
fn serialize_option_f64_as_string<S>(
    value: &Option<f64>,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match value {
        Some(f) => serializer.serialize_str(&f.to_string()),
        None => serializer.serialize_str(""),
    }
}

/// Generic representation for any Apple Health XML element.
#[derive(Debug, Clone)]
pub struct GenericRecord {
    pub element_name: String,
    pub attributes: std::collections::BTreeMap<String, String>,
}

impl GenericRecord {
    pub fn from_xml(element: &BytesStart) -> Result<Self> {
        let element_name = String::from_utf8_lossy(element.name().as_ref()).to_string();
        let mut attributes = std::collections::BTreeMap::new();
        for attr in element.attributes() {
            let attr = attr
                .map_err(|e| AppError::ParseError(format!("Failed to parse attribute: {}", e)))?;
            let key = String::from_utf8_lossy(attr.key.as_ref()).to_string();
            let value = extract_string_value(&attr.value);
            attributes.insert(key, value);
        }
        Ok(GenericRecord {
            element_name,
            attributes,
        })
    }
}

impl Serialize for GenericRecord {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("GenericRecord", 2)?;
        state.serialize_field("element", &self.element_name)?;
        let attrs = serde_json::to_string(&self.attributes).map_err(serde::ser::Error::custom)?;
        state.serialize_field("attributes", &attrs)?;
        state.end()
    }
}

impl Processable for GenericRecord {
    fn grouping_key(&self) -> String {
        if self.element_name == "Record" {
            if let Some(typ) = self.attributes.get("type") {
                return typ.clone();
            }
        }
        self.element_name.clone()
    }

    fn as_serializable(&self) -> &dyn ErasedSerialize {
        self
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Record {
    #[serde(rename = "type")]
    pub record_type: String,
    pub value: String,
    #[serde(serialize_with = "serialize_option_as_string")]
    pub unit: Option<String>,
    #[serde(rename = "creationDate")]
    pub creation_date: String,
    #[serde(rename = "startDate")]
    pub start_date: String,
    #[serde(rename = "endDate")]
    pub end_date: String,
    #[serde(rename = "sourceName")]
    pub source_name: String,
    #[serde(
        rename = "sourceVersion",
        serialize_with = "serialize_option_as_string"
    )]
    pub source_version: Option<String>,
    #[serde(serialize_with = "serialize_option_as_string")]
    pub device: Option<String>,
}

impl Record {
    pub fn from_xml(element: &BytesStart) -> Result<Self> {
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
            let attr = attr
                .map_err(|e| AppError::ParseError(format!("Failed to parse attribute: {}", e)))?;
            match attr.key {
                QName(ATTR_TYPE) => {
                    record.record_type = extract_string_value(&attr.value);
                }
                QName(ATTR_VALUE) => {
                    record.value = extract_string_value(&attr.value);
                }
                QName(ATTR_UNIT) => {
                    record.unit = Some(extract_string_value(&attr.value));
                }
                QName(ATTR_CREATION_DATE) => {
                    record.creation_date = extract_string_value(&attr.value);
                }
                QName(ATTR_START_DATE) => {
                    record.start_date = extract_string_value(&attr.value);
                }
                QName(ATTR_END_DATE) => {
                    record.end_date = extract_string_value(&attr.value);
                }
                QName(ATTR_SOURCE_NAME) => {
                    record.source_name = extract_string_value(&attr.value);
                }
                QName(ATTR_SOURCE_VERSION) => {
                    record.source_version = Some(extract_string_value(&attr.value));
                }
                QName(ATTR_DEVICE) => {
                    record.device = Some(extract_string_value(&attr.value));
                }
                _ => {} // Ignore unknown attributes
            }
        }

        Ok(record)
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Workout {
    #[serde(rename = "workoutActivityType")]
    pub activity_type: String,
    pub duration: f64,
    #[serde(
        rename = "totalDistance",
        serialize_with = "serialize_option_f64_as_string"
    )]
    pub total_distance: Option<f64>,
    #[serde(
        rename = "totalEnergyBurned",
        serialize_with = "serialize_option_f64_as_string"
    )]
    pub total_energy_burned: Option<f64>,
    #[serde(rename = "sourceName")]
    pub source_name: String,
    #[serde(serialize_with = "serialize_option_as_string")]
    pub device: Option<String>,
    #[serde(rename = "startDate")]
    pub start_date: String,
    #[serde(rename = "endDate")]
    pub end_date: String,
}

impl Workout {
    pub fn from_xml(element: &BytesStart) -> Result<Self> {
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
            let attr = attr
                .map_err(|e| AppError::ParseError(format!("Failed to parse attribute: {}", e)))?;
            match attr.key {
                QName(ATTR_WORKOUT_ACTIVITY_TYPE) => {
                    workout.activity_type = extract_string_value(&attr.value);
                }
                QName(ATTR_DURATION) => {
                    workout.duration = parse_numeric_value(&attr.value, "duration")?;
                }
                QName(ATTR_TOTAL_DISTANCE) => {
                    workout.total_distance =
                        Some(parse_numeric_value(&attr.value, "totalDistance")?);
                }
                QName(ATTR_TOTAL_ENERGY_BURNED) => {
                    workout.total_energy_burned =
                        Some(parse_numeric_value(&attr.value, "totalEnergyBurned")?);
                }
                QName(ATTR_SOURCE_NAME) => {
                    workout.source_name = extract_string_value(&attr.value);
                }
                QName(ATTR_DEVICE) => {
                    workout.device = Some(extract_string_value(&attr.value));
                }
                QName(ATTR_START_DATE) => {
                    workout.start_date = extract_string_value(&attr.value);
                }
                QName(ATTR_END_DATE) => {
                    workout.end_date = extract_string_value(&attr.value);
                }
                _ => {} // Ignore unknown attributes
            }
        }

        Ok(workout)
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ActivitySummary {
    #[serde(rename = "dateComponents")]
    pub date_components: String,
    #[serde(
        rename = "activeEnergyBurned",
        serialize_with = "serialize_option_f64_as_string"
    )]
    pub active_energy_burned: Option<f64>,
    #[serde(
        rename = "activeEnergyBurnedGoal",
        serialize_with = "serialize_option_f64_as_string"
    )]
    pub active_energy_burned_goal: Option<f64>,
    #[serde(
        rename = "appleExerciseTime",
        serialize_with = "serialize_option_f64_as_string"
    )]
    pub apple_exercise_time: Option<f64>,
    #[serde(
        rename = "appleStandHours",
        serialize_with = "serialize_option_f64_as_string"
    )]
    pub apple_stand_hours: Option<f64>,
}

impl ActivitySummary {
    pub fn from_xml(element: &BytesStart) -> Result<Self> {
        let mut summary = ActivitySummary {
            date_components: String::new(),
            active_energy_burned: None,
            active_energy_burned_goal: None,
            apple_exercise_time: None,
            apple_stand_hours: None,
        };

        for attr in element.attributes() {
            let attr = attr
                .map_err(|e| AppError::ParseError(format!("Failed to parse attribute: {}", e)))?;
            match attr.key {
                QName(ATTR_DATE_COMPONENTS) => {
                    summary.date_components = extract_string_value(&attr.value);
                }
                QName(ATTR_ACTIVE_ENERGY_BURNED) => {
                    summary.active_energy_burned =
                        Some(parse_numeric_value(&attr.value, "activeEnergyBurned")?);
                }
                QName(ATTR_ACTIVE_ENERGY_BURNED_GOAL) => {
                    summary.active_energy_burned_goal =
                        Some(parse_numeric_value(&attr.value, "activeEnergyBurnedGoal")?);
                }
                QName(ATTR_APPLE_EXERCISE_TIME) => {
                    summary.apple_exercise_time =
                        Some(parse_numeric_value(&attr.value, "appleExerciseTime")?);
                }
                QName(ATTR_APPLE_STAND_HOURS) => {
                    summary.apple_stand_hours =
                        Some(parse_numeric_value(&attr.value, "appleStandHours")?);
                }
                _ => {} // Ignore unknown attributes
            }
        }

        Ok(summary)
    }
}
