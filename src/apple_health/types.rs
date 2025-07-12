use crate::core::Processable;
use crate::error::{AppError, Result};
use erased_serde::Serialize as ErasedSerialize;
use quick_xml::events::BytesStart;
use serde::{Serialize, Serializer};

// Helper function to extract string from attribute value
fn extract_string_value(value: &[u8]) -> String {
    String::from_utf8_lossy(value).to_string()
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
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(self.attributes.len()))?;
        for (k, v) in &self.attributes {
            map.serialize_entry(k, v)?;
        }
        map.end()
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

    fn sort_key(&self) -> Option<String> {
        let keys = [
            "startDate",
            "date",
            "dateComponents",
            "creationDate",
            "endDate",
            "dateIssued",
            "receivedDate",
        ];
        for k in keys {
            if let Some(v) = self.attributes.get(k) {
                return Some(v.clone());
            }
        }
        None
    }
}
