use crate::core::Processable;
use crate::error::{AppError, Result};
use crate::sinks::csv_zip::CsvWritable;
use quick_xml::events::BytesStart;

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

impl CsvWritable for GenericRecord {
    fn headers(&self) -> Vec<String> {
        self.attributes.keys().cloned().collect()
    }

    fn write<W: std::io::Write>(
        &self,
        writer: &mut csv::Writer<W>,
        headers: &[String],
    ) -> csv::Result<()> {
        let record: Vec<&str> = headers
            .iter()
            .map(|h| self.attributes.get(h).map(String::as_str).unwrap_or(""))
            .collect();
        writer.write_record(&record)
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

    fn sort_key(&self) -> Option<&str> {
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
                return Some(v.as_str());
            }
        }
        None
    }
}
