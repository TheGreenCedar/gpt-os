use crate::core::Processable;
use crate::error::{AppError, Result};
use crate::sinks::csv_zip::CsvWritable;
use ahash::AHashMap;
use quick_xml::events::BytesStart;

/// Generic representation for any Apple Health XML element.
#[derive(Debug, Clone)]
pub struct GenericRecord {
    pub element_name: String,
    pub attributes: AHashMap<String, String>,
}

impl GenericRecord {
    pub fn from_xml(element: &BytesStart) -> Result<Self> {
        let element_name = String::from_utf8(element.name().as_ref().to_vec())
            .map_err(|e| AppError::ParseError(format!("Invalid element name: {}", e)))?;

        let attributes_iter = element.attributes();
        let (lower, _) = attributes_iter.size_hint();
        let mut attributes = AHashMap::with_capacity(lower);

        for attr in attributes_iter {
            let attr = attr
                .map_err(|e| AppError::ParseError(format!("Failed to parse attribute: {}", e)))?;

            let key = String::from_utf8(attr.key.as_ref().to_vec())
                .map_err(|e| AppError::ParseError(format!("Invalid attribute key: {}", e)))?;

            let value = String::from_utf8(attr.value.into_owned())
                .map_err(|e| AppError::ParseError(format!("Invalid attribute value: {}", e)))?;

            attributes.insert(key, value);
        }

        Ok(GenericRecord {
            element_name,
            attributes,
        })
    }
}

impl CsvWritable for GenericRecord {
    fn header_keys(&self) -> impl Iterator<Item = &str> {
        self.attributes.keys().map(String::as_str)
    }

    fn write<W: std::io::Write>(
        &self,
        writer: &mut csv::Writer<W>,
        headers: &[&str],
    ) -> csv::Result<()> {
        let record: Vec<&str> = headers
            .iter()
            .map(|h| self.attributes.get(*h).map(String::as_str).unwrap_or(""))
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
