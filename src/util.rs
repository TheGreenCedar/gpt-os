use std::time::{SystemTime, UNIX_EPOCH};

/// Generate a random ID for temporary directories
pub fn generate_random_id() -> String {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    format!("{}", timestamp)
}

/// Sanitize a filename by replacing invalid characters
pub fn sanitize_filename(input: &str) -> String {
    // Replace common Apple Health type prefixes for cleaner names
    let clean_name = input
        .replace("HKQuantityTypeIdentifier", "")
        .replace("HKCategoryTypeIdentifier", "")
        .replace("HKCharacteristicTypeIdentifier", "")
        .replace("HKWorkoutActivityType", "");

    // Remove or replace invalid filename characters
    clean_name
        .chars()
        .map(|c| match c {
            '<' | '>' | ':' | '"' | '/' | '\\' | '|' | '?' | '*' => '_',
            c if c.is_control() => '_',
            c => c,
        })
        .collect::<String>()
        .trim_matches('_')
        .to_string()
}
