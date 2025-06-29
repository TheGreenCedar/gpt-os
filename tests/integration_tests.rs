// This file contains integration tests for the application, verifying the functionality of the tool with sample inputs and expected outputs.

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::Path;
    use std::process::Command;

    const SAMPLE_EXPORT: &str = "tests/fixtures/sample_export.xml";
    const OUTPUT_ZIP: &str = "output.zip";

    #[test]
    fn test_integration() {
        // Run the apple-health-transformer command with the sample export
        let output = Command::new("target/debug/apple-health-transformer")
            .arg(SAMPLE_EXPORT)
            .arg(OUTPUT_ZIP)
            .output()
            .expect("Failed to execute command");

        // Check if the command was successful
        assert!(output.status.success());

        // Verify that the output ZIP file was created
        assert!(Path::new(OUTPUT_ZIP).exists());

        // Clean up the generated output ZIP file
        fs::remove_file(OUTPUT_ZIP).expect("Failed to remove output ZIP file");
    }

    // Additional tests can be added here to cover more scenarios
}
