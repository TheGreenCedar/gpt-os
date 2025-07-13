// This file contains integration tests for the application, verifying the functionality of the tool with sample inputs and expected outputs.

use std::collections::HashMap;
use std::fs;
use std::io::{Read, Seek, Write};
use std::path::Path;

use assert_cmd::Command;
use tempfile::NamedTempFile;
use zip::{ZipArchive, ZipWriter, write::FileOptions};

const SAMPLE_EXPORT: &str = "tests/fixtures/sample_export.xml";

#[test]
fn test_integration() {
    let output_zip = NamedTempFile::new().expect("temp file");

    Command::cargo_bin("gpt-os")
        .expect("binary")
        .arg(SAMPLE_EXPORT)
        .arg(output_zip.path())
        .assert()
        .success();

    assert!(output_zip.path().exists());
}

#[test]
fn test_zipped_input_produces_same_output() {
    let xml_output = NamedTempFile::new().expect("temp file");
    Command::cargo_bin("gpt-os")
        .expect("binary")
        .arg(SAMPLE_EXPORT)
        .arg(xml_output.path())
        .assert()
        .success();

    let xml_data = fs::read(SAMPLE_EXPORT).expect("read xml");
    let mut zip_input = tempfile::Builder::new()
        .suffix(".zip")
        .tempfile()
        .expect("zip input");
    {
        let mut writer = ZipWriter::new(&mut zip_input);
        writer
            .start_file("export.xml", FileOptions::<()>::default())
            .expect("start file");
        writer.write_all(&xml_data).expect("write");
        writer.finish().expect("finish");
        zip_input
            .as_file_mut()
            .seek(std::io::SeekFrom::Start(0))
            .unwrap();
    }

    let zip_output = NamedTempFile::new().expect("temp file");
    Command::cargo_bin("gpt-os")
        .expect("binary")
        .arg(zip_input.path())
        .arg(zip_output.path())
        .assert()
        .success();

    let xml_map = read_zip(xml_output.path());
    let zip_map = read_zip(zip_output.path());
    assert_eq!(xml_map, zip_map);
}

fn read_zip(path: &Path) -> HashMap<String, Vec<u8>> {
    let file = fs::File::open(path).expect("open zip");
    let mut archive = ZipArchive::new(file).expect("open archive");
    let mut map = HashMap::new();
    for i in 0..archive.len() {
        let mut f = archive.by_index(i).expect("entry");
        let mut data = Vec::new();
        f.read_to_end(&mut data).expect("read");
        map.insert(f.name().to_string(), data);
    }
    map
}
// Additional tests can be added here to cover more scenarios
