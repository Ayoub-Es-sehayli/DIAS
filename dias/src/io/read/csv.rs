use polars::prelude::*;

use super::reader::Reader;

impl Reader for CsvReadOptions {
    fn extract(self) -> PolarsResult<DataFrame> {
        self.try_into_reader_with_file_path(None)?.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::fs::File;
    use std::io::prelude::*;
    use tempfile::TempDir;

    struct Fixture {
        path: PathBuf,
        _tempdir: TempDir,
    }
    impl Fixture {
        fn blank(fixture_filename: &str) -> Self {
            let tempdir = tempfile::tempdir().unwrap();
            let mut path = PathBuf::from(&tempdir.path());
            path.push(&fixture_filename);

            Fixture { _tempdir: tempdir, path }
        }
        fn setup(fixture_filename: &str) -> Self {
            let fixture = Fixture::blank(fixture_filename);
            let mut csv = File::create(fixture.path.clone()).expect("Temp file creation failed");
            csv.write_all(b"Column1,Column2\n").expect("Failed to write to temp file");
            csv.write_all(b"Value1,Value2\n").expect("Failed to write to temp file");

            fixture
        }
    }
    
    #[test]
    fn test_with_default_params() {
        let csv = Fixture::setup("data.csv");
        
        let reader = CsvReadOptions {
            path: Some(csv.path),
            ..CsvReadOptions::default()
        };

        let result = reader.extract().expect("Read Error");

        let col_names = ["Column1", "Column2"];
        
        assert_eq!(result.get_column_names(), col_names);
        assert!(!result.is_empty());
    }
}
