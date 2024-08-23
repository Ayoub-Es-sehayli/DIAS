use polars::prelude::{len, LazyFrame};

use super::transformer::Transformation;

pub struct CountRows {}

impl Transformation for CountRows {
    fn apply(self, df: LazyFrame) -> LazyFrame {
        df.select([len().alias("Row Count")])
    }
}

#[cfg(test)]
mod tests {
    use polars::prelude::*;

    use super::*;
    #[test]
    fn test_rows_properly_counted() {
        let transformation = CountRows{};

        let df = df!(
            "col1" => &[1, 2, 3, 4, 5],
        ).ok().unwrap().lazy();

        let result = transformation.apply(df.lazy()).collect().unwrap();

        
        assert_eq!(result.column("Row Count").unwrap().sum::<i32>().unwrap(), 5);
    }
}
