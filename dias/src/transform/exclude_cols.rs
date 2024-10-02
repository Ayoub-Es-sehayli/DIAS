use polars::prelude::*;

use super::transformer::Transformation;

pub struct ExcludeCols {
    columns: Vec<String>
}

impl Transformation for ExcludeCols {
    fn apply(self, df: LazyFrame) -> LazyFrame {
        df.select(&[col("*").exclude(self.columns)])
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use std::string::String;

    #[test]
    fn test_can_select_cols_subset(){
        let transformation = ExcludeCols {
            columns: vec![String::from("col2"), String::from("col4")]
        };

        let df = df!(
            "col1" => &[1],
            "col2" => &[2],
            "col3" => &[3],
            "col4" => &[4],
        ).ok().unwrap().lazy();

        let result = transformation.apply(df).collect().unwrap().schema();

        assert_eq!(result.len(), 2);
    }
}
