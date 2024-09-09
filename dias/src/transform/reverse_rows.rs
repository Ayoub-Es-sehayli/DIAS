use polars::prelude::LazyFrame;

use super::transformer::Transformation;


pub struct ReverseRows {}

impl Transformation for ReverseRows {
    fn apply(self, df: LazyFrame) -> LazyFrame {
        df.reverse()
    }
}

#[cfg(test)]
mod tests {
    use polars::prelude::*;

    use super::*;
    #[test]
    fn test_rows_reverse() {
        let transformation = ReverseRows {};
        
        let df = df!(
            "col1" => &[1, 2, 3, 4, 5],
        ).unwrap().lazy();

        let result = transformation.apply(df.lazy()).collect().unwrap();
        let expected = df!(
            "col1" => &[5, 4, 3, 2, 1]
        ).unwrap();

        assert_eq!(expected, result);
    }
}
