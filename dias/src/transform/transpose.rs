use polars::prelude::*;

use super::transformer::Transformation;

// Note: This a very expensive operation
struct Transpose {}

impl Transformation for Transpose {
    fn apply(self, df: LazyFrame) -> LazyFrame {
        df.collect().unwrap().transpose(None, None).unwrap().lazy()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_can_transpose() {
        let transformation = Transpose {};
        let df = df!(
            "a" => &["a1", "a2", "a3", "a4"],
            "b" => &["b1", "b2", "b3", "b4"]
        ).unwrap();

        let result = transformation.apply(df.lazy()).collect().unwrap();
        let expected = df!(
            "collumn_0" => &["a1", "b1"],
            "collumn_1" => &["a2", "b2"],
            "collumn_2" => &["a3", "b3"],
            "collumn_3" => &["a4", "b4"],
        ).unwrap();

        assert_eq!(expected, result);
    }
}
