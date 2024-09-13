use polars::prelude::*;

use super::transformer::Transformation;

struct CastCols<'a> {
    dtypes: PlHashMap<&'a str, DataType>,
    strict: bool
}

impl<'a> Transformation for CastCols<'a> {
    fn apply(self, df: LazyFrame) -> LazyFrame {
        df.cast(self.dtypes, self.strict)
    }
}

#[cfg(test)]
mod tests {
    use polars::prelude::*;

    use super::*;
    #[test]
    fn test_columns_can_be_cast() {
        let mut dtypes = PlHashMap::new();
        dtypes.insert("a", DataType::Int8);
        dtypes.insert("b", DataType::Int16);
        let transformation = CastCols {
            strict: false,
            dtypes
        };
        let df = df!{
            "a" => [1,2,3,4,5],
            "b" => [6,7,8,9,10],
            "c" => ["a", "b", "c", "d", "e"]
        }.unwrap();
        let result = transformation.apply(df.lazy()).collect().unwrap();
        
        assert_eq!(result.dtypes(), &[DataType::Int8, DataType::Int16, DataType::String]);
    }
}
