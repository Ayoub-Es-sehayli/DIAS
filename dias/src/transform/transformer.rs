use polars::prelude::*;

pub trait Transformation {
    fn apply(self, df: LazyFrame) -> LazyFrame;
}

