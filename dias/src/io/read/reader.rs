use polars::prelude::*;

pub trait Reader {
    fn extract(self) -> PolarsResult<DataFrame>;
}
