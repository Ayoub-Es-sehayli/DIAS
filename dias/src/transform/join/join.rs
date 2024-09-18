use polars::prelude::*;
use std::default::Default;

use crate::transform::transformer::Transformation;

struct Join<'a> {
    strategy: JoinType,
    right: Option<LazyFrame>,
    left_on: &'a [Expr],
    right_on: &'a [Expr],
    allow_parallel: bool,
    force_parallel: bool,
    join_nulls: bool,
    coalese: JoinCoalesce,
    suffix: Option<&'a str>,
}

impl<'a> Default for Join<'a> {
    fn default() -> Join<'a> {
        Join {
            strategy: JoinType::Left,
            right: None,
            left_on: &[],
            right_on: &[],
            allow_parallel: false,
            force_parallel: false,
            join_nulls: false,
            coalese: JoinCoalesce::JoinSpecific,
            suffix: Some("_right")
        }
    }
}

impl<'a> Transformation for Join<'a> {
    fn apply(self, df: LazyFrame) -> LazyFrame {
        let mut builder = df.join_builder();
        match self.right {
            Some(right) => {
                builder = builder.with(right);
            },
            _ => panic!("Expected a right Dataframe")
        }

        if self.left_on.len() == 0 || self.right_on.len() == 0 {
            panic!("No columns to join on");
        }

        builder = builder
            .how(self.strategy)
            .left_on(self.left_on)
            .right_on(self.right_on)
            .allow_parallel(self.allow_parallel)
            .force_parallel(self.force_parallel)
            .coalesce(self.coalese)
            .join_nulls(self.join_nulls);

        if let Some(suffix) = self.suffix {
            builder = builder.suffix(suffix);
        }

        builder.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_can_perform_join() {
        let df_customers = df! (
            "customer_id" => &[1, 2, 3],
            "name" => &["Alice", "Bob", "Charlie"],
        ).unwrap();
        let df_orders = df!(
            "order_id"=> &["a", "b", "c"],
            "customer_id"=> &[1, 2, 2],
            "amount"=> &[100, 200, 300],
        ).unwrap();
        let transformation = Join {
            strategy: JoinType::Left,
            right: Some(df_orders.lazy()),
            left_on: &[col("customer_id")],
            right_on: &[col("customer_id")],
            ..Default::default()
        };

        let result = transformation.apply(df_customers.lazy()).collect().unwrap();
        let expected = df!(
            "customer_id" => &[1, 2, 2, 3],
            "name" => &["Alice", "Bob", "Bob", "Charlie"],
            "order_id"=> &[Some("a"), Some("b"), Some("c"), None],
            "amount"=> &[Some(100), Some(200), Some(300), None],
        ).unwrap();

        assert_eq!(result, expected);
    }
}
