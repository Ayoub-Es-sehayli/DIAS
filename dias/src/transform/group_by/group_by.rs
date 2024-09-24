use polars::prelude::*;

use crate::transform::transformer::Transformation;

struct Aggregation {
    new_column: String,
    aggregation: AggExpr
}

struct GroupBy {
    grouping: Vec<Expr>,
    aggregations: Vec<Aggregation>
}

impl GroupBy {
    fn make_aggregation_expr(self) -> Vec<Expr> {
        self.aggregations
            .into_iter()
            .map(|agg| Expr::Agg(agg.aggregation.clone()).alias(&agg.new_column))
            .collect::<Vec<_>>()
    }
}

impl Transformation for GroupBy {
    fn apply(self, df: LazyFrame) -> LazyFrame {
        df.group_by(&self.grouping)
          .agg(self.make_aggregation_expr())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_can_aggregate_on_group_by() {
        let df = df!(
            "customer_id" => &[1, 2, 2, 3],
            "name" => &["Alice", "Bob", "Bob", "Charlie"],
            "order_id"=> &[Some("a"), Some("b"), Some("c"), None],
            "amount"=> &[Some(100), Some(200), Some(300), None],
        ).unwrap();
        let aggregations: Vec<Aggregation> = vec![
                Aggregation { new_column: String::from("Sum"), aggregation: AggExpr::Sum(Arc::new("amount".into())) }            
        ];
        let transformation = GroupBy {
            grouping: vec!["name".into()],
            aggregations
        };
        let result = transformation.apply(df.lazy()).collect().unwrap();
        let expected = df!(
            "name" => &["Alice", "Bob", "Charlie"],
            "Sum" => &[100, 500, 0]
        ).unwrap();

        assert_eq!(expected, result);
    }
}
