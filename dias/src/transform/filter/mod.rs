mod clause;

use clause::Clause;
use polars::prelude::*;
use super::transformer::Transformation;

enum Branch {
    And(Clause),
    Or(Clause),
}
fn make_branch(expr: Expr, branch: &Branch) -> Expr{
    match branch {
        Branch::And(clause) => expr.and(clause.make_expr()),
        Branch::Or(clause) => expr.or(clause.make_expr()),
    }
}
pub struct Filter {
    main_clause: Clause,
    branches: Vec<Branch>
}

impl Filter {
    fn make_filter(self) -> Expr {
        let filter = self.main_clause.make_expr();
        self.branches.iter().fold(filter, make_branch)
    }
}

impl Transformation for Filter {
    fn apply(self, df: LazyFrame) -> LazyFrame {
        df.filter(self.make_filter())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_can_filter_rows() {
        let df = df! {
           "a" => [Some(1), Some(2), Some(3), Some(4), None],
           "b" => [Some(5), None, Some(2), Some(3), Some(1)]
       }.unwrap();

       let transformation = Filter {
           main_clause: Clause {
               column: String::from("a"),
               operator: clause::FilterOperators::PolarsOperator(Operator::GtEq, lit(3)),
           },
           branches: vec![
               Branch::And(Clause {
                   column: String::from("b"),
                   operator: clause::FilterOperators::PolarsOperator(Operator::Lt, col("a")),
               })
           ]
       };
       let result = transformation.apply(df.lazy()).collect().unwrap();
       let expected = df! {
           "a" => [Some(3), Some(4)],
           "b" => [Some(2), Some(3)]
       }.unwrap();

       assert_eq!(expected, result);
    }
}
