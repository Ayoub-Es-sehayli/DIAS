use polars::prelude::*;

pub struct Clause {
    pub column: String,
    pub operator: FilterOperators,
}

impl Clause {

    fn make_operation(&self, expr: Expr) -> Expr {
        match &self.operator {
            FilterOperators::PolarsOperator(op, value) => {
                if op.is_arithmetic() {
                    panic!("Cannot use arithmetic operators in a filter step");
                }
                binary_expr(expr, *op, value.clone())
            },
            FilterOperators::PolarsBooleanFunction(boolean_func) => {
                match boolean_func {
                    BooleanFunction::Not => panic!("Cannot use `Not` Expression by itself to construct a Filter step"),
                    BooleanFunction::AllHorizontal | BooleanFunction::AnyHorizontal => panic!("Cannot use AllHorizontal & AnyHorizontal to construct a Filter step"),
                    BooleanFunction::IsIn => panic!("Cannot use BooleanFunction::IsIn, please use FilterOperators::IsIn instead"),
                    BooleanFunction::Any { ignore_nulls } => expr.any(*ignore_nulls),
                    BooleanFunction::All { ignore_nulls } => expr.all(*ignore_nulls),
                    BooleanFunction::IsNan => expr.is_nan(),
                    BooleanFunction::IsNotNan => expr.is_not_nan(),
                    BooleanFunction::IsNull => expr.is_null(),
                    BooleanFunction::IsNotNull => expr.is_not_null(),
                    BooleanFunction::IsFinite => expr.is_finite(),
                    BooleanFunction::IsInfinite => expr.is_infinite(),
                }
            },
            FilterOperators::Contains { pattern, literal, strict } => {
                if *literal {
                    return expr.str().contains_literal(lit(pattern.clone()));
                }
                expr.str().contains(lit(pattern.clone()), *strict)
            },
            FilterOperators::NotContains { pattern, literal, strict } => {
                if *literal {
                    return expr.str().contains_literal(lit(pattern.clone())).not();
                }
                expr.str().contains(lit(pattern.clone()), *strict).not()
            },
            FilterOperators::StartsWith(substr) => expr.str().starts_with(lit(substr.clone())),
            FilterOperators::EndsWith(substr) => expr.str().ends_with(lit(substr.clone())),
            FilterOperators::NotStartsWith(substr) => expr.str().starts_with(lit(substr.clone())).not(),
            FilterOperators::NotEndsWith(substr) => expr.str().ends_with(lit(substr.clone())).not(),

            FilterOperators::IsIn(e) => expr.is_in(e.clone())
        }
    }

    pub fn make_expr(&self) -> Expr {
        let expr = col(&self.column);

        self.make_operation(expr)
    }
}

pub enum FilterOperators{
    PolarsOperator(Operator, Expr),
    PolarsBooleanFunction(BooleanFunction),
    // String Filters
    Contains { pattern: String, literal: bool, strict: bool },
    NotContains { pattern: String, literal: bool, strict: bool },
    StartsWith(String),
    EndsWith(String),
    NotStartsWith(String),
    NotEndsWith(String),
    // Lookup Filters
    IsIn(Expr)
}
