use std::rc::Rc;
use polars::prelude::*;
use crate::transform::transformer::Transformation;

struct ParseText {
    cols: Vec<Rc<ParseTextOp>>
}

impl ParseText {
    fn get_col_names(&self) -> Vec<String> {
        let mut names = self.cols
            .iter()
            .map(|target| target.get_col_name().clone())
            .collect::<Vec<_>>();
        names.dedup();
        names
    }
    fn get_exprs(self) -> Vec<Expr> {
        self.cols
            .iter()
            .map(|target| target.to_expr())
            .collect::<Vec<_>>()
    }
}

impl Transformation for ParseText {
    fn apply(self, df: LazyFrame) -> LazyFrame {
        df.select(
            itertools::concat([
                vec![all().exclude(self.get_col_names())],
                self.get_exprs()
            ])
        )
    }
}

enum ParseTextOp {
    ToDate{column: String, options: StrptimeOptions, alias: String},
    ToTime{column: String, options: StrptimeOptions, alias: String},
    ToDateTime{
        column: String,
        options: StrptimeOptions,
        time_unit: Option<TimeUnit>,
        time_zone: Option<TimeZone>,
        alias: String
    }
}

impl ParseTextOp {
    fn get_col_name(&self) -> String {
        match self {
            Self::ToDate { column, .. } => String::from(column),
            Self::ToTime { column, .. } => String::from(column),
            Self::ToDateTime { column, .. } => String::from(column)
        }
    }
    fn to_expr(&self) -> Expr {
        match &self {
            ParseTextOp::ToDate { column, options, alias } => {
                col(column).str().to_date(options.clone()).alias(alias)
            },
            ParseTextOp::ToTime { column, options, alias } => {
                col(column).str().to_time(options.clone()).alias(alias)
            },
            ParseTextOp::ToDateTime { column, options, time_unit, time_zone, alias } => {
                col(column).str()
                    .to_datetime(time_unit.clone(), time_zone.clone(), options.clone(), lit("raise"))
                    .alias(alias)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::prelude::*;

    #[test]
    fn test_can_get_column_names() {
        let transformation = ParseText {
            cols: vec![
                Rc::new(ParseTextOp::ToDate {
                    column: String::from("string"),
                    alias: String::from("Date"),
                    options: StrptimeOptions::default()
                }),
                Rc::new(ParseTextOp::ToTime {
                    column: String::from("string"),
                    alias: String::from("Time"),
                    options: StrptimeOptions::default()
                })
            ]
        };
        
        let result = transformation.get_col_names();
        let expected = vec![String::from("string")];
        assert_eq!(expected, result);
    }

    #[test]
    fn test_can_parse_text_into_dates() {
        let transformation = ParseText {
            cols: vec![
                Rc::new(ParseTextOp::ToDate {
                    column: String::from("string"),
                    alias: String::from("Date"),
                    options: StrptimeOptions{
                        format: Some("%Y-%m-%d %H:%M:%s".into()),
                        ..Default::default()
                    }
                })
            ]
        };
        let df = df!(
            "string" => &[
                "2020-01-01 16:50:03",
                "2020-02-02 16:50:03",
                "2020-03-03 16:50:03"
            ]
        ).unwrap();
        let result = transformation.apply(df.lazy()).collect().unwrap();
        let expected = df!(
            "Date" => &[
                NaiveDate::from_ymd_opt(2020, 1, 1).unwrap(),
                NaiveDate::from_ymd_opt(2020, 2, 2).unwrap(),
                NaiveDate::from_ymd_opt(2020, 3, 3).unwrap(),
            ]
        ).unwrap();
        assert_eq!(expected, result);
    }

    #[test]
    fn test_can_parse_text_into_time() {
        let transformation = ParseText {
            cols: vec![
                Rc::new(ParseTextOp::ToTime {
                    column: String::from("string"),
                    alias: String::from("Time"),
                    options: StrptimeOptions{
                        format: Some("%Y-%m-%d %H:%M:%S".into()),
                        ..Default::default()
                    }
                })
            ]
        };
        let df = df!(
            "string" => &[
                "2020-01-01 16:51:01",
                "2020-02-02 16:52:02",
                "2020-03-03 16:53:03"
            ]
        ).unwrap();
        let result = transformation.apply(df.lazy()).collect().unwrap();
        let expected = df!(
            "Time" => &[
                NaiveTime::from_hms_opt(16, 51, 1).unwrap(),
                NaiveTime::from_hms_opt(16, 52, 2).unwrap(),
                NaiveTime::from_hms_opt(16, 53, 3).unwrap(),
            ]
        ).unwrap();
        assert_eq!(expected, result);
    }
    
    #[test]
    fn test_can_parse_text_into_datetimes() {
        let transformation = ParseText {
            cols: vec![
                Rc::new(ParseTextOp::ToDateTime {
                    column: String::from("string"),
                    alias: String::from("DateTime"),
                    options: StrptimeOptions{
                        format: Some("%Y-%m-%d %H:%M:%S".into()),
                        ..Default::default()
                    },
                    time_unit: None,
                    time_zone: None
                })
            ]
        };
        let df = df!(
            "string" => &[
                "2020-01-01 16:51:01",
                "2020-02-02 16:52:02",
                "2020-03-03 16:53:03"
            ]
        ).unwrap();
        let result = transformation.apply(df.lazy()).collect().unwrap();
        let expected = df!(
            "DateTime" => &[
                NaiveDate::from_ymd_opt(2020, 1, 1).unwrap().and_hms_opt(16, 51, 1).unwrap(),
                NaiveDate::from_ymd_opt(2020, 2, 2).unwrap().and_hms_opt(16, 52, 2).unwrap(),
                NaiveDate::from_ymd_opt(2020, 3, 3).unwrap().and_hms_opt(16, 53, 3).unwrap(),
            ]
        ).unwrap();
        assert_eq!(expected, result);
    }

}
