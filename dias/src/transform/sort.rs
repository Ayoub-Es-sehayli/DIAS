use polars::prelude::*;
use super::transformer::Transformation;

pub struct Sort {
    pub by: Vec<String>,
    pub descending: Option<bool>,
    pub descending_multi: Option<Vec<bool>>,
    pub nulls_last: Option<bool>,
    pub nulls_last_multi: Option<Vec<bool>>,
    pub multithreaded: Option<bool>,
    pub maintain_order: Option<bool>,
}

impl Sort {
    fn make_sort_options(&self) -> SortMultipleOptions {
       let mut sort_options = SortMultipleOptions::default();

       if let Some(descending)= &self.descending {
           sort_options = sort_options.with_order_descending(descending.clone());
       }
       if let Some(descending)= &self.descending_multi {
           sort_options = sort_options.with_order_descending_multi(descending.clone());
       }
       if let Some(nulls_last)= &self.nulls_last {
           sort_options = sort_options.with_nulls_last(nulls_last.clone());
       }
       if let Some(nulls_last)= &self.nulls_last_multi {
           sort_options = sort_options.with_nulls_last_multi(nulls_last.clone());
       }
       if let Some(multithreaded)= &self.multithreaded {
           sort_options = sort_options.with_multithreaded(multithreaded.clone());
       }
       if let Some(maintain_order)= &self.maintain_order {
           sort_options = sort_options.with_maintain_order(maintain_order.clone());
       }
       sort_options
    }
}

impl Default for Sort {
    fn default() -> Sort {
        Sort {
            by: Vec::new(),
            descending_multi: None,
            descending: None,
            multithreaded: Some(true),
            maintain_order: Some(false),
            nulls_last: None,
            nulls_last_multi: None
        }
    }
}

impl Transformation for Sort {
    fn apply(self, df: LazyFrame) -> LazyFrame {
       let sort_options = self.make_sort_options();
       df.sort(self.by, sort_options)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_df_sorted() {
       let df = df! {
           "a" => [Some(1), Some(2), None, Some(4), None],
           "b" => [Some(5), None, Some(3), Some(2), Some(1)]
       }.unwrap();
       let transformation = Sort{
            by: vec![String::from("a"), String::from("b")],
            descending_multi: Some(vec![false, true]),
            nulls_last: Some(true),
            ..Default::default()
       };
       let result = transformation.apply(df.lazy()).collect().unwrap();
       let expected = df! {
           "a" => [Some(1), Some(2), Some(4), None, None],
           "b" => [Some(5), None, Some(2), Some(3), Some(1)]
       }.unwrap();

       assert_eq!(expected, result);
    }
}
