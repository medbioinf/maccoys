// std imports
use std::{collections::HashMap, rc::Rc, slice::Iter};

// 3rd party imports
use polars::{frame::DataFrame, prelude::AnyValue, series::SeriesIter};

/// Row of a dataframe
///
pub struct Row<'a> {
    col_index: Rc<HashMap<String, usize>>,
    col_values: Vec<AnyValue<'a>>,
}

impl<'a> Row<'a> {
    pub fn new(col_index: Rc<HashMap<String, usize>>, col_values: Vec<AnyValue<'a>>) -> Row<'a> {
        Row {
            col_index,
            col_values,
        }
    }

    pub fn get_values(&'a self) -> &'a Vec<AnyValue<'a>> {
        &self.col_values
    }

    pub fn iter(&self) -> Iter<'_, AnyValue<'a>> {
        self.col_values.iter()
    }

    pub fn len(&self) -> usize {
        self.col_values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.col_values.is_empty()
    }
}

impl<'a> std::ops::Index<&str> for Row<'a> {
    type Output = AnyValue<'a>;

    fn index(&self, col_name: &str) -> &Self::Output {
        let col_index = self.col_index.get(col_name).unwrap();
        &self.col_values[*col_index]
    }
}

/// Iterates the rows of the dataframe. Probably a bit more efficient than using the `DataFrame::get_row` method,
/// which is discouraged in the polars documentation.
pub struct RowIter<'a> {
    col_index: Rc<HashMap<String, usize>>,
    col_iterators: Vec<SeriesIter<'a>>,
}

impl<'a> RowIter<'a> {
    pub fn new(dataframe: &'a DataFrame) -> Self {
        let col_index = Rc::new(
            dataframe
                .columns()
                .iter()
                .enumerate()
                .map(|(i, col)| (col.name().to_string(), i))
                .collect::<HashMap<String, usize>>(),
        );
        let col_iterators = dataframe
            .columns()
            .iter()
            .map(|col| col.as_materialized_series().iter())
            .collect::<Vec<SeriesIter<'_>>>();
        Self {
            col_index,
            col_iterators,
        }
    }
}

impl<'a> Iterator for RowIter<'a> {
    type Item = Row<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut values = Vec::with_capacity(self.col_iterators.len());
        for iter in self.col_iterators.iter_mut() {
            values.push(iter.next()?);
        }
        Some(Row::new(self.col_index.clone(), values))
    }
}
