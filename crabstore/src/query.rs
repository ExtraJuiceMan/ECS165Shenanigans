use std::{any::Any, iter::Sum};

use bincode::Error;

use crate::{table::Table, RecordRust};

pub trait Query {
    fn reverse(&self, table: &Table) -> Option<impl Query>;
    fn run(&self, table: &Table) -> Result<impl Any, QueryError>;
}
pub struct SumQuery {
    start_range: u64,
    end_range: u64,
    column_index: usize,
}
impl Query for SumQuery {
    fn reverse(&self, table: &Table) -> Option<impl Query> {
        Option::<SumQuery>::None
    }
    fn run(&self, table: &Table) -> Result<u64, QueryError> {
        let x: Result<u64, QueryError> =
            Ok(table.sum_query(self.start_range, self.end_range, self.column_index));
        match x {
            Ok(x) => Ok(x),
            Err(_) => Err(QueryError::InvalidQuery),
        }
    }
}
pub enum QueryError {
    InvalidQuery,
    LockFail,
}
pub struct SelectQuery {
    search_value: u64,
    column_index: usize,
    included_columns: Vec<usize>,
}
impl Query for SelectQuery {
    fn reverse(&self, table: &Table) -> Option<impl Query> {
        Option::<SelectQuery>::None
    }
    fn run(&self, table: &Table) -> Result<Vec<RecordRust>, QueryError> {
        let x: Result<Vec<RecordRust>, QueryError> =
            Ok(table.select_query(self.search_value, self.column_index, &self.included_columns));
        match x {
            Ok(x) => Ok(x),
            Err(_) => Err(QueryError::InvalidQuery),
        }
    }
}
pub struct UndeleteQuery {}
