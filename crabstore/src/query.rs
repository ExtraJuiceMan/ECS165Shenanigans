use std::{any::Any, iter::Sum};

use bincode::Error;

use crate::rid::RID;
use crate::{table::Table, RecordRust};
pub trait Query<Reverse, Output> {
    fn reverse(&self, table: &Table) -> Option<Box<Reverse>>;
    fn preval(&self, table: &Table) -> Vec<RID>;
    fn run(&self, table: &Table) -> Result<Box<Output>, QueryError>;
}
pub struct SumQuery {
    start_range: u64,
    end_range: u64,
    column_index: usize,
}
impl Query<SumQuery, u64> for SumQuery {
    fn reverse(&self, table: &Table) -> Option<Box<SumQuery>> {
        None
    }
    fn run(&self, table: &Table) -> Result<Box<u64>, QueryError> {
        let x: Result<Box<u64>, QueryError> = Ok(Box::new(table.sum_query(
            self.start_range,
            self.end_range,
            self.column_index,
        )));
        match x {
            Ok(x) => Ok(x),
            Err(_) => Err(QueryError::InvalidQuery),
        }
    }

    fn preval(&self, table: &Table) -> Vec<RID> {
        todo!()
    }
}
pub enum QueryEnum {
    SumQuery(SumQuery),
    SelectQuery(SelectQuery),
    UndeleteQuery(UndeleteQuery),
}

impl QueryEnum {}
pub enum QueryError {
    InvalidQuery,
    LockFail,
}
pub struct SelectQuery {
    search_value: u64,
    column_index: usize,
    included_columns: Vec<usize>,
}
impl Query<SelectQuery, Vec<RecordRust>> for SelectQuery {
    fn reverse(&self, table: &Table) -> Option<Box<SelectQuery>> {
        Option::<Box<SelectQuery>>::None
    }
    fn run(&self, table: &Table) -> Result<Box<Vec<RecordRust>>, QueryError> {
        let x: Result<Vec<RecordRust>, QueryError> =
            Ok(table.select_query(self.search_value, self.column_index, &self.included_columns));
        match x {
            Ok(x) => Ok(Box::new(x)),
            Err(_) => Err(QueryError::InvalidQuery),
        }
    }

    fn preval(&self, table: &Table) -> Vec<RID> {
        todo!()
    }
}
pub struct UndeleteQuery {
    search_value: u64,
    column_index: usize,
}
