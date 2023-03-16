use std::sync::Arc;
use std::{any::Any, iter::Sum};

use bincode::Error;

use crate::rid::RID;
use crate::table::TableData;
use crate::{table::Table, RecordRust};
use core::fmt::Debug;

pub trait Query<Reverse, Output>: Debug {
    fn reverse(&mut self, table: Arc<TableData>) -> Option<Box<Reverse>>;
    fn preval(&mut self, table: Arc<TableData>) -> &Vec<RID>;
    fn run(&mut self, table: Arc<TableData>) -> Result<Box<Output>, QueryError>;
}
#[derive(Debug, Clone)]
pub struct SumQuery {
    start_range: u64,
    end_range: u64,
    column_index: usize,
    preval: Option<Vec<RID>>,
    sum: Option<u64>,
}
impl Query<SumQuery, u64> for SumQuery {
    fn reverse(&mut self, _table: Arc<TableData>) -> Option<Box<SumQuery>> {
        None
    }
    fn run(&mut self, table: Arc<TableData>) -> Result<Box<u64>, QueryError> {
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

    fn preval(&mut self, table: Arc<TableData>) -> &Vec<RID> {
        match &self.preval {
            None => {
                self.preval = Some(table.sum_preval_query(
                    self.start_range,
                    self.end_range,
                    self.column_index,
                ));
            }
            Some(x) => {}
        };
        &self.preval.as_ref().unwrap()
    }
}
#[derive(Debug, Clone)]
pub enum QueryEnum {
    SumQuery(SumQuery),
    SelectQuery(SelectQuery),
    UndeleteQuery(UndeleteQuery),
    DeleteQuery(DeleteQuery),
    InsertQuery(InsertQuery),
    UpdateQuery(UpdateQuery),
}

impl QueryEnum {}
pub enum QueryError {
    InvalidQuery,
    LockFail,
}
#[derive(Debug, Clone)]
pub struct SelectQuery {
    search_value: u64,
    column_index: usize,
    included_columns: Vec<usize>,
    preval: Option<Vec<RID>>,
}
impl Query<SelectQuery, Vec<RecordRust>> for SelectQuery {
    fn reverse(&mut self, table: Arc<TableData>) -> Option<Box<SelectQuery>> {
        Option::<Box<SelectQuery>>::None
    }
    fn run(&mut self, table: Arc<TableData>) -> Result<Box<Vec<RecordRust>>, QueryError> {
        let x: Result<Vec<RecordRust>, QueryError> =
            Ok(table.select_query(self.search_value, self.column_index, &self.included_columns));
        match x {
            Ok(x) => Ok(Box::new(x)),
            Err(_) => Err(QueryError::InvalidQuery),
        }
    }

    fn preval(&mut self, table: Arc<TableData>) -> &Vec<RID> {
        self.preval = Some(table.select_preval_query(
            self.search_value,
            self.column_index,
            &self.included_columns,
        ));
        &self.preval.as_ref().unwrap()
    }
}
#[derive(Debug, Clone)]
pub struct InsertQuery {
    record: Option<RecordRust>,
    values: Vec<u64>,
    preval: Option<Vec<RID>>,
}
impl Query<DeleteQuery, bool> for InsertQuery {
    fn reverse(&mut self, table: Arc<TableData>) -> Option<Box<DeleteQuery>> {
        match &self.record {
            None => None,
            Some(x) => Some(Box::new(DeleteQuery {
                search_value: x.columns[table.primary_key()],
                preval: None,
            })),
        }
    }
    fn run(&mut self, table: Arc<TableData>) -> Result<Box<bool>, QueryError> {
        let x: Result<bool, QueryError> = Ok(table.insert_query(&self.values));
        match x {
            Ok(x) => Ok(Box::new(x)),
            Err(_) => Err(QueryError::InvalidQuery),
        }
    }

    fn preval(&mut self, table: Arc<TableData>) -> &Vec<RID> {
        match self.preval {
            None => {
                self.preval = Some(match table.insert_preval_query(&self.values) {
                    None => Vec::new(),
                    Some(x) => vec![x],
                });
            }
            Some(_) => {}
        };
        &self.preval.as_ref().unwrap()
    }
}
#[derive(Debug, Clone)]
pub struct DeleteQuery {
    search_value: u64,
    preval: Option<Vec<RID>>,
}
impl Query<UndeleteQuery, bool> for DeleteQuery {
    fn reverse(&mut self, table: Arc<TableData>) -> Option<Box<UndeleteQuery>> {
        Some(Box::new(UndeleteQuery {
            search_value: self.search_value,
            preval: self.preval(table).clone(),
        }))
    }
    fn run(&mut self, table: Arc<TableData>) -> Result<Box<bool>, QueryError> {
        let x: Result<bool, QueryError> = Ok(table.delete_query(self.search_value));
        match x {
            Ok(x) => Ok(Box::new(x)),
            Err(_) => Err(QueryError::InvalidQuery),
        }
    }

    fn preval(&mut self, table: Arc<TableData>) -> &Vec<RID> {
        match self.preval {
            None => {
                self.preval = match table.delete_preval_query(self.search_value) {
                    None => Some(Vec::new()),
                    Some(x) => Some(vec![x]),
                }
            }
            Some(_) => {}
        };
        &self.preval.as_ref().unwrap()
    }
}
#[derive(Debug, Clone)]
pub struct UndeleteQuery {
    search_value: u64,
    preval: Vec<RID>,
}
impl Query<DeleteQuery, bool> for UndeleteQuery {
    fn reverse(&mut self, table: Arc<TableData>) -> Option<Box<DeleteQuery>> {
        Some(Box::new(DeleteQuery {
            search_value: self.search_value,
            preval: None,
        }))
    }
    fn run(&mut self, table: Arc<TableData>) -> Result<Box<bool>, QueryError> {
        let x: Result<bool, QueryError> = Ok(table.undelete_query(self.preval[0]));
        match x {
            Ok(x) => Ok(Box::new(x)),
            Err(_) => Err(QueryError::InvalidQuery),
        }
    }

    fn preval(&mut self, table: Arc<TableData>) -> &Vec<RID> {
        &self.preval
    }
}
#[derive(Debug, Clone)]
pub struct UpdateQuery {
    search_value: u64,
    column_index: usize,
    preval: Option<Vec<RID>>,
}
