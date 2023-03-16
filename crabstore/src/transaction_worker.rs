use crate::transaction::Transaction;
#[derive(Debug)]
pub struct TransactionWorker {
    transactions: Vec<Transaction>,
    table: Arc<Table>,
}
