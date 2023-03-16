use crate::transaction::Transaction;

/*struct TransactionWorker {
    transactions: Vec<Transaction>,
    thread: Option<std::thread::JoinHandle<()>>,
    loc
}
impl TransactionWorker {
    pub fn new(table: Arc<TableData>) -> Self {
        let thread = None;
        Self {
            transactions: Vec::new(),
            table,
            thread,
        }
    }
    pub fn add_transaction(&mut self, transaction: Transaction) {
        self.transactions.push(transaction);
    }
    pub fn get_transactions(&self) -> &Vec<Transaction> {
        &self.transactions
    }
    pub fn get_transactions_mut(&mut self) -> &mut Vec<Transaction> {
        &mut self.transactions
    }
    pub fn get_table(&self) -> Arc<TableData> {
        self.table.clone()
    }
    pub fn get_table_mut(&mut self) -> &mut Arc<TableData> {
        &mut self.table
    }
    pub fn join(self) -> std::thread::Result<()> {
        self.thread.unwrap().join()
    }
}

*/
