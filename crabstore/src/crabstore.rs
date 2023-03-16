use std::{
    collections::HashMap,
    fs::{self, File},
    io::{self, BufWriter, Read, Seek, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use rkyv::ser::{
    serializers::{AllocScratch, CompositeSerializer, SharedSerializeMap, WriteSerializer},
    Serializer,
};

use crate::table::Table;

#[derive(Clone, Debug, Default)]
pub struct CrabStore {
    pub directory: PathBuf,
    tables: HashMap<String, Arc<Table>>,
}

impl CrabStore {
    pub fn load_table_index(file: &Path) -> Vec<String> {
        let crab_file = File::options().read(true).open(file);

        if crab_file.is_err() && crab_file.as_ref().unwrap_err().kind() == io::ErrorKind::NotFound {
            File::create(file).expect("Failed to find database file");

            return Vec::new();
        }

        let mut crab_file = crab_file.unwrap();

        crab_file.rewind().unwrap();

        let mut crab_bytes = Vec::new();
        crab_file.read_to_end(&mut crab_bytes).unwrap();

        unsafe {
            rkyv::from_bytes_unchecked::<Vec<String>>(&crab_bytes)
                .expect("Failed to deserialize database file")
        }
    }

    pub fn persist_table_index(file: &Path, table_names: Vec<String>) {
        let mut crab_file = File::options()
            .write(true)
            .truncate(true)
            .create(true)
            .open(file)
            .expect("Failed to open database file");

        crab_file.rewind().unwrap();

        let mut bufwriter = BufWriter::new(crab_file);

        bufwriter.rewind().unwrap();

        let mut serializer = CompositeSerializer::new(
            WriteSerializer::new(bufwriter),
            AllocScratch::default(),
            SharedSerializeMap::new(),
        );

        serializer
            .serialize_value(&table_names)
            .expect("Unable to serialize table names");

        let (buf, _, _) = serializer.into_components();

        buf.into_inner().flush().unwrap();
    }

    pub fn database_filename(directory: &Path) -> PathBuf {
        directory.join(Path::new("crab_dt.CRAB"))
    }

    pub fn table_filename(directory: &Path, table: &str) -> PathBuf {
        let mut table_file = table.to_string();
        table_file.push_str("_db.CRAB");

        directory.join(Path::new(&table_file))
    }

    pub fn page_dir_filename(directory: &Path, table: &str) -> PathBuf {
        let mut pd_file = table.to_string();
        pd_file.push_str("_pd.CRAB");

        directory.join(Path::new(&pd_file))
    }

    pub fn index_filename(directory: &Path, table: &str) -> PathBuf {
        let mut id_file = table.to_string();
        id_file.push_str("_id.CRAB");

        directory.join(Path::new(&id_file))
    }

    pub fn range_filename(directory: &Path, table: &str) -> PathBuf {
        let mut rd_file = table.to_string();
        rd_file.push_str("_rd.CRAB");

        directory.join(Path::new(&rd_file))
    }
}

impl CrabStore {
    pub fn new(directory: PathBuf) -> Self {
        CrabStore {
            directory,
            tables: HashMap::new(),
        }
    }

    pub fn create_table(&mut self, name: &str, num_columns: usize, key_index: usize) -> Arc<Table> {
        let table = Arc::new(Table::new(
            name.to_string(),
            num_columns,
            key_index,
            &CrabStore::table_filename(&self.directory, name),
            &CrabStore::page_dir_filename(&self.directory, name),
            &CrabStore::index_filename(&self.directory, name),
            &CrabStore::range_filename(&self.directory, name),
        ));
        self.tables.insert(name.to_string(), Arc::clone(&table));
        table
    }

    pub fn drop_table(&mut self, name: &str) -> bool {
        self.tables.remove(name);
        true
    }

    pub fn get_table(&self, name: &str) -> Arc<Table> {
        Arc::clone(self.tables.get(name).expect("Table not found"))
    }

    pub fn open(&mut self) {
        fs::create_dir_all(&self.directory).expect("Failed to create database directories.");

        let table_names =
            CrabStore::load_table_index(&CrabStore::database_filename(&self.directory));

        for name in table_names.iter() {
            self.tables.insert(
                name.to_string(),
                Arc::new(Table::load(
                    name,
                    &CrabStore::table_filename(&self.directory, name),
                    &CrabStore::page_dir_filename(&self.directory, name),
                    &CrabStore::index_filename(&self.directory, name),
                    &CrabStore::range_filename(&self.directory, name),
                )),
            );
        }
    }

    pub fn close(&mut self) {
        let table_names = self.tables.keys().cloned().collect::<Vec<String>>();

        CrabStore::persist_table_index(&CrabStore::database_filename(&self.directory), table_names);

        for table in self.tables.values() {
            table.persist();
        }

        self.tables.clear();
    }

    fn delete(path: String) {
        fs::remove_dir_all(path).unwrap();
    }
}
