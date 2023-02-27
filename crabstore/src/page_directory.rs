use std::{
    borrow::Borrow,
    collections::HashMap,
    fs::File,
    io::{BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
};

use nohash::BuildNoHashHasher;
use rkyv::{
    ser::{
        serializers::{
            AllocScratch, AllocSerializer, CompositeSerializer, SharedSerializeMap, WriteSerializer,
        },
        Serializer,
    },
    Archive, Deserialize, Serialize,
};

#[derive(Archive, Deserialize, Serialize, Clone)]
pub struct PageDirectoryEntry {
    col_pages: Box<[usize]>,
}

impl PageDirectoryEntry {
    pub fn new(num_columns: usize) -> Self {
        let mut columns: Vec<usize> = Vec::with_capacity(crate::NUM_METADATA_COLUMNS + num_columns);
        columns.resize_with(crate::NUM_METADATA_COLUMNS + num_columns, Default::default);
        PageDirectoryEntry {
            col_pages: columns.into_boxed_slice(),
        }
    }

    pub fn column_page(&self, index: usize) -> usize {
        self.col_pages[index]
    }
}

pub struct PageDirectory {
    path: PathBuf,
    directory: HashMap<usize, PageDirectoryEntry, BuildNoHashHasher<usize>>,
}

impl PageDirectory {
    pub fn new(path: &Path) -> Self {
        if !path.exists() {
            File::create(path).unwrap();
        }

        PageDirectory {
            path: path.into(),
            directory: HashMap::with_hasher(BuildNoHashHasher::default()),
        }
    }

    pub fn load(path: &Path) -> Self {
        if !path.exists() {
            File::create(path).unwrap();
            return PageDirectory::new(path);
        }

        let mut pd_file = File::options().read(true).open(path).unwrap();
        let mut pd_bytes = Vec::new();

        pd_file
            .read_to_end(&mut pd_bytes)
            .expect("Unable to read page directory file");

        let archived = unsafe {
            rkyv::archived_root::<HashMap<usize, PageDirectoryEntry, BuildNoHashHasher<usize>>>(
                &pd_bytes,
            )
        };

        PageDirectory {
            path: path.into(),
            directory: archived
                .deserialize(&mut rkyv::Infallible)
                .expect("Failed to deserialize page directory"),
        }
    }

    pub fn persist(&mut self) {
        self.directory.insert(1, PageDirectoryEntry::new(5));
        let pd_file = File::options()
            .write(true)
            .truncate(true)
            .open(self.path.clone())
            .unwrap();

        let mut serializer = CompositeSerializer::new(
            WriteSerializer::new(BufWriter::new(pd_file)),
            AllocScratch::default(),
            SharedSerializeMap::new(),
        );

        serializer
            .serialize_value(&self.directory)
            .expect("Unable to serialize page directory");

        let (buf, _, _) = serializer.into_components();

        buf.into_inner()
            .flush()
            .expect("Failed to flush page directory");
    }
}
