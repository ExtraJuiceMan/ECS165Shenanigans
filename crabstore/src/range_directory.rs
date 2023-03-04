use crate::{page::PageRange, rid::RID};
use rkyv::{
    de::deserializers::SharedDeserializeMap,
    ser::{
        serializers::{AllocScratch, CompositeSerializer, SharedSerializeMap, WriteSerializer},
        Serializer,
    },
    Deserialize,
};

use std::{
    fs::File,
    io::{BufWriter, Read, Write},
    path::{Path, PathBuf},
};

pub struct RangeDirectory {
    path: PathBuf,
    directory: Vec<PageRange>,
}

impl RangeDirectory {
    pub fn get(&self, range: usize) -> &PageRange {
        &self.directory[range]
    }

    pub fn next_tid(&self, range: usize) -> RID {
        self.directory[range].next_tid()
    }

    pub fn next_range_id(&self) -> usize {
        self.directory.len()
    }

    pub fn allocate_range(&mut self, range: PageRange) {
        self.directory.push(range);
    }

    pub fn new_range_tail(&mut self, range: usize, new_tail: PageRange) {
        self.directory[range].current_tail_page = new_tail.current_tail_page;
        self.directory[range].next_tid = new_tail.next_tid;
    }

    pub fn new(path: &Path) -> Self {
        RangeDirectory {
            path: path.into(),
            directory: Vec::new(),
        }
    }

    pub fn load(path: &Path) -> Self {
        let mut rd_file = File::options().read(true).open(path).unwrap();
        let mut rd_bytes = Vec::new();

        rd_file
            .read_to_end(&mut rd_bytes)
            .expect("Unable to read page directory file");

        let archived = unsafe { rkyv::archived_root::<Vec<PageRange>>(&rd_bytes) };
        let directory = archived
            .deserialize(&mut SharedDeserializeMap::new())
            .expect("Failed to deserialize page directory");

        RangeDirectory {
            path: path.into(),
            directory,
        }
    }

    pub fn persist(&self) {
        let rd_file = File::options()
            .write(true)
            .truncate(true)
            .create(true)
            .open(self.path.clone())
            .expect("Unable to open range directory file");

        let mut serializer = CompositeSerializer::new(
            WriteSerializer::new(BufWriter::new(rd_file)),
            AllocScratch::default(),
            SharedSerializeMap::new(),
        );

        serializer
            .serialize_value(&self.directory)
            .expect("Unable to serialize range directory");

        let (buf, _, _) = serializer.into_components();

        buf.into_inner()
            .flush()
            .expect("Failed to flush range directory");
    }
}
