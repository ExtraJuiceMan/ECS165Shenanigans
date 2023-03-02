use std::{
    borrow::Borrow,
    collections::HashMap,
    fs::File,
    io::{BufReader, BufWriter, Read, Write},
    ops::RangeBounds,
    path::{Path, PathBuf},
    rc::Rc,
    slice::SliceIndex,
    sync::Arc,
};

use nohash::BuildNoHashHasher;
use rkyv::{
    de::{deserializers::SharedDeserializeMap, SharedDeserializeRegistry},
    ser::{
        serializers::{AllocScratch, CompositeSerializer, SharedSerializeMap, WriteSerializer},
        Serializer,
    },
    Archive, Deserialize, Serialize,
};

use crate::rid::RID;

pub struct PageDirectory {
    path: PathBuf,
    directory: HashMap<usize, Arc<[usize]>, BuildNoHashHasher<usize>>,
}

impl PageDirectory {
    pub fn get(&self, rid: RID) -> Option<Arc<[usize]>> {
        self.directory.get(&rid.page()).map(|x| Arc::clone(x))
    }

    pub fn set(&mut self, rid: RID, page_ids: &[Option<usize>]) {
        if let Some(current_vals) = self.directory.get_mut(&rid.page()) {
            let mut cols_clone = Arc::<[usize]>::new_uninit_slice(current_vals.len());

            for (i, potential_page) in page_ids.into_iter().enumerate() {
                if let Some(new_page) = potential_page {
                    Arc::get_mut(&mut cols_clone).unwrap()[i].write(*new_page);
                } else {
                    Arc::get_mut(&mut cols_clone).unwrap()[i].write(current_vals[i]);
                }
            }

            self.directory
                .insert(rid.page(), unsafe { cols_clone.assume_init() });

            return;
        }

        let mut entry = Arc::<[usize]>::new_uninit_slice(page_ids.len());

        for (i, x) in page_ids.into_iter().enumerate() {
            Arc::get_mut(&mut entry).unwrap()[i]
                .write(x.expect("Must provide all columns of page dir entry if new"));
        }

        self.directory
            .insert(rid.page(), unsafe { entry.assume_init() });
    }

    pub fn new_page(&mut self, page_num: usize, column_page_ids: Arc<[usize]>) {
        self.directory
            .try_insert(page_num, Arc::clone(&column_page_ids))
            .expect("Tried to allocate new page with existing page number");
    }

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
            return PageDirectory::new(path);
        }

        let mut pd_file = File::options().read(true).open(path).unwrap();
        let mut pd_bytes = Vec::new();

        pd_file
            .read_to_end(&mut pd_bytes)
            .expect("Unable to read page directory file");

        let archived = unsafe {
            rkyv::archived_root::<HashMap<usize, Arc<[usize]>, BuildNoHashHasher<usize>>>(&pd_bytes)
        };

        PageDirectory {
            path: path.into(),
            directory: archived
                .deserialize(&mut SharedDeserializeMap::new())
                .expect("Failed to deserialize page directory"),
        }
    }

    pub fn persist(&mut self) {
        let pd_file = File::options()
            .write(true)
            .truncate(true)
            .open(self.path.clone())
            .expect("Unable to open page directory file");

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
