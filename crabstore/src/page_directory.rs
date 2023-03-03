use std::{
    borrow::Borrow,
    collections::HashMap,
    fs::File,
    hash::{BuildHasher, BuildHasherDefault},
    io::{BufReader, BufWriter, Read, Write},
    ops::RangeBounds,
    path::{Path, PathBuf},
    rc::Rc,
    slice::SliceIndex,
    sync::Arc,
};

use rkyv::{
    collections::ArchivedHashMap,
    de::{deserializers::SharedDeserializeMap, SharedDeserializeRegistry},
    ser::{
        serializers::{AllocScratch, CompositeSerializer, SharedSerializeMap, WriteSerializer},
        Serializer,
    },
    Archive, Deserialize, Serialize,
};
use rustc_hash::{FxHashMap, FxHasher};

use crate::rid::RID;

pub struct PageDirectory {
    path: PathBuf,
    directory: FxHashMap<usize, Arc<[usize]>>,
}

impl PageDirectory {
    #[inline(always)]
    pub fn get(&self, rid: RID) -> Option<Arc<[usize]>> {
        self.get_page(rid.page())
    }

    pub fn get_page(&self, page: usize) -> Option<Arc<[usize]>> {
        self.directory.get(&page).map(Arc::clone)
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

        for (i, x) in page_ids.iter().enumerate() {
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

    pub fn replace_page(
        &mut self,
        page_num: usize,
        replacement: &Arc<[usize]>,
    ) -> Option<Arc<[usize]>> {
        self.directory.insert(page_num, Arc::clone(replacement))
    }

    pub fn new(path: &Path) -> Self {
        if !path.exists() {
            File::create(path).unwrap();
        }

        PageDirectory {
            path: path.into(),
            directory: FxHashMap::with_capacity_and_hasher(
                80000,
                BuildHasherDefault::<FxHasher>::default(),
            ),
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

        let archived = unsafe { rkyv::archived_root::<FxHashMap<usize, Arc<[usize]>>>(&pd_bytes) };

        let directory = archived
            .deserialize(&mut SharedDeserializeMap::new())
            .expect("Failed to deserialize page directory");

        PageDirectory {
            path: path.into(),
            directory,
        }
    }

    pub fn persist(&self) {
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
