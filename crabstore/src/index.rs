use crate::rid::RID;
use core::fmt;
use pyo3::prelude::*;
use rkyv::{
    de::deserializers::SharedDeserializeMap,
    ser::{
        serializers::{AllocScratch, CompositeSerializer, SharedSerializeMap, WriteSerializer},
        Serializer,
    },
    Archive, Deserialize, Serialize,
};
use serde::{Deserialize as SerdeDeserialize, Serialize as SerdeSerialize};
use std::{
    collections::BTreeMap,
    fmt::Debug,
    io::{BufWriter, Read, Write},
    ops::{Bound, RangeBounds},
    path::PathBuf,
    sync::RwLock,
};
use std::{fs::File, path::Path};
pub trait Indexable<K: Debug + Ord, V: Debug>: Send + Sync + 'static {
    fn update(&self, key: K, rid: V);
    fn get(&self, key: &K) -> Option<&Vec<V>>;
    fn get_range(&self, start: K, end: K) -> Vec<V>;
    fn remove(&self, key: K, value: V);
    fn iter(&self) -> Box<dyn Iterator<Item = (K, V)> + '_>;
    fn new() -> Self
    where
        Self: Sized;
}
impl Debug for dyn Indexable<u64, RID> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Indexable")
    }
}
#[derive(Archive, serde::Serialize, serde::Deserialize)]
pub struct BTreeIndex<K, V> {
    index: RwLock<BTreeMap<K, Vec<V>>>,
}
impl<K, V> Indexable<K, V> for BTreeIndex<K, V>
where
    K: Debug + Sync + Send + Ord + Clone + 'static,
    V: Debug + Sync + Send + Ord + Clone + 'static,
{
    fn update(&self, key: K, value: V) {
        if let Some(ref mut rids) = self.index.write().unwrap().get_mut(&key) {
            rids.push(value);
        } else {
            let mut vec = Vec::with_capacity(4);
            vec.push(value);
            self.index.write().unwrap().insert(key, vec);
        }
    }
    fn get(&self, key: &K) -> Option<&Vec<V>> {
        self.index.read().unwrap().get(key)
    }
    fn get_range(&self, start: K, end: K) -> Vec<V> {
        self.index
            .read()
            .unwrap()
            .range(start..=end)
            .flat_map(|item| item.1.clone())
            .collect::<Vec<V>>()
    }
    fn remove(&self, key: K, value: V) {
        if let Some(ref mut rids) = self.index.write().unwrap().get_mut(&key) {
            rids.retain(|x| *x != value);
        }
    }

    fn iter(&self) -> Box<dyn Iterator<Item = (K, V)> + '_> {
        Box::new(self.index.read().unwrap().iter().flat_map(|(k, v)| {
            v.iter().map(move |x| {
                let k = k.clone();
                let x = x.clone();
                (k, x)
            })
        }))
    }
    fn new() -> Self
    where
        Self: Sized,
    {
        Self {
            index: RwLock::new(BTreeMap::new()),
        }
    }
}
#[derive(Debug)]
//change to BTreeMap when we need to implement ranges
pub struct Index {
    path: PathBuf,
    indices: Vec<Option<Box<dyn Indexable<u64, RID>>>>,
}

impl fmt::Display for Index {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for (i, v) in self.indices.iter().enumerate() {
            write!(f, "Index on Column {}:\n", i).unwrap();
            match &v {
                Some(v) => {
                    for (key, value) in v.iter() {
                        write!(f, "Key: {} | Value: {:?}\n", key, value)?;
                    }
                }
                None => {
                    write!(f, "None\n").unwrap();
                }
            }
        }

        Ok(())
    }
}

impl Index {
    pub fn new(key_index: usize, num_columns: usize, path: &Path) -> Self {
        let mut indices: Vec<Option<Box<dyn Indexable<u64, RID>>>> =
            Vec::with_capacity(num_columns);
        indices.resize_with(num_columns, Default::default);
        indices[key_index] = Some(Box::new(BTreeIndex::new()));

        Index {
            path: path.into(),
            indices,
        }
    }

    pub fn load(path: &Path) -> Self {
        let mut id_file = File::options()
            .read(true)
            .open(path)
            .expect("Unable to open index file");

        let mut id_bytes = Vec::new();

        id_file
            .read_to_end(&mut id_bytes)
            .expect("Unable to read index file");

        let archived =
            unsafe { rkyv::archived_root::<Vec<Option<&dyn Indexable<u64, Vec<RID>>>>>(&id_bytes) };

        Index {
            path: path.into(),
            indices: archived
                .deserialize(&mut SharedDeserializeMap::new())
                .expect("Failed to deserialize page directory"),
        }
    }

    pub fn persist(&self) {
        let id_file = File::options()
            .write(true)
            .truncate(true)
            .create(true)
            .open(&self.path)
            .expect("Unable to open index file");

        let mut serializer = CompositeSerializer::new(
            WriteSerializer::new(BufWriter::new(id_file)),
            AllocScratch::default(),
            SharedSerializeMap::new(),
        );

        serializer
            .serialize_value(&self.indices)
            .expect("Unable to serialize indexes");

        let (buf, _, _) = serializer.into_components();

        buf.into_inner().flush().expect("Failed to flush indices");
    }

    pub fn update_index(&mut self, column_number: usize, value: u64, rid: RID) {
        if let Some(ref mut index) = self.indices[column_number] {
            index.update(value, rid);
        }
    }

    pub fn remove_index(&mut self, column_number: usize, value: u64, rid: RID) {
        if let Some(ref mut index) = self.indices[column_number] {
            index.remove(value, rid);
        }
    }

    pub fn get_from_index(&self, column_number: usize, value: u64) -> Option<Vec<RID>> {
        self.indices[column_number]
            .as_ref()
            .map(|map| match map.get(&value) {
                None => Vec::new(),
                Some(rids) => rids.clone(),
            })
    }

    pub fn range_from_index(&self, column_number: usize, start: u64, end: u64) -> Option<Vec<RID>> {
        self.indices[column_number]
            .as_ref()
            .map(|map| map.get_range(start, end))
    }

    pub fn create_index<T: Indexable<u64, RID>>(&mut self, column_number: usize) {
        self.indices[column_number] = Some(Box::new(T::new()));
    }

    pub fn drop_index(&mut self, column_number: usize) {
        self.indices[column_number] = None;
    }
}
