use crate::rid::RID;
use core::fmt;
use pyo3::prelude::*;
use rkyv::{
    de::deserializers::SharedDeserializeMap,
    ser::{
        serializers::{AllocScratch, CompositeSerializer, SharedSerializeMap, WriteSerializer},
        Serializer,
    },
    Deserialize,
};
use std::{
    collections::BTreeMap,
    io::{BufWriter, Read, Write},
    ops::RangeBounds,
    path::PathBuf,
};
use std::{fs::File, path::Path};

#[derive(Clone, Debug, Default)]
#[pyclass(subclass)]
//change to BTreeMap when we need to implement ranges
pub struct Index {
    path: PathBuf,
    indices: Vec<Option<BTreeMap<u64, Vec<RID>>>>,
}
impl fmt::Display for Index {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for (i, v) in self.indices.iter().enumerate() {
            write!(f, "Index on Column {}:\n", i).unwrap();
            match v {
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
        let mut indices = Vec::with_capacity(num_columns);
        indices.resize_with(num_columns, Default::default);
        indices[key_index] = Some(BTreeMap::new());

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
            unsafe { rkyv::archived_root::<Vec<Option<BTreeMap<u64, Vec<RID>>>>>(&id_bytes) };

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

    pub fn index_meta_to_bit_vector(&self) -> usize {
        let mut bit_vector: usize = 0;
        for (i, x) in self.indices.iter().enumerate() {
            if x.is_none() {
                continue;
            }

            bit_vector |= 1 << i;
        }

        bit_vector
    }

    pub fn create_indexes_from_bit_vector(&mut self, bit_vector: usize) {
        for idx in 0..self.indices.len() {
            if (1 << idx) & bit_vector != 0 {
                self.create_index(idx);
            }
        }
    }

    pub fn update_index(&mut self, column_number: usize, value: u64, rid: RID) {
        if let Some(ref mut index) = self.indices[column_number] {
            if let Some(ref mut rids) = index.get_mut(&value) {
                rids.push(rid);
            } else {
                let mut vec = Vec::with_capacity(4);
                vec.push(rid);
                index.insert(value, vec);
            }
        }
    }

    pub fn remove_index(&mut self, column_number: usize, value: u64, rid: RID) {
        if let Some(ref mut index) = self.indices[column_number] {
            if let Some(ref mut rids) = index.get_mut(&value) {
                rids.retain(|x| x.raw() != rid.raw());
            }
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

    pub fn range_from_index(
        &self,
        column_number: usize,
        range: impl RangeBounds<u64>,
    ) -> Option<Vec<RID>> {
        self.indices[column_number].as_ref().map(|map| {
            map.range(range)
                .flat_map(|item| item.1.clone())
                .collect::<Vec<RID>>()
        })
    }

    pub fn create_index(&mut self, column_number: usize) {
        self.indices[column_number] = Some(BTreeMap::new());
    }

    pub fn drop_index(&mut self, column_number: usize) {
        self.indices[column_number] = None;
    }
}
