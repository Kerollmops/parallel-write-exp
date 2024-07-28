use std::mem;
use std::num::NonZeroUsize;

use lru::LruCache;
use roaring::RoaringBitmap;
use sled::Tree;
use smallvec::SmallVec;
use tempfile::TempDir;

use crate::del_add::{DelAdd, KvReaderDelAdd, KvWriterDelAdd};

pub(crate) struct TempDatabase {
    pub(crate) db: sled::Db,
    pub(crate) tempdir: TempDir,
    pub(crate) del_add_word_docids: CachedTree,
}

impl TempDatabase {
    pub fn new() -> sled::Result<Self> {
        let tempdir = tempfile::tempdir()?;
        eprintln!("Creating temporary sled folder in {}", tempdir.path().display());
        let db = sled::open(tempdir.path())?;
        let cache_size = NonZeroUsize::new(1000).unwrap();

        let del_add_word_docids = db.open_tree("word-docids")?;
        del_add_word_docids.set_merge_operator(merge_del_add_roaring_bitmap);

        Ok(TempDatabase {
            db,
            tempdir,
            del_add_word_docids: CachedTree::new(cache_size, del_add_word_docids),
        })
    }
}

#[derive(Debug, Clone)]
pub struct CachedTree {
    cache: lru::LruCache<SmallVec<[u8; 20]>, DelAddRoaringBitmap>,
    tree: Tree,
    deladd_buffer: Vec<u8>,
    cbo_buffer: Vec<u8>,
}

impl CachedTree {
    pub fn new(cap: NonZeroUsize, tree: Tree) -> Self {
        CachedTree {
            cache: lru::LruCache::new(cap),
            tree,
            deladd_buffer: Vec::new(),
            cbo_buffer: Vec::new(),
        }
    }
}

impl CachedTree {
    pub fn insert_del_u32(&mut self, key: &[u8], n: u32) -> sled::Result<()> {
        match self.cache.get_mut(key) {
            Some(DelAddRoaringBitmap { del, add: _ }) => {
                del.get_or_insert_with(RoaringBitmap::new).insert(n);
            }
            None => {
                let value = DelAddRoaringBitmap::new_del_u32(n);
                if let Some((key, deladd)) = self.cache.push(key.into(), value) {
                    self.write_entry(key, deladd)?;
                }
            }
        }

        Ok(())
    }

    pub fn insert_del(&mut self, key: &[u8], bitmap: RoaringBitmap) -> sled::Result<()> {
        match self.cache.get_mut(key) {
            Some(DelAddRoaringBitmap { del, add: _ }) => {
                *del.get_or_insert_with(RoaringBitmap::new) |= bitmap;
            }
            None => {
                let value = DelAddRoaringBitmap::new_del(bitmap);
                if let Some((key, deladd)) = self.cache.push(key.into(), value) {
                    self.write_entry(key, deladd)?;
                }
            }
        }

        Ok(())
    }

    pub fn insert_add_u32(&mut self, key: &[u8], n: u32) -> sled::Result<()> {
        match self.cache.get_mut(key) {
            Some(DelAddRoaringBitmap { del: _, add }) => {
                add.get_or_insert_with(RoaringBitmap::new).insert(n);
            }
            None => {
                let value = DelAddRoaringBitmap::new_add_u32(n);
                if let Some((key, deladd)) = self.cache.push(key.into(), value) {
                    self.write_entry(key, deladd)?;
                }
            }
        }

        Ok(())
    }

    pub fn insert_add(&mut self, key: &[u8], bitmap: RoaringBitmap) -> sled::Result<()> {
        match self.cache.get_mut(key) {
            Some(DelAddRoaringBitmap { del: _, add }) => {
                *add.get_or_insert_with(RoaringBitmap::new) |= bitmap;
            }
            None => {
                let value = DelAddRoaringBitmap::new_add(bitmap);
                if let Some((key, deladd)) = self.cache.push(key.into(), value) {
                    self.write_entry(key, deladd)?;
                }
            }
        }

        Ok(())
    }

    pub fn insert_del_add_u32(&mut self, key: &[u8], n: u32) -> sled::Result<()> {
        match self.cache.get_mut(key) {
            Some(DelAddRoaringBitmap { del, add }) => {
                del.get_or_insert_with(RoaringBitmap::new).insert(n);
                add.get_or_insert_with(RoaringBitmap::new).insert(n);
            }
            None => {
                let value = DelAddRoaringBitmap::new_del_add_u32(n);
                if let Some((key, deladd)) = self.cache.push(key.into(), value) {
                    self.write_entry(key, deladd)?;
                }
            }
        }

        Ok(())
    }

    fn write_entry<A: AsRef<[u8]>>(
        &mut self,
        key: A,
        deladd: DelAddRoaringBitmap,
    ) -> sled::Result<()> {
        self.deladd_buffer.clear();
        let mut value_writer = KvWriterDelAdd::new(&mut self.deladd_buffer);
        match deladd {
            DelAddRoaringBitmap { del: Some(del), add: None } => {
                self.cbo_buffer.clear();
                RoaringBitmap::serialize_into(&del, &mut self.cbo_buffer)?;
                value_writer.insert(DelAdd::Deletion, &self.cbo_buffer)?;
            }
            DelAddRoaringBitmap { del: None, add: Some(add) } => {
                self.cbo_buffer.clear();
                RoaringBitmap::serialize_into(&add, &mut self.cbo_buffer)?;
                value_writer.insert(DelAdd::Addition, &self.cbo_buffer)?;
            }
            DelAddRoaringBitmap { del: Some(del), add: Some(add) } => {
                self.cbo_buffer.clear();
                RoaringBitmap::serialize_into(&del, &mut self.cbo_buffer)?;
                value_writer.insert(DelAdd::Deletion, &self.cbo_buffer)?;

                self.cbo_buffer.clear();
                RoaringBitmap::serialize_into(&add, &mut self.cbo_buffer)?;
                value_writer.insert(DelAdd::Addition, &self.cbo_buffer)?;
            }
            DelAddRoaringBitmap { del: None, add: None } => return Ok(()),
        }
        // self.tree.merge(key, value_writer.into_inner().unwrap()).map(drop)
        Ok(())
    }

    pub fn direct_insert(&mut self, key: &[u8], val: &[u8]) -> sled::Result<()> {
        // self.tree.merge(key, val).map(drop)
        Ok(())
    }

    pub fn into_tree(mut self) -> sled::Result<Tree> {
        let default_arc = LruCache::new(NonZeroUsize::MIN);
        for (key, deladd) in mem::replace(&mut self.cache, default_arc) {
            self.write_entry(key, deladd)?;
        }
        Ok(self.tree)
    }
}

#[derive(Debug, Clone)]
pub struct DelAddRoaringBitmap {
    pub del: Option<RoaringBitmap>,
    pub add: Option<RoaringBitmap>,
}

impl DelAddRoaringBitmap {
    fn new_del_add_u32(n: u32) -> Self {
        DelAddRoaringBitmap {
            del: Some(RoaringBitmap::from([n])),
            add: Some(RoaringBitmap::from([n])),
        }
    }

    fn new_del(bitmap: RoaringBitmap) -> Self {
        DelAddRoaringBitmap { del: Some(bitmap), add: None }
    }

    fn new_del_u32(n: u32) -> Self {
        DelAddRoaringBitmap { del: Some(RoaringBitmap::from([n])), add: None }
    }

    fn new_add(bitmap: RoaringBitmap) -> Self {
        DelAddRoaringBitmap { del: None, add: Some(bitmap) }
    }

    fn new_add_u32(n: u32) -> Self {
        DelAddRoaringBitmap { del: None, add: Some(RoaringBitmap::from([n])) }
    }
}

// TODO better use cbo_roaring_bitmaps
fn merge_del_add_roaring_bitmap(
    _key: &[u8],
    old_bytes: Option<&[u8]>,
    new_bytes: &[u8],
) -> Option<Vec<u8>> {
    // safety: Can't we handle errors, here?

    fn union_del_add<'a>(
        out: &'a mut Vec<u8>,
        key: DelAdd,
        old_obkv: &'a KvReaderDelAdd,
        new_obkv: &'a KvReaderDelAdd,
    ) -> &'a [u8] {
        match (old_obkv.get(key), new_obkv.get(key)) {
            (None, None) => {
                RoaringBitmap::new().serialize_into(&mut *out).unwrap();
                out
            }
            (None, Some(new_bytes)) => new_bytes,
            (Some(old_bytes), None) => old_bytes,
            (Some(old_bytes), Some(new_bytes)) => {
                let old = RoaringBitmap::deserialize_unchecked_from(old_bytes).unwrap();
                let new = RoaringBitmap::deserialize_unchecked_from(new_bytes).unwrap();
                let merge = old | new;
                out.reserve(merge.serialized_size());
                merge.serialize_into(&mut *out).unwrap();
                out
            }
        }
    }

    match old_bytes {
        Some(old_bytes) => {
            let old_obkv: &KvReaderDelAdd = old_bytes.into();
            let new_obkv: &KvReaderDelAdd = new_bytes.into();

            let mut output_deladd_obkv = KvWriterDelAdd::memory();

            let mut buffer = Vec::new();
            let del_bytes = union_del_add(&mut buffer, DelAdd::Deletion, old_obkv, new_obkv);
            output_deladd_obkv.insert(DelAdd::Deletion, del_bytes).unwrap();

            buffer.clear();
            let add_bytes = union_del_add(&mut buffer, DelAdd::Addition, old_obkv, new_obkv);
            output_deladd_obkv.insert(DelAdd::Addition, add_bytes).unwrap();

            Some(output_deladd_obkv.into_inner().unwrap())
        }
        None => Some(new_bytes.to_vec()),
    }
}
