use std::path::Path;

use heed::types::{SerdeJson, Str};
use heed::{Database, Env, EnvOpenOptions, RoTxn, Unspecified};
use roaring::RoaringBitmap;

use crate::fields_ids_map::FieldsIdsMap;
use crate::obkv_codec::ObkvCodec;
use crate::roaring_bitmap_codec::RoaringBitmapCodec;
use crate::BEU32;

pub(crate) struct MainDatabase {
    pub(crate) env: Env,

    /// Contains many different types (e.g. the fields ids map).
    pub(crate) main: Database<Unspecified, Unspecified>,

    /// Maps the external documents ids with the internal document id.
    pub external_documents_ids: Database<Str, BEU32>,

    /// Maps the document id to the document as an obkv store.
    pub(crate) documents: Database<BEU32, ObkvCodec>,
    // pub(crate) word_docids: Database<Str, RoaringBitmapCodec>,
}

impl MainDatabase {
    pub fn open(path: &Path, size: usize) -> heed::Result<Self> {
        let env = unsafe { EnvOpenOptions::new().map_size(size).max_dbs(10).open(path)? };

        let mut wtxn = env.write_txn()?;
        let main = env.create_database(&mut wtxn, Some("main"))?;
        let external_documents_ids =
            env.create_database(&mut wtxn, Some("external-documents-ids"))?;
        let documents = env.create_database(&mut wtxn, Some("documents"))?;
        // let word_docids = env.create_database(&mut wtxn, Some("word-docids"))?;
        wtxn.commit()?;

        Ok(MainDatabase { env, main, external_documents_ids, documents })
    }

    pub fn document_ids(&self, rtxn: &RoTxn) -> heed::Result<RoaringBitmap> {
        Ok(self
            .main
            .remap_types::<Str, RoaringBitmapCodec>()
            .get(rtxn, "document-ids")?
            .unwrap_or_default())
    }

    pub fn fields_ids_map(&self, rtxn: &RoTxn<'_>) -> heed::Result<FieldsIdsMap> {
        Ok(self
            .main
            .remap_types::<Str, SerdeJson<FieldsIdsMap>>()
            .get(rtxn, "fields-ids-map")?
            .unwrap_or_default())
    }

    // /// Writes all the entries from the sled tree into LMDB. It will erase them is they already exists.
    // ///
    // /// Returns the number of entries written. TODO return the number of bytes returned?
    // pub fn write_sled_tree(&self, wtxn: &mut RwTxn, tree_info: TreeInfo) -> heed::Result<usize> {
    //     // ...
    //     todo!()
    // }
}
