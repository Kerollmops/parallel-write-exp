use std::io;

use charabia::Tokenizer;
use obkv::KvReaderU16;

use crate::temp_database::{CachedSorter, DelAddRoaringBitmapMerger};
use crate::DocumentId;

pub fn extract_word_pair_proximity_docids(
    docid: DocumentId,
    previous_doc: Option<&KvReaderU16>,
    new_doc: &KvReaderU16,
    tokenizer: &Tokenizer,
    output: &mut CachedSorter<DelAddRoaringBitmapMerger>,
) -> grenad::Result<(), io::Error> {
    todo!()
}
