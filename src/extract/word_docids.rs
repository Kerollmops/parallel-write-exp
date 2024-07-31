use std::io;

use charabia::normalizer::NormalizerOption;
use charabia::{Normalize, Tokenizer};
use obkv::KvReaderU16;

use crate::merge::DelAddRoaringBitmapMerger;
use crate::temp_database::CachedSorter;
use crate::DocumentId;

pub fn extract_word_docids(
    docid: DocumentId,
    previous_doc: Option<&KvReaderU16>,
    new_doc: &KvReaderU16,
    _tokenizer: &Tokenizer,
    output: &mut CachedSorter<DelAddRoaringBitmapMerger>,
) -> grenad::Result<(), io::Error> {
    let normalizer_options = NormalizerOption::default();

    if let Some(previous_doc) = previous_doc {
        for (_, v) in previous_doc.iter() {
            // Only manage the direct JSON strings
            // TODO manage the JSON strings correctly (escaped chars)
            if v.first().zip(v.last()) == Some((&b'"', &b'"')) {
                let s = std::str::from_utf8(&v[1..v.len() - 1]).unwrap();
                // for token in tokenizer.tokenize(s).filter(|t| t.is_word()) {
                //     let key = token.lemma().normalize(&normalizer_options);
                for token in s.split_whitespace() {
                    let key = token.normalize(&normalizer_options);
                    output.insert_del_u32(key.as_bytes(), docid)?;
                }
            }
        }
    }

    for (_, v) in new_doc.iter() {
        // Only manage the direct JSON strings
        // TODO manage the JSON strings correctly (escaped chars)
        if v.first().zip(v.last()) == Some((&b'"', &b'"')) {
            let s = std::str::from_utf8(&v[1..v.len() - 1]).unwrap();
            // for token in tokenizer.tokenize(s).filter(|t| t.is_word()) {
            //     let key = token.lemma().normalize(&normalizer_options);
            for token in s.split_whitespace() {
                let key = token.normalize(&normalizer_options);
                output.insert_add_u32(key.as_bytes(), docid)?;
            }
        }
    }

    Ok(())
}
