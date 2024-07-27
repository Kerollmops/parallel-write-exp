use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::Cursor;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use anyhow::Context;
use clap::{Parser, ValueEnum};
use documents::DocumentsBatchReader;
use fields_ids_map::FieldsIdsMap;
use main_database::MainDatabase;
use memmap2::Mmap;
use obkv::{KvReaderU16, KvWriterU16};
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use temp_database::{CachedTree, TempDatabase};
use walkdir::{Result, WalkDir};

pub type FieldId = u16;
pub type DocumentId = u32;
pub type BEU32 = heed::types::U32<heed::byteorder::BE>;
pub type Object = serde_json::Map<String, serde_json::Value>;

mod del_add;
mod documents;
mod fields_ids_map;
mod main_database;
mod obkv_codec;
mod roaring_bitmap_codec;
mod temp_database;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Path of the Meilisearch index.
    #[arg(short, long)]
    index_path: PathBuf,

    #[arg(long, default_value = "id")]
    primary_key: String,

    #[arg(long, value_enum)]
    operation: DocumentOperation,

    /// Max size of the database
    #[arg(long, default_value = "20GiB")]
    size: byte_unit::Byte,
}

#[derive(Debug, Default, Clone, ValueEnum, PartialEq, Eq)]
enum DocumentOperation {
    Replace,
    #[default]
    Update,
}

fn main() -> anyhow::Result<()> {
    let Args { index_path, size, primary_key, operation } = Args::parse();

    anyhow::ensure!(operation == DocumentOperation::Update, "Only updates are supported for now");

    let update_files_path = {
        let mut path = index_path.clone();
        path.pop();
        path.pop();
        path.join("update_files")
    };
    let maindb = MainDatabase::open(&index_path, size.as_u64() as usize)?;
    let tempdb = TempDatabase::new()?;

    let before_fetch = Instant::now();
    let rtxn = maindb.env.read_txn()?;
    let fields_ids_map = maindb.fields_ids_map(&rtxn)?;
    let (document_versions, fields_ids_map) =
        fetch_update_document_versions(update_files_path, &primary_key, fields_ids_map)?;
    eprintln!(
        "Fetching {} document versions took {:.02?}",
        document_versions.len(),
        before_fetch.elapsed()
    );

    let before_print = Instant::now();
    document_versions
        .par_iter()
        .map_init(
            || maindb.env.read_txn(),
            |rtxn_result, (id, offsets)| -> anyhow::Result<Box<KvReaderU16>> {
                let Ok(rtxn) = rtxn_result else { todo!("handle rtxn issue") };

                let mut document = BTreeMap::new();
                let original = match maindb.external_documents_ids.get(rtxn, id)? {
                    Some(internal_id) => maindb.documents.get(rtxn, &internal_id)?,
                    None => None,
                };

                if let Some(original) = original {
                    original.into_iter().for_each(|(k, v)| {
                        document.insert(k, v.to_vec());
                    });
                }

                for DocumentOffset { content, offset } in offsets.into_iter().rev() {
                    let reader = DocumentsBatchReader::from_reader(Cursor::new(content.as_ref()))?;
                    let (mut cursor, batch_index) = reader.into_cursor_and_fields_index();
                    let obkv = cursor.get(*offset)?.expect("must exists");

                    obkv.into_iter().for_each(|(k, v)| {
                        let field_name = batch_index.name(k).unwrap();
                        let id = fields_ids_map.id(field_name).unwrap();
                        document.insert(id, v.to_vec());
                    });
                }

                // Let's construct the new obkv in memory
                let mut writer = KvWriterU16::memory();
                document.into_iter().for_each(|(id, value)| writer.insert(id, value).unwrap());
                let boxed = writer.into_inner().unwrap().into_boxed_slice();

                Ok(boxed.into()) as anyhow::Result<_>
            },
        )
        .try_for_each(|result| result.map(drop))?;

    eprintln!("Computing the new documents version took {:.02?}", before_print.elapsed());

    // rayon::scope_fifo(|s| {
    //     s.spawn_fifo(|_s| {
    //         // ...
    //     });
    // });

    // generate_documents()
    //     .take(100)
    //     .par_bridge()
    //     .map_init(
    //         || {
    //             let _guard = scopeguard::guard((), |_| {
    //                 println!("Hello Scope Exit!");
    //             });
    //             Ok((maindb.env.read_txn()?, tempdb.del_add_word_docids.clone(), _guard))
    //                 as heed::Result<_>
    //         },
    //         |result, (docid, text)| {
    //             let Ok((rtxn, ref mut cache, _guard)) = result.as_mut() else {
    //                 todo!("handle the error")
    //             };
    //             let old = maindb.documents.get(rtxn, &docid)?;
    //             extract_word_docids(docid, old, &text, cache)?;

    //             Ok(()) as anyhow::Result<()>
    //         },
    //     )
    //     .for_each(|_| ());

    // let mut wtxn = maindb.env.write_txn()?;
    // for tree_info in tree_infos {
    //     // ...
    // }

    // wtxn.commit()?;

    Ok(())
}

struct DocumentOffset {
    pub content: Arc<Mmap>,
    pub offset: u32,
}

// TODO use SmallString/SmartString instead, document IDs are short.
// NOTE this is only useful when the update is a PATCH (an update not a replace).
fn fetch_update_document_versions(
    update_files_path: PathBuf,
    primary_key: &str,
    mut fields_ids_map: FieldsIdsMap,
) -> anyhow::Result<(HashMap<String, Vec<DocumentOffset>>, FieldsIdsMap)> {
    let mut document_ids_offsets = HashMap::new();

    let mut file_count: usize = 0;
    for result in WalkDir::new(update_files_path)
        // TODO handle errors
        .sort_by_key(|entry| entry.metadata().unwrap().created().unwrap())
    {
        let entry = result?;
        if !entry.file_type().is_file() {
            continue;
        }

        let file = File::open(entry.path())
            .with_context(|| format!("While opening {}", entry.path().display()))?;
        let content = unsafe {
            Mmap::map(&file)
                .map(Arc::new)
                .with_context(|| format!("While memory mapping {}", entry.path().display()))?
        };

        let reader = documents::DocumentsBatchReader::from_reader(Cursor::new(content.as_ref()))?;
        let (mut batch_cursor, batch_index) = reader.into_cursor_and_fields_index();
        batch_index.iter().for_each(|(_, name)| {
            fields_ids_map.insert(name);
        });
        let mut offset: u32 = 0;
        while let Some(document) = batch_cursor.next_document()? {
            let primary_key = batch_index.id(primary_key).unwrap();
            let document_id = document.get(primary_key).unwrap();
            let document_id = std::str::from_utf8(document_id).unwrap();
            let document_offset = DocumentOffset { content: content.clone(), offset };
            match document_ids_offsets.get_mut(document_id) {
                None => {
                    document_ids_offsets.insert(document_id.to_string(), vec![document_offset]);
                }
                Some(offsets) => offsets.push(document_offset),
            }
            offset += 1;
        }

        file_count += 1;
    }

    eprintln!("{file_count} files were read to get the content");

    Ok((document_ids_offsets, fields_ids_map))
}

enum TreeInfo {
    Documents { tree: sled::Tree },
    WordDocids { tree: sled::Tree },
}

// TODO type the CachedTree
fn extract_word_docids(
    docid: u32,
    previous_doc: Option<&str>,
    new_doc: &str,
    output: &mut CachedTree,
) -> sled::Result<()> {
    if let Some(previous_doc) = previous_doc {
        for token in previous_doc.split_whitespace() {
            // TODO use charabia
            let token_normalized = token.to_lowercase();
            output.insert_del_u32(token_normalized.as_bytes(), docid)?;
        }
    }

    for token in new_doc.split_whitespace() {
        let token_normalized = token.to_lowercase();
        output.insert_add_u32(token_normalized.as_bytes(), docid)?;
    }

    Ok(())
}

fn extract_word_pair_proximity_docids(
    docid: u32,
    previous_doc: Option<&str>,
    new_doc: &str,
    output: &mut CachedTree,
) -> sled::Result<()> {
    todo!()
}
