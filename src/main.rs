use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::Cursor;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use anyhow::Context;
use charabia::{Tokenize, Tokenizer, TokenizerBuilder};
use clap::{Parser, ValueEnum};
use documents::DocumentsBatchReader;
use fields_ids_map::FieldsIdsMap;
use heed::types::Bytes;
use heed::RoTxn;
use indicatif::{ParallelProgressIterator, ProgressBar, ProgressStyle};
use items_pool::ItemsPool;
use main_database::MainDatabase;
use memmap2::Mmap;
use obkv::{KvReaderU16, KvWriterU16};
use rayon::iter::{IntoParallelRefIterator, ParallelBridge, ParallelIterator};
use sequential_docids::SequentialDocids;
use temp_database::{CachedTree, TempDatabase};
use walkdir::WalkDir;

pub type FieldId = u16;
pub type DocumentId = u32;
pub type BEU32 = heed::types::U32<heed::byteorder::BE>;
pub type Object = serde_json::Map<String, serde_json::Value>;

mod concurrent_docids;
mod del_add;
mod documents;
mod fields_ids_map;
mod items_pool;
mod main_database;
mod obkv_codec;
mod roaring_bitmap_codec;
mod sequential_docids;
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
    #[arg(long, default_value = "200GiB")]
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
    let used_document_ids = maindb.document_ids(&rtxn)?;
    let mut sequential_docids = SequentialDocids::new(&used_document_ids);

    let rtxn = maindb.env.read_txn()?;
    let (document_versions, fields_ids_map) = fetch_update_document_versions(
        &maindb,
        &rtxn,
        &mut sequential_docids,
        update_files_path,
        &primary_key,
        fields_ids_map,
    )?;
    eprintln!(
        "Fetching {} document versions and generating missing docids took {:.02?}",
        document_versions.len(),
        before_fetch.elapsed()
    );

    let before_print = Instant::now();
    // This rayon iterator can be cloned and used in multiple threads.
    // Its purpose is to generate the new version of the documents by merging it with the version of the RoTxn.
    let merged_documents = document_versions.par_iter().map_init(
        || maindb.env.read_txn(),
        |result_rtxn, offset| {
            let Ok(rtxn) = result_rtxn else { todo!("handle rtxn issue") };
            merge_document_obkv(rtxn, &maindb, &fields_ids_map, offset)
        },
    );

    let documents_count = document_versions.len() as u64;
    let style = ProgressStyle::with_template("{msg:<15} {wide_bar} {pos}/{len} {eta}").unwrap();

    // TODO Make this number configurable and maybe a multiple of the number of indexing threads.
    let (tree_infos_sender, tree_infos) = crossbeam_channel::unbounded();
    rayon::scope(|s| {
        s.spawn(|_s| {
            // This task is meant to send the documents directly to the LMDB writer.
            // It is the first task because it's the fastest to process.
            let before = Instant::now();
            let p = ProgressBar::new(documents_count)
                .with_style(style.clone())
                .with_message("documents");
            let result = merged_documents.clone().progress_with(p.clone()).try_for_each(|result| {
                let (docid, document) = result?;
                let _ = tree_infos_sender.send(TreeInfo::Document { docid, document });
                Ok(())
            });

            if let Err(error) = result {
                let _ = tree_infos_sender.send(TreeInfo::Error(error));
            }

            p.finish_with_message(format!(
                "Done extracting documents in {:.02?}",
                before.elapsed()
            ));

            let before = Instant::now();
            let word_docids_cache_pool = ItemsPool::new(|| Ok(tempdb.del_add_word_docids.clone()));
            let tokenizers_poll =
                ItemsPool::new(|| Ok(TokenizerBuilder::default().into_tokenizer()));
            let p = ProgressBar::new(documents_count).with_style(style).with_message("word docids");
            let result = merged_documents.clone().progress_with(p.clone()).try_for_each_init(
                || maindb.env.read_txn(),
                |rtxn_result, result| {
                    let Ok(rtxn) = rtxn_result else { todo!("handle rtxn issue") };
                    word_docids_cache_pool.with(|cache| {
                        tokenizers_poll.with(|tokenizer| {
                            let (docid, new_document) = result?;
                            let old_document = maindb.documents.get(rtxn, &docid)?;
                            extract_word_docids(
                                docid,
                                old_document,
                                &new_document,
                                tokenizer,
                                cache,
                            )?;
                            Ok(()) as anyhow::Result<_>
                        })
                    })
                },
            );

            if let Err(error) = result {
                let _ = tree_infos_sender.send(TreeInfo::Error(error));
            }

            let result: sled::Result<Vec<_>> = word_docids_cache_pool
                .into_items()
                .par_bridge()
                .map(|cache| cache.into_tree())
                .collect();

            if let Err(error) = result {
                let _ = tree_infos_sender.send(TreeInfo::Error(error.into()));
            }

            p.finish_with_message(format!(
                "Done extracting word docids in {:.02?}",
                before.elapsed()
            ));

            // WARN You *must* explicitly drop it or it will be dropped too late
            drop(tree_infos_sender);
        });

        eprintln!("I am running...");

        let mut wtxn = maindb.env.write_txn()?;
        for tree_info in tree_infos {
            match tree_info {
                TreeInfo::Document { docid, document } => {
                    maindb.documents.remap_data_type::<Bytes>().put(
                        &mut wtxn,
                        &docid,
                        document.as_bytes(),
                    )?;
                }
                TreeInfo::WordDocids { tree } => todo!(),
                TreeInfo::Error(_) => todo!(),
            }
            rayon::yield_now();
        }
        // wtxn.commit()?;

        eprintln!("I am done...");

        Ok(()) as anyhow::Result<_>
    })?;

    eprintln!("Computing the new documents version took {:.02?}", before_print.elapsed());

    Ok(())
}

fn merge_document_obkv(
    rtxn: &RoTxn,
    maindb: &MainDatabase,
    fields_ids_map: &FieldsIdsMap,
    (_external_id, (internal_id, offsets)): (&String, &(DocumentId, Vec<DocumentOffset>)),
) -> anyhow::Result<(DocumentId, Box<KvReaderU16>)> {
    let mut document = BTreeMap::new();
    let original_obkv = maindb.documents.get(rtxn, internal_id)?;

    if let Some(original_obkv) = original_obkv {
        original_obkv.into_iter().for_each(|(k, v)| {
            document.insert(k, v.to_vec());
        });
    }

    for DocumentOffset { content, offset } in offsets.iter().rev() {
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

    Ok((*internal_id, boxed.into())) as anyhow::Result<_>
}

struct DocumentOffset {
    pub content: Arc<Mmap>,
    pub offset: u32,
}

// TODO use SmallString/SmartString instead, document IDs are short.
// NOTE this is only useful when the update is a PATCH (an update not a replace).
fn fetch_update_document_versions(
    maindb: &MainDatabase,
    rtxn: &RoTxn,
    sequential_docids: &mut SequentialDocids,
    update_files_path: PathBuf,
    primary_key: &str,
    mut fields_ids_map: FieldsIdsMap,
) -> anyhow::Result<(HashMap<String, (DocumentId, Vec<DocumentOffset>)>, FieldsIdsMap)> {
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
                    let docid = match maindb.external_documents_ids.get(&rtxn, document_id)? {
                        Some(docid) => docid,
                        None => sequential_docids.next().context("no more available docids")?,
                    };
                    document_ids_offsets
                        .insert(document_id.to_string(), (docid, vec![document_offset]));
                }
                Some((_, offsets)) => offsets.push(document_offset),
            }
            offset += 1;
        }

        file_count += 1;
    }

    eprintln!("{file_count} files were read to get the content");

    Ok((document_ids_offsets, fields_ids_map))
}

enum TreeInfo {
    Document { docid: DocumentId, document: Box<KvReaderU16> },
    WordDocids { tree: sled::Tree },
    Error(anyhow::Error),
}

// TODO type the CachedTree
fn extract_word_docids(
    docid: u32,
    previous_doc: Option<&KvReaderU16>,
    new_doc: &KvReaderU16,
    tokenizer: &Tokenizer,
    output: &mut CachedTree,
) -> sled::Result<()> {
    if let Some(previous_doc) = previous_doc {
        for (_, v) in previous_doc.iter() {
            // Only manage the direct JSON strings
            if v.first().zip(v.last()) == Some((&b'"', &b'"')) {
                let s = std::str::from_utf8(&v[1..v.len() - 1]).unwrap();
                for token in tokenizer.tokenize(s).filter(|t| t.is_word()) {
                    let key = token.lemma().as_bytes();
                    output.insert_del_u32(key, docid)?;
                }
            }
        }
    }

    for (_, v) in new_doc.iter() {
        // Only manage the direct JSON strings
        if v.first().zip(v.last()) == Some((&b'"', &b'"')) {
            let s = std::str::from_utf8(&v[1..v.len() - 1]).unwrap();
            for token in tokenizer.tokenize(s).filter(|t| t.is_word()) {
                let key = token.lemma().as_bytes();
                output.insert_add_u32(key, docid)?;
            }
        }
    }

    Ok(())
}

fn extract_word_pair_proximity_docids(
    docid: u32,
    previous_doc: Option<&KvReaderU16>,
    new_doc: &KvReaderU16,
    output: &mut CachedTree,
) -> sled::Result<()> {
    todo!()
}
