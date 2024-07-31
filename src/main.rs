use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::{Cursor, Seek};
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use anyhow::Context;
use charabia::TokenizerBuilder;
use clap::{Parser, ValueEnum};
use codec::CboRoaringBitmapCodec;
use del_add::DelAdd;
use documents::DocumentsBatchReader;
use fields_ids_map::FieldsIdsMap;
use grenad::{MergerBuilder, Reader, Sorter};
use heed::types::Bytes;
use heed::{Database, PutFlags, RoTxn};
use indicatif::{MultiProgress, ParallelProgressIterator, ProgressBar, ProgressStyle};
use items_pool::ItemsPool;
use main_database::MainDatabase;
use memmap2::Mmap;
use merge::DelAddRoaringBitmapMerger;
use obkv::{KvReader, KvReaderU16, KvWriterU16};
use rayon::iter::{IntoParallelRefIterator, ParallelBridge, ParallelIterator};
use roaring::RoaringBitmap;
use sequential_docids::SequentialDocids;
use temp_database::CachedSorter;
use walkdir::WalkDir;

pub type FieldId = u16;
pub type DocumentId = u32;
pub type KvReaderDelAdd = KvReader<DelAdd>;
pub type BEU32 = heed::types::U32<heed::byteorder::BE>;
pub type Object = serde_json::Map<String, serde_json::Value>;

const LRU_CACHE_SIZE: NonZeroUsize = match NonZeroUsize::new(10_000) {
    Some(v) => v,
    None => NonZeroUsize::MIN,
};

mod codec;
mod concurrent_docids;
mod del_add;
mod documents;
mod extract;
mod fields_ids_map;
mod items_pool;
mod main_database;
mod merge;
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

    let before_fetch = Instant::now();
    let rtxn = maindb.env.read_txn()?;
    let put_flag =
        if maindb.documents.is_empty(&rtxn)? { PutFlags::APPEND } else { PutFlags::empty() };
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
    if documents_count == 0 {
        return Ok(());
    }

    let style =
        ProgressStyle::with_template("{msg:<25} [{elapsed}] {wide_bar} {pos}/{len} {eta}").unwrap();
    let (tree_infos_sender, tree_infos) = crossbeam_channel::unbounded();

    rayon::scope(|s| {
        s.spawn(|_s| {
            // This task is meant to send the documents directly to the LMDB writer.
            // It is the first task because it's the fastest to process.
            let documents_progress = ProgressBar::new(documents_count)
                .with_style(style.clone())
                .with_message("extract documents");
            let result = merged_documents
                .clone()
                .progress_with(documents_progress.clone())
                .try_for_each(|result| {
                    let (docid, document) = result?;
                    let _ = tree_infos_sender.send(TreeInfo::Document { docid, document });
                    Ok(())
                });

            if let Err(error) = result {
                let _ = tree_infos_sender.send(TreeInfo::Error(error));
            }

            documents_progress.finish_with_message("documents extracted");

            let word_docids_progress = ProgressBar::new(documents_count)
                .with_style(style.clone())
                .with_message("extract word docids");
            let context_pool = ItemsPool::new(|| {
                Ok((
                    maindb.env.read_txn()?,
                    TokenizerBuilder::default().into_tokenizer(),
                    CachedSorter::new(LRU_CACHE_SIZE, Sorter::new(DelAddRoaringBitmapMerger)),
                ))
            });
            let result = merged_documents
                .clone()
                .progress_with(word_docids_progress.clone())
                .try_for_each(|result| {
                    context_pool.with(|(rtxn, tokenizer, cache)| {
                        let (docid, new_document) = result?;
                        let old_document = maindb.documents.get(rtxn, &docid)?;
                        extract::extract_word_docids(
                            docid,
                            old_document,
                            &new_document,
                            tokenizer,
                            cache,
                        )?;
                        Ok(()) as anyhow::Result<_>
                    })
                });

            word_docids_progress.finish_with_message("word docids extracted");

            if let Err(error) = result {
                let _ = tree_infos_sender.send(TreeInfo::Error(error));
            }

            let dump_word_docids_progress = ProgressBar::new_spinner()
                .with_style(style.clone())
                .with_message("dump word docids");
            let result: anyhow::Result<Vec<_>> = context_pool
                .into_items()
                .par_bridge()
                .progress_with(dump_word_docids_progress.clone())
                .map(|(_rtxn, _tokenizer, cache)| {
                    let sorter = cache.into_sorter()?;
                    sorter.into_reader_cursors().map_err(Into::into)
                })
                .collect();

            dump_word_docids_progress.finish_with_message("word docids cache dumped");

            let rtxn = maindb.env.read_txn().unwrap();
            let word_docids = maindb.word_docids.remap_key_type::<Bytes>();
            let _ = tree_infos_sender.send(match result {
                Ok(reader_cursors) => {
                    let merging_writing_word_docids_progress = ProgressBar::new_spinner()
                        .with_style(style.clone())
                        .with_message("merging & writing word docids");

                    // TODO make this multithreaded
                    let mut merger_builder = MergerBuilder::new(DelAddRoaringBitmapMerger);
                    for reader_cursors in reader_cursors {
                        merger_builder.extend(reader_cursors);
                    }
                    let merger = merger_builder.build();

                    let mut writer = tempfile::tempfile().map(grenad::Writer::new).unwrap();
                    let mut buffer = Vec::new();
                    let mut iter = merger.into_stream_merger_iter().unwrap();
                    while let Some((key, val)) = iter.next().unwrap() {
                        buffer.clear();
                        match merge_del_add_with_cbo_roaring_bitmap(
                            word_docids,
                            &rtxn,
                            key,
                            val.into(),
                            &mut buffer,
                        )
                        .unwrap()
                        {
                            Some(bitmap_bytes) => writer.insert(key, bitmap_bytes).unwrap(),
                            None => continue,
                        }
                        merging_writing_word_docids_progress.inc(1);
                        merging_writing_word_docids_progress.tick();
                    }

                    // merger.write_into_stream_writer(&mut writer).unwrap();
                    let mut file = writer.into_inner().unwrap();
                    file.rewind().unwrap();
                    let reader = Reader::new(file).unwrap();

                    merging_writing_word_docids_progress.finish_with_message("word docids merged");

                    TreeInfo::WordDocids { reader }
                }
                Err(error) => TreeInfo::Error(error),
            });

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
                TreeInfo::WordDocids { reader } => {
                    let p = ProgressBar::new(reader.len())
                        .with_style(style.clone())
                        .with_message("writing word docids");
                    let mut cursor = reader.into_cursor()?;
                    while let Some((key, value)) = cursor.move_on_next()? {
                        if key.len() <= 511 && !key.is_empty() {
                            maindb
                                .word_docids
                                .remap_types::<Bytes, Bytes>()
                                .put_with_flags(&mut wtxn, put_flag, key, value)?;
                            p.inc(1);
                        }
                    }
                    p.finish_with_message("word docids written");
                }
                TreeInfo::Error(error) => return Err(error),
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

fn merge_del_add_with_cbo_roaring_bitmap<'b>(
    word_docids: Database<Bytes, CboRoaringBitmapCodec>,
    rtxn: &RoTxn,
    key: &[u8],
    del_add: &KvReader<DelAdd>,
    buffer: &'b mut Vec<u8>,
) -> heed::Result<Option<&'b [u8]>> {
    if key.is_empty() {
        return Ok(None);
    }
    match word_docids.get(rtxn, key).unwrap() {
        Some(mut previous_bitmap) => {
            if let Some(bitmap_bytes) = del_add.get(DelAdd::Deletion) {
                let bitmap = RoaringBitmap::deserialize_unchecked_from(bitmap_bytes).unwrap();
                previous_bitmap -= bitmap;
            }
            if let Some(bitmap_bytes) = del_add.get(DelAdd::Addition) {
                let bitmap = RoaringBitmap::deserialize_unchecked_from(bitmap_bytes).unwrap();
                previous_bitmap |= bitmap;
            }
            Ok(Some(CboRoaringBitmapCodec::serialize_into(&previous_bitmap, buffer)))
        }
        None => match del_add.get(DelAdd::Addition) {
            Some(bitmap_bytes) => {
                // TODO introduce a CboRoaringBitmap::reencode method
                //      that will or not reencode the bitmap, depending on the length
                let bitmap = RoaringBitmap::deserialize_unchecked_from(bitmap_bytes).unwrap();
                Ok(Some(CboRoaringBitmapCodec::serialize_into(&bitmap, buffer)))
            }
            None => Ok(None),
        },
    }
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
    WordDocids { reader: grenad::Reader<File> },
    Error(anyhow::Error),
}
