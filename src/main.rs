use std::fs::File;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::time::Instant;

use clap::{Parser, ValueEnum};
use del_add::DelAdd;
use heed::types::Bytes;
use heed::PutFlags;
use indicatif::{ProgressBar, ProgressStyle};
use main_database::MainDatabase;
use obkv::{KvReader, KvReaderU16};
use process::{
    merge_word_docids_cursors_into_reader, par_extract_word_docids, par_send_merged_documents,
};
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use sequential_docids::SequentialDocids;
use update::{fetch_update_document_versions, merge_document_obkv_for_updates};

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
mod del_add;
mod documents;
mod extract;
mod fields_ids_map;
mod items_pool;
mod main_database;
mod merge;
mod process;
mod sequential_docids;
mod temp_database;
mod update;

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

/// An internal type to send informations directly to the LMDB writer thread.
enum TreeInfo {
    Document { docid: DocumentId, document: Box<KvReaderU16> },
    WordDocids { reader: grenad::Reader<File> },
    Error(anyhow::Error),
}

fn main() -> anyhow::Result<()> {
    let Args { index_path, size, primary_key, operation } = Args::parse();

    let start = Instant::now();
    anyhow::ensure!(operation == DocumentOperation::Update, "Only updates are supported for now");

    let style =
        ProgressStyle::with_template("{msg:<25} [{elapsed}] {wide_bar} {pos}/{len} {eta}").unwrap();

    let update_files_path = {
        let mut path = index_path.clone();
        path.pop();
        path.pop();
        path.join("update_files")
    };
    let maindb = MainDatabase::open(&index_path, size.as_u64() as usize)?;

    let rtxn = maindb.env.read_txn()?;
    let put_flag =
        if maindb.documents.is_empty(&rtxn)? { PutFlags::APPEND } else { PutFlags::empty() };
    let fields_ids_map = maindb.fields_ids_map(&rtxn)?;
    let used_document_ids = maindb.document_ids(&rtxn)?;
    let mut sequential_docids = SequentialDocids::new(&used_document_ids);
    let (document_versions, fields_ids_map) = fetch_update_document_versions(
        &rtxn,
        &maindb,
        style.clone(),
        &mut sequential_docids,
        update_files_path,
        &primary_key,
        fields_ids_map,
    )?;

    let documents_count = document_versions.len() as u64;

    // This rayon iterator can be cloned and used in multiple threads.
    // Its purpose is to generate the new version of the documents by merging it with the version of the RoTxn.
    // TODO prefer using an ItemsPool so that we can return an error if we cannot open an rotxn
    let merged_documents = document_versions.par_iter().map_init(
        || maindb.env.read_txn(),
        |result_rtxn, (_external_id, (internal_id, offsets))| {
            let Ok(rtxn) = result_rtxn else { todo!("handle rtxn issue") };
            merge_document_obkv_for_updates(rtxn, &maindb, &fields_ids_map, *internal_id, offsets)
        },
    );

    let (tree_infos_sender, tree_infos) = crossbeam_channel::unbounded();
    rayon::scope(|s| {
        s.spawn(|_s| {
            if let Err(e) = par_send_merged_documents(
                style.clone(),
                documents_count,
                merged_documents.clone(),
                &tree_infos_sender,
            ) {
                let _ = tree_infos_sender.send(TreeInfo::Error(e));
            }

            let readers_cursors = match par_extract_word_docids(
                style.clone(),
                documents_count,
                &maindb,
                merged_documents.clone(),
            ) {
                Ok(reader_cursors) => reader_cursors,
                Err(e) => {
                    let _ = tree_infos_sender.send(TreeInfo::Error(e));
                    return;
                }
            };

            let _ = match merge_word_docids_cursors_into_reader(
                style.clone(),
                &maindb,
                readers_cursors,
            ) {
                Ok(reader) => tree_infos_sender.send(TreeInfo::WordDocids { reader }),
                Err(error) => tree_infos_sender.send(TreeInfo::Error(error)),
            };

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
                        // TODO improve this
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

    eprintln!("Indexing all the documents took {:.02?}", start.elapsed());

    Ok(())
}
