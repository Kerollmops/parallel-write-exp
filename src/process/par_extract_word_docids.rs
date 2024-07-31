use std::fs::File;

use charabia::TokenizerBuilder;
use grenad::{ReaderCursor, Sorter};
use indicatif::{ParallelProgressIterator, ProgressBar, ProgressStyle};
use obkv::KvReaderU16;
use rayon::iter::{IntoParallelIterator, ParallelBridge, ParallelIterator};

use crate::items_pool::ItemsPool;
use crate::main_database::MainDatabase;
use crate::merge::DelAddRoaringBitmapMerger;
use crate::temp_database::CachedSorter;
use crate::{extract, LRU_CACHE_SIZE};

/// Extracts the word docids in parallel.
///
/// Returns the set of `grenad::Reader<File>` containing the extracted word docids.
/// The keys are utf8 words and KvReaderDelAdd with RoaringBitmaps as values.
///
/// Waiting for the word docids to be fully processed is advantageous because we only need to do
/// one single write by key in LMDB: no fetch, merge with bitmaps, writes and that possibly multiple times.
/// Reducing the number of writes in LMDB reduces the fragmentation and therefore the size of the final database.
///
/// It uses the documents merged parallel iterator that generates the documents,
/// therefore it lazily generates the documents in parallel to do it's work.
pub fn par_extract_word_docids<I>(
    style: ProgressStyle,
    documents_count: u64,
    maindb: &MainDatabase,
    documents: I,
) -> anyhow::Result<Vec<ReaderCursor<File>>>
where
    I: IntoParallelIterator<Item = anyhow::Result<(u32, Box<KvReaderU16>)>>,
{
    let context_pool = ItemsPool::new(|| {
        Ok((
            maindb.env.read_txn()?,
            TokenizerBuilder::default().into_tokenizer(),
            CachedSorter::new(LRU_CACHE_SIZE, Sorter::new(DelAddRoaringBitmapMerger)),
        ))
    });

    let p = ProgressBar::new(documents_count)
        .with_style(style.clone())
        .with_message("extract word docids");

    documents.into_par_iter().progress_with(p.clone()).try_for_each(|result| {
        context_pool.with(|(rtxn, tokenizer, cache)| {
            let (docid, new_document) = result?;
            let old_document = maindb.documents.get(rtxn, &docid)?;
            extract::extract_word_docids(docid, old_document, &new_document, tokenizer, cache)?;
            Ok(()) as anyhow::Result<_>
        })
    })?;

    p.finish_with_message("word docids extracted");

    let p = ProgressBar::new_spinner().with_style(style).with_message("dump word docids");

    let result: anyhow::Result<Vec<_>> = context_pool
        .into_items()
        .par_bridge()
        .progress_with(p.clone())
        .map(|(_rtxn, _tokenizer, cache)| {
            let sorter = cache.into_sorter()?;
            sorter.into_reader_cursors().map_err(Into::into)
        })
        .collect();

    p.finish_with_message("word docids cache dumped");

    result.map(|v| v.into_iter().flatten().collect())
}
