use std::fs::File;

use grenad::{MergerBuilder, Reader, ReaderCursor};
use heed::types::Bytes;
use heed::{Database, RoTxn};
use indicatif::{ProgressBar, ProgressStyle};
use roaring::RoaringBitmap;

use crate::codec::CboRoaringBitmapCodec;
use crate::del_add::DelAdd;
use crate::main_database::MainDatabase;
use crate::merge::DelAddRoaringBitmapMerger;
use crate::KvReaderDelAdd;

/// Merge the word docids reader cursors that comes from
/// the different threads into a single ordered grenad reader.
///
/// Returns a single `grenad::Reader<File>` containing the extracted word docids.
/// The keys are utf8 words and KvReaderDelAdd with RoaringBitmaps as values.
///
/// This *must be multitheaded* to increase the CPU usage:
///   1. We could loose the ordering when doing that and therefore
///      not being able to use APPEND with LMDB
///   2. We are currently writing a lot on disk just to merge the values
///      into a single ordered grenad file. We could try merging the values
///      in parallel to the writer thread (LMDB is fast).
pub fn merge_word_docids_cursors_into_reader(
    style: ProgressStyle,
    maindb: &MainDatabase,
    reader_cursors: Vec<ReaderCursor<File>>,
) -> anyhow::Result<Reader<File>> {
    let p =
        ProgressBar::new_spinner().with_style(style).with_message("merging & writing word docids");

    let mut merger_builder = MergerBuilder::new(DelAddRoaringBitmapMerger);
    merger_builder.extend(reader_cursors);
    let merger = merger_builder.build();

    let rtxn = maindb.env.read_txn()?;
    let word_docids = maindb.word_docids.remap_key_type::<Bytes>();

    let mut writer = tempfile::tempfile().map(grenad::Writer::new)?;
    let mut buffer = Vec::new();

    let mut iter = merger.into_stream_merger_iter()?;
    while let Some((key, val)) = iter.next()? {
        buffer.clear();

        let result = merge_del_add_with_cbo_roaring_bitmap(
            word_docids,
            &rtxn,
            key,
            val.into(),
            &mut buffer,
        )?;

        match result {
            Some(bitmap_bytes) => writer.insert(key, bitmap_bytes)?,
            None => continue,
        }
        p.inc(1);
        p.tick();
    }

    p.finish_with_message("word docids merged");

    let file = writer.into_inner()?;
    Reader::new(file).map_err(Into::into)
}

fn merge_del_add_with_cbo_roaring_bitmap<'b>(
    word_docids: Database<Bytes, CboRoaringBitmapCodec>,
    rtxn: &RoTxn,
    key: &[u8],
    del_add: &KvReaderDelAdd,
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
