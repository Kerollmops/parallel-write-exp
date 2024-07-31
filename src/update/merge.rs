use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::Cursor;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use heed::RoTxn;
use indicatif::{ProgressBar, ProgressStyle};
use memmap2::Mmap;
use obkv::{KvReaderU16, KvWriterU16};
use smallvec::{smallvec, SmallVec};
use smartstring::{Compact, SmartString};
use walkdir::WalkDir;

use crate::documents::DocumentsBatchReader;
use crate::fields_ids_map::FieldsIdsMap;
use crate::main_database::MainDatabase;
use crate::sequential_docids::SequentialDocids;
use crate::DocumentId;

pub type MapDocumentsIdsVersions =
    HashMap<SmartString<Compact>, (DocumentId, SmallVec<[DocumentOffset; 4]>)>;

/// Represents an offset where a document lives
/// in an mmapped grenad reader file.
pub struct DocumentOffset {
    /// The mmapped grenad reader file.
    pub content: Arc<Mmap>,
    /// The offset of the document in the file.
    pub offset: u32,
}

/// Retrieves pointers to the different versions of the documents
/// and associated them to the document ids into an HashMap.
///
/// It also generates the right FieldsIdsMap and returns it.
///
/// This function is only useful when doing update (PATH) of documents and
/// not replacement. For replacements only the latest version of a document
/// is important.
pub fn fetch_update_document_versions(
    rtxn: &RoTxn,
    maindb: &MainDatabase,
    style: ProgressStyle,
    sequential_docids: &mut SequentialDocids,
    update_files_path: PathBuf,
    primary_key: &str,
    mut fields_ids_map: FieldsIdsMap,
) -> anyhow::Result<(MapDocumentsIdsVersions, FieldsIdsMap)> {
    let mut docids_version_offsets = MapDocumentsIdsVersions::new();
    let p =
        ProgressBar::new_spinner().with_style(style).with_message("fetching documents versions");

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

        let reader =
            crate::documents::DocumentsBatchReader::from_reader(Cursor::new(content.as_ref()))?;
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
            match docids_version_offsets.get_mut(document_id) {
                None => {
                    let docid = match maindb.external_documents_ids.get(rtxn, document_id)? {
                        Some(docid) => docid,
                        None => sequential_docids.next().context("no more available docids")?,
                    };
                    docids_version_offsets
                        .insert(document_id.into(), (docid, smallvec![document_offset]));
                }
                Some((_, offsets)) => offsets.push(document_offset),
            }
            offset += 1;
            p.inc(1);
        }

        file_count += 1;
    }

    p.finish_with_message(format!("fetched document versions from {file_count} files"));

    Ok((docids_version_offsets, fields_ids_map))
}

/// Reads the previous version of a document from the database,
/// the new versions in the grenad update files and merges them
/// to generate a new boxed obkv.
///
/// This function is only meant to be used when doing an update
/// and not a replacement.
pub fn merge_document_obkv_for_updates(
    rtxn: &RoTxn,
    // Let's construct the new obkv in memory
    maindb: &MainDatabase,
    fields_ids_map: &FieldsIdsMap,
    internal_id: DocumentId,
    version_offsets: &[DocumentOffset],
) -> anyhow::Result<(DocumentId, Box<KvReaderU16>)> {
    let mut document = BTreeMap::new();
    let original_obkv = maindb.documents.get(rtxn, &internal_id)?;

    if let Some(original_obkv) = original_obkv {
        original_obkv.into_iter().for_each(|(k, v)| {
            document.insert(k, v.to_vec());
        });
    }

    for DocumentOffset { content, offset } in version_offsets.iter().rev() {
        let reader = DocumentsBatchReader::from_reader(Cursor::new(content.as_ref()))?;
        let (mut cursor, batch_index) = reader.into_cursor_and_fields_index();
        let obkv = cursor.get(*offset)?.expect("must exists");

        obkv.into_iter().for_each(|(k, v)| {
            let field_name = batch_index.name(k).unwrap();
            let id = fields_ids_map.id(field_name).unwrap();
            document.insert(id, v.to_vec());
        });
    }

    let mut writer = KvWriterU16::memory();
    document.into_iter().for_each(|(id, value)| writer.insert(id, value).unwrap());
    let boxed = writer.into_inner().unwrap().into_boxed_slice();

    Ok((internal_id, boxed.into())) as anyhow::Result<_>
}
