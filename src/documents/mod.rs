mod builder;
mod enriched;
mod primary_key;
mod reader;
mod serde_impl;

use std::fmt::Debug;
use std::io;
use std::str::Utf8Error;

use anyhow::Context;
use bimap::BiHashMap;
pub use builder::DocumentsBatchBuilder;
pub use primary_key::FieldIdMapper;
pub use reader::{DocumentsBatchCursor, DocumentsBatchCursorError, DocumentsBatchReader};
use serde::{Deserialize, Serialize};

use crate::{FieldId, Object};

/// The key that is used to store the `DocumentsBatchIndex` datastructure,
/// it is the absolute last key of the list.
const DOCUMENTS_BATCH_INDEX_KEY: [u8; 8] = u64::MAX.to_be_bytes();

/// A bidirectional map that links field ids to their name in a document batch.
#[derive(Default, Clone, Debug, Serialize, Deserialize)]
pub struct DocumentsBatchIndex(pub BiHashMap<FieldId, String>);

impl DocumentsBatchIndex {
    /// Insert the field in the map, or return it's field id if it doesn't already exists.
    pub fn insert(&mut self, field: &str) -> FieldId {
        match self.0.get_by_right(field) {
            Some(field_id) => *field_id,
            None => {
                let field_id = self.0.len() as FieldId;
                self.0.insert(field_id, field.to_string());
                field_id
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn iter(&self) -> bimap::hash::Iter<'_, FieldId, String> {
        self.0.iter()
    }

    pub fn name(&self, id: FieldId) -> Option<&str> {
        self.0.get_by_left(&id).map(AsRef::as_ref)
    }

    pub fn id(&self, name: &str) -> Option<FieldId> {
        self.0.get_by_right(name).cloned()
    }

    pub fn recreate_json(&self, document: &obkv::KvReaderU16<'_>) -> anyhow::Result<Object> {
        let mut map = Object::new();

        for (k, v) in document.iter() {
            // TODO: TAMO: update the error type
            let key = self.0.get_by_left(&k).context("not found")?.clone();
            let value = serde_json::from_slice::<serde_json::Value>(v)?;
            map.insert(key, value);
        }

        Ok(map)
    }
}

impl FieldIdMapper for DocumentsBatchIndex {
    fn id(&self, name: &str) -> Option<FieldId> {
        self.id(name)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Error parsing number {value:?} at line {line}: {error}")]
    ParseFloat { error: std::num::ParseFloatError, line: usize, value: String },
    #[error("Error parsing boolean {value:?} at line {line}: {error}")]
    ParseBool { error: std::str::ParseBoolError, line: usize, value: String },
    #[error("Invalid document addition format, missing the documents batch index.")]
    InvalidDocumentFormat,
    #[error("Invalid enriched data.")]
    InvalidEnrichedData,
    #[error(transparent)]
    InvalidUtf8(#[from] Utf8Error),
    #[error(transparent)]
    Csv(#[from] csv::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Serialize(serde_json::Error),
    #[error(transparent)]
    Grenad(#[from] grenad::Error),
    #[error(transparent)]
    Io(#[from] io::Error),
}
