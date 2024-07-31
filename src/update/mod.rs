mod merge;
mod replace;

pub use merge::{
    fetch_update_document_versions, merge_document_obkv_for_updates, DocumentOffset,
    MapDocumentsIdsVersions,
};
