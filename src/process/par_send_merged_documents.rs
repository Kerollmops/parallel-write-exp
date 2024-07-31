use crossbeam_channel::Sender;
use indicatif::{ParallelProgressIterator, ProgressBar, ProgressStyle};
use obkv::KvReaderU16;
use rayon::iter::{IntoParallelIterator, ParallelIterator};

use crate::TreeInfo;

/// This task is meant to send the documents directly to the LMDB writer.
///
/// It uses the documents merged parallel iterator that generates the documents,
/// therefore it lazily generates the documents in parallel
/// and send them to the writer thread.
pub fn par_send_merged_documents<I>(
    style: ProgressStyle,
    documents_count: u64,
    documents: I,
    sender: &Sender<TreeInfo>,
) -> anyhow::Result<()>
where
    I: IntoParallelIterator<Item = anyhow::Result<(u32, Box<KvReaderU16>)>>,
{
    let p = ProgressBar::new(documents_count).with_style(style).with_message("extract documents");

    documents.into_par_iter().progress_with(p.clone()).try_for_each(|result| {
        let (docid, document) = result?;
        let _ = sender.send(TreeInfo::Document { docid, document });
        Ok(()) as anyhow::Result<_>
    })?;

    p.finish_with_message("documents extracted");

    Ok(())
}
