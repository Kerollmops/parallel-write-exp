use std::io::{self, Write};

use grenad::{CompressionType, WriterBuilder};
use serde::de::Deserializer;
use serde_json::{to_writer, Value};

use super::{DocumentsBatchIndex, Error, DOCUMENTS_BATCH_INDEX_KEY};
use crate::documents::serde_impl::DocumentVisitor;
use crate::Object;

/// The `DocumentsBatchBuilder` provides a way to build a documents batch in the intermediary
/// format used by milli.
///
/// The writer used by the `DocumentsBatchBuilder` can be read using a `DocumentsBatchReader`
/// to iterate over the documents.
///
/// ## example:
/// ```
/// use serde_json::json;
/// use milli::documents::DocumentsBatchBuilder;
///
/// let json = json!({ "id": 1, "name": "foo" });
///
/// let mut builder = DocumentsBatchBuilder::new(Vec::new());
/// builder.append_json_object(json.as_object().unwrap()).unwrap();
/// let _vector = builder.into_inner().unwrap();
/// ```
pub struct DocumentsBatchBuilder<W> {
    /// The inner grenad writer, the last value must always be the `DocumentsBatchIndex`.
    writer: grenad::Writer<W>,
    /// A map that creates the relation between field ids and field names.
    fields_index: DocumentsBatchIndex,
    /// The number of documents that were added to this builder,
    /// it doesn't take the primary key of the documents into account at this point.
    documents_count: u32,

    /// A buffer to store a temporary obkv buffer and avoid reallocating.
    obkv_buffer: Vec<u8>,
    /// A buffer to serialize the values and avoid reallocating,
    /// serialized values are stored in an obkv.
    value_buffer: Vec<u8>,
}

impl<W: Write> DocumentsBatchBuilder<W> {
    pub fn new(writer: W) -> DocumentsBatchBuilder<W> {
        DocumentsBatchBuilder {
            writer: WriterBuilder::new().compression_type(CompressionType::None).build(writer),
            fields_index: DocumentsBatchIndex::default(),
            documents_count: 0,
            obkv_buffer: Vec::new(),
            value_buffer: Vec::new(),
        }
    }

    /// Returns the number of documents inserted into this builder.
    pub fn documents_count(&self) -> u32 {
        self.documents_count
    }

    /// Appends a new JSON object into the batch and updates the `DocumentsBatchIndex` accordingly.
    pub fn append_json_object(&mut self, object: &Object) -> io::Result<()> {
        // Make sure that we insert the fields ids in order as the obkv writer has this requirement.
        let mut fields_ids: Vec<_> = object.keys().map(|k| self.fields_index.insert(k)).collect();
        fields_ids.sort_unstable();

        self.obkv_buffer.clear();
        let mut writer = obkv::KvWriter::new(&mut self.obkv_buffer);
        for field_id in fields_ids {
            let key = self.fields_index.name(field_id).unwrap();
            self.value_buffer.clear();
            to_writer(&mut self.value_buffer, &object[key])?;
            writer.insert(field_id, &self.value_buffer)?;
        }

        let internal_id = self.documents_count.to_be_bytes();
        let document_bytes = writer.into_inner()?;
        self.writer.insert(internal_id, &document_bytes)?;
        self.documents_count += 1;

        Ok(())
    }

    /// Appends a new JSON array of objects into the batch and updates the `DocumentsBatchIndex` accordingly.
    pub fn append_json_array<R: io::Read>(&mut self, reader: R) -> Result<(), Error> {
        let mut de = serde_json::Deserializer::from_reader(reader);
        let mut visitor = DocumentVisitor::new(self);
        de.deserialize_any(&mut visitor)?
    }

    /// Appends a new CSV file into the batch and updates the `DocumentsBatchIndex` accordingly.
    pub fn append_csv<R: io::Read>(&mut self, mut reader: csv::Reader<R>) -> Result<(), Error> {
        // Make sure that we insert the fields ids in order as the obkv writer has this requirement.
        let mut typed_fields_ids: Vec<_> = reader
            .headers()?
            .into_iter()
            .map(parse_csv_header)
            .map(|(k, t)| (self.fields_index.insert(k), t))
            .enumerate()
            .collect();
        // Make sure that we insert the fields ids in order as the obkv writer has this requirement.
        typed_fields_ids.sort_unstable_by_key(|(_, (fid, _))| *fid);

        let mut record = csv::StringRecord::new();
        let mut line = 0;
        while reader.read_record(&mut record)? {
            // We increment here and not at the end of the while loop to take
            // the header offset into account.
            line += 1;

            self.obkv_buffer.clear();
            let mut writer = obkv::KvWriter::new(&mut self.obkv_buffer);

            for (i, (field_id, type_)) in typed_fields_ids.iter() {
                self.value_buffer.clear();

                let value = &record[*i];
                let trimmed_value = value.trim();
                match type_ {
                    AllowedType::Number => {
                        if trimmed_value.is_empty() {
                            to_writer(&mut self.value_buffer, &Value::Null)?;
                        } else if let Ok(integer) = trimmed_value.parse::<i64>() {
                            to_writer(&mut self.value_buffer, &integer)?;
                        } else {
                            match trimmed_value.parse::<f64>() {
                                Ok(float) => {
                                    to_writer(&mut self.value_buffer, &float)?;
                                }
                                Err(error) => {
                                    return Err(Error::ParseFloat {
                                        error,
                                        line,
                                        value: value.to_string(),
                                    });
                                }
                            }
                        }
                    }
                    AllowedType::Boolean => {
                        if trimmed_value.is_empty() {
                            to_writer(&mut self.value_buffer, &Value::Null)?;
                        } else {
                            match trimmed_value.parse::<bool>() {
                                Ok(bool) => {
                                    to_writer(&mut self.value_buffer, &bool)?;
                                }
                                Err(error) => {
                                    return Err(Error::ParseBool {
                                        error,
                                        line,
                                        value: value.to_string(),
                                    });
                                }
                            }
                        }
                    }
                    AllowedType::String => {
                        if value.is_empty() {
                            to_writer(&mut self.value_buffer, &Value::Null)?;
                        } else {
                            to_writer(&mut self.value_buffer, value)?;
                        }
                    }
                }

                // We insert into the obkv writer the value buffer that has been filled just above.
                writer.insert(*field_id, &self.value_buffer)?;
            }

            let internal_id = self.documents_count.to_be_bytes();
            let document_bytes = writer.into_inner()?;
            self.writer.insert(internal_id, &document_bytes)?;
            self.documents_count += 1;
        }

        Ok(())
    }

    /// Flushes the content on disk and stores the final version of the `DocumentsBatchIndex`.
    pub fn into_inner(mut self) -> io::Result<W> {
        let DocumentsBatchBuilder { mut writer, fields_index, .. } = self;

        // We serialize and insert the `DocumentsBatchIndex` as the last key of the grenad writer.
        self.value_buffer.clear();
        to_writer(&mut self.value_buffer, &fields_index)?;
        writer.insert(DOCUMENTS_BATCH_INDEX_KEY, &self.value_buffer)?;

        writer.into_inner()
    }
}

#[derive(Debug)]
enum AllowedType {
    String,
    Boolean,
    Number,
}

fn parse_csv_header(header: &str) -> (&str, AllowedType) {
    // if there are several separators we only split on the last one.
    match header.rsplit_once(':') {
        Some((field_name, field_type)) => match field_type {
            "string" => (field_name, AllowedType::String),
            "boolean" => (field_name, AllowedType::Boolean),
            "number" => (field_name, AllowedType::Number),
            // if the pattern isn't recognized, we keep the whole field.
            _otherwise => (header, AllowedType::String),
        },
        None => (header, AllowedType::String),
    }
}
