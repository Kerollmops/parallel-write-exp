use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::FieldId;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldsIdsMap {
    names_ids: BTreeMap<String, FieldId>,
    ids_names: BTreeMap<FieldId, String>,
    next_id: Option<FieldId>,
}

impl FieldsIdsMap {
    pub fn new() -> FieldsIdsMap {
        FieldsIdsMap { names_ids: BTreeMap::new(), ids_names: BTreeMap::new(), next_id: Some(0) }
    }

    /// Returns the number of fields ids in the map.
    pub fn len(&self) -> usize {
        self.names_ids.len()
    }

    /// Returns `true` if the map is empty.
    pub fn is_empty(&self) -> bool {
        self.names_ids.is_empty()
    }

    /// Returns the field id related to a field name, it will create a new field id if the
    /// name is not already known. Returns `None` if the maximum field id as been reached.
    pub fn insert(&mut self, name: &str) -> Option<FieldId> {
        match self.names_ids.get(name) {
            Some(id) => Some(*id),
            None => {
                let id = self.next_id?;
                self.next_id = id.checked_add(1);
                self.names_ids.insert(name.to_owned(), id);
                self.ids_names.insert(id, name.to_owned());
                Some(id)
            }
        }
    }

    // /// Get the ids of a field and all its nested fields based on its name.
    // pub fn nested_ids(&self, name: &str) -> Vec<FieldId> {
    //     self.names_ids
    //         .range(name.to_string()..)
    //         .take_while(|(key, _)| key.starts_with(name))
    //         .filter(|(key, _)| crate::is_faceted_by(key, name))
    //         .map(|(_name, id)| *id)
    //         .collect()
    // }

    /// Get the id of a field based on its name.
    pub fn id(&self, name: &str) -> Option<FieldId> {
        self.names_ids.get(name).copied()
    }

    /// Get the name of a field based on its id.
    pub fn name(&self, id: FieldId) -> Option<&str> {
        self.ids_names.get(&id).map(String::as_str)
    }

    /// Remove a field name and id based on its name.
    pub fn remove(&mut self, name: &str) -> Option<FieldId> {
        match self.names_ids.remove(name) {
            Some(id) => self.ids_names.remove_entry(&id).map(|(id, _)| id),
            None => None,
        }
    }

    /// Iterate over the ids and names in the ids order.
    pub fn iter(&self) -> impl Iterator<Item = (FieldId, &str)> {
        self.ids_names.iter().map(|(id, name)| (*id, name.as_str()))
    }

    /// Iterate over the ids in the order of the ids.
    pub fn ids(&'_ self) -> impl Iterator<Item = FieldId> + '_ {
        self.ids_names.keys().copied()
    }

    /// Iterate over the names in the order of the ids.
    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.ids_names.values().map(AsRef::as_ref)
    }
}

impl Default for FieldsIdsMap {
    fn default() -> FieldsIdsMap {
        FieldsIdsMap::new()
    }
}

impl crate::documents::FieldIdMapper for FieldsIdsMap {
    fn id(&self, name: &str) -> Option<FieldId> {
        self.id(name)
    }
}
