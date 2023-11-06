use std::time::SystemTime;
use std::vec::Vec;

pub struct ClientRecord {
    id: String,
    last_update: SystemTime,
}

pub struct ClientRecords(Vec<ClientRecord>);

impl ClientRecords {
    pub fn ids(&mut self) -> Vec<String> {
        let mut ids: Vec<String> = Vec::new();

        for record in self.0.iter() {
            ids.push(record.id.clone());
        }

        return ids;
    }
}
