use anyhow::Result;
use aws_sdk_dynamodb::types::AttributeValue;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_dynamo::{from_item, to_item};
use std::collections::HashMap;
use std::string::String;
use std::vec::Vec;

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ClientRecord {
    pub id: String,
    pub last_update: DateTime<Utc>,
}

impl ClientRecord {
    pub fn from_dynamodb_item(item: HashMap<String, AttributeValue>) -> Result<Self> {
        let record: Self = from_item(item)?;
        return Ok(record);
    }

    pub fn to_dynamodb_item(&mut self) -> Result<HashMap<String, AttributeValue>> {
        let item = to_item(self)?;
        return Ok(item);
    }
}

pub struct ClientRecords(pub Vec<Box<ClientRecord>>);

impl ClientRecords {
    pub fn ids(&mut self) -> Vec<String> {
        let mut ids: Vec<String> = Vec::new();

        for record in self.0.iter() {
            ids.push(record.id.clone());
        }

        return ids;
    }
}
