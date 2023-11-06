use super::config::{Config, ConsumedRecord};
use anyhow::{anyhow, Context, Result};
use aws_sdk_dynamodb as dynamodb;
use aws_sdk_kinesis as kinesis;
use std::{any, string::String};

pub struct Hydra {
    config: Config,

    kinesis_stream_arn: String,

    consumer_tx: Sender<ConsumedRecord>,
    consumer_rx: Receiver<ConsumedRecord>,
}

impl Hydra {
    pub fn new(c: Config) -> Result<Self> {
        if c.checkpoints_table_name == "" {
            return Err(anyhow!("invalid checkpoints_table_name"));
        }

        if c.clients_table_name == "" {
            return Err(anyhow!("invalid checkpoints_table_name"));
        }

        let (ctx, crx) = channel(c.buffer_size);

        let hydra = Self {
            config: c,

            kinesis_stream_arn: String::from(""),

            consumer_tx: ctx,
            consumer_rx: crx,
        };

        return Ok(hydra);
    }

    // initializes the hydra and returns the kinesis_stream_arn if
    // the initialization was a success
    pub async fn init(&mut self) -> Result<()> {
        // initiate all async calls
        let ddb_client1 = self.config.dynamodb_client.clone();
        let ddb_client2 = self.config.dynamodb_client.clone();
        let kinesis_client = self.config.kinesis_client.clone();

        let checkpoints_table_name = self.config.checkpoints_table_name.clone();
        let clients_table_name = self.config.clients_table_name.clone();
        let kinesis_stream_name = self.config.kinesis_stream_name.clone();

        let checkpoints_table_ready_f = tokio::spawn(async move {
            return ddb_table_ready(ddb_client1, &checkpoints_table_name).await;
        });

        let clients_table_ready_f = tokio::spawn(async move {
            return ddb_table_ready(ddb_client2, &clients_table_name).await;
        });

        let kinesis_stream_ready_f = tokio::spawn(async move {
            return kinesis_stream_ready(kinesis_client, &kinesis_stream_name).await;
        });

        let checkpoints_table_ready = checkpoints_table_ready_f.await.unwrap()?;
        let clients_table_ready = clients_table_ready_f.await.unwrap()?;
        let kinesis_stream_arn = kinesis_stream_ready_f.await.unwrap()?;

        // perform validation + initialization
        if !checkpoints_table_ready {
            return Err(anyhow!("checkpoints table is not ready"));
        }

        if !clients_table_ready {
            return Err(anyhow!("clients table is not ready"));
        }

        if kinesis_stream_arn == "" {
            return Err(anyhow!("invalid empty arn for kinesis stream"));
        } else {
            self.kinesis_stream_arn = kinesis_stream_arn.to_string();
        }

        return Ok(());
    }
}

async fn ddb_table_ready(client: Box<dynamodb::Client>, table_name: &String) -> Result<bool> {
    let request = client.describe_table().table_name(table_name);
    let resp = request.send().await?;

    match resp.table() {
        None => return Err(anyhow!("no dynamodb table found with name ${table_name}")),
        Some(table) => match table.table_status() {
            None => return Err(anyhow!("no table status was found")),
            Some(table_status) => {
                if *table_status != dynamodb::types::TableStatus::Updating
                    && *table_status != dynamodb::types::TableStatus::Active
                {
                    return Ok(false);
                } else {
                    return Ok(true);
                }
            }
        },
    };
}

async fn kinesis_stream_ready(
    client: Box<kinesis::Client>,
    stream_name: &String,
) -> Result<String> {
    let request = client.describe_stream().stream_name(stream_name);
    let resp = request.send().await?;

    match resp.stream_description() {
        None => return Err(anyhow!("no kinesis stream found with name ${stream_name}")),
        Some(stream_description) => {
            if stream_description.stream_status != kinesis::types::StreamStatus::Updating
                && stream_description.stream_status != kinesis::types::StreamStatus::Active
            {
                let stream_status = stream_description.stream_status.as_str();
                return Err(anyhow!(
                    "kinesis stream exists but is in an inactive state: ${stream_status}"
                ));
            } else {
                return Ok(stream_description.stream_arn().to_string());
            }
        }
    };
}
