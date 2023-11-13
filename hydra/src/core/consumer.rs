use super::clients::{ClientRecord, ClientRecords};
use super::config::{Config, ConsumedRecord};
use anyhow::{anyhow, Result};
use aws_sdk_dynamodb as dynamodb;
use aws_sdk_kinesis as kinesis;
use chrono::{Duration, Utc};
use std::ops::{Add, Mul};
use std::string::String;
use tokio::select;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::task::JoinSet;
use tokio::time as tokiotime;
use tokio_util::sync::CancellationToken;

// CONSTANTS
const REGISTRATION_FREQUENCY_SEC: u32 = 5;

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

    pub fn output(&mut self) -> &Receiver<ConsumedRecord> {
        return &self.consumer_rx;
    }

    pub async fn run(&mut self) -> Result<()> {
        let mut tasks = JoinSet::new();

        for i in 0..10 {
            tasks.spawn(async move { return i });
        }

        while let Some(res) = tasks.join_next().await {
            match res {
                Ok(_) => {}
                Err(e) => {
                    tasks.abort_all();
                    return Err(anyhow!("exiting run function with error: ${e}"));
                }
            }
        }

        return Ok(());
    }

    async fn get_clients(&mut self) -> Result<ClientRecords> {
        let ddb_client = self.config.dynamodb_client.clone();
        let request = ddb_client
            .scan()
            .table_name(self.config.clients_table_name.clone())
            .consistent_read(true);

        let response = request.send().await?;
        let items = response.items();

        let mut client_records: ClientRecords = ClientRecords(Vec::new());
        for item in items {
            let client_record_r = ClientRecord::from_dynamodb_item(item.clone());
            match client_record_r {
                Err(_) => {
                    // TODO: callback error
                }
                Ok(client_record) => {
                    // skip stale clients which have not been updated in
                    // three regisration periods
                    if Utc::now().signed_duration_since(client_record.last_update)
                        > Duration::seconds(REGISTRATION_FREQUENCY_SEC as i64).mul(3)
                    {
                        continue;
                    }

                    client_records.0.push(Box::new(client_record));
                }
            }
        }

        // sort the client records for consistent ordering
        // and resulting usability
        client_records.0.sort_by(|a, b| {
            return a.id.cmp(&b.id);
        });

        return Ok(client_records);
    }

    async fn register_client_self(&mut self) -> Result<()> {
        let expiry_time =
            Utc::now().add(Duration::seconds(REGISTRATION_FREQUENCY_SEC as i64).mul(3));

        let request = self
            .config
            .dynamodb_client
            .update_item()
            .table_name(self.config.clients_table_name.clone())
            .update_expression("SET #LU = :LU, #TTL = :TTL")
            .expression_attribute_names("#LU", "LastUpdate")
            .expression_attribute_names("#TTL", "TTL")
            .expression_attribute_values(
                ":LU",
                dynamodb::types::AttributeValue::N(Utc::now().to_rfc3339()),
            )
            .expression_attribute_values(
                ":TTL",
                dynamodb::types::AttributeValue::N(expiry_time.to_rfc3339()),
            )
            .key(
                "ID",
                dynamodb::types::AttributeValue::S(self.config.client_name.clone()),
            );

        match request.send().await {
            Ok(_) => return Ok(()),
            Err(e) => {
                let id = self.config.client_name.clone();
                return Err(anyhow!("failed to update client registry for ${id}: ${e}"));
            }
        }
    }

    async fn deregister_client_self(&mut self) -> Result<()> {
        let request = self
            .config
            .dynamodb_client
            .delete_item()
            .table_name(self.config.clients_table_name.clone())
            .key(
                "ID",
                dynamodb::types::AttributeValue::S(self.config.client_name.clone()),
            );

        match request.send().await {
            Ok(_) => return Ok(()),
            Err(e) => {
                let id = self.config.client_name.clone();
                return Err(anyhow!("failed to delete client registry for ${id}: ${e}"));
            }
        }
    }

    async fn maintain_client_registration_self(
        &mut self,
        cancellation_token: CancellationToken,
    ) -> Result<()> {
        // initial registration
        match self.register_client_self().await {
            Ok(_) => {}
            Err(e) => {
                return Err(anyhow!("failed initial client registration of self: ${e}"));
            }
        }

        let mut registration_timer = tokiotime::interval(tokiotime::Duration::from_secs(
            REGISTRATION_FREQUENCY_SEC.into(),
        ));

        loop {
            select! {
                _ = cancellation_token.cancelled() => {
                    match self.deregister_client_self().await {
                        Ok(_) => {}
                        Err(e) => {
                            // TODO: callback error
                            return Err(anyhow!("failed to deregister self on maintain registration exit: ${e}"));
                        }
                    }
                }
                _ = registration_timer.tick() => {
                    // continuous registration
                    match self.register_client_self().await {
                        Ok(_) => {}
                        Err(_) => {
                            // TODO: callback error
                        }
                    }
                }

            }
        }
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
