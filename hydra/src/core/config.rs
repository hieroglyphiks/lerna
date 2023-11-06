use std::boxed::Box;
use std::collections::HashSet;
use std::future::Future;
use std::option::Option;
use std::string::String;
use std::time::SystemTime;

use anyhow::{Error, Result};
use tokio::sync::mpsc::Sender;

use aws_sdk_dynamodb as dynamodb;
use aws_sdk_kinesis as kinesis;

pub struct Callbacks<'a> {
    // a callback which is called when a kinesis event is sent to a downstream client
    // input:
    // ------
    // - the approximate time the record was inserted into kinesis
    // - the time when kinsumer retrieved the record from kinesis
    event_to_client: Option<Box<dyn FnMut(SystemTime, SystemTime) + 'a>>,
    // a callback which is called when the consumer occurs a runtime error
    // input:
    // ------
    // - the runtime error
    runtime_error: Option<Box<dyn FnMut(Error) + 'a>>,
}

impl<'a> Callbacks<'a> {
    pub fn new(
        etc: Option<Box<dyn FnMut(SystemTime, SystemTime) + 'a>>,
        re: Option<Box<dyn FnMut(Error) + 'a>>,
    ) -> Self {
        return Self {
            event_to_client: etc,
            runtime_error: re,
        };
    }
}

pub struct ConsumedRecord {}

pub trait Consumer {
    fn runner(
        &mut self,
        checkpoints_table_name: String,
        kinesis_stream_arn: String,
    ) -> Box<dyn Future<Output = ()> + Send>;
    fn set_shards(&mut self, shard_ids: HashSet<String>) -> Result<()>;
    fn output(&mut self) -> Sender<ConsumedRecord>;
}

pub struct Config {
    // Core configuration options
    pub buffer_size: usize,
    pub kinesis_stream_name: String,
    pub checkpoints_table_name: String,
    pub clients_table_name: String,
    pub application_name: String,

    // Consumer
    pub consumer: Box<dyn Consumer>,

    // AWS Clients
    pub kinesis_client: Box<kinesis::Client>,
    pub dynamodb_client: Box<dynamodb::Client>,
}
