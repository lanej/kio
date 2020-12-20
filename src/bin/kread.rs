extern crate clap;

use clap::{App, Arg};
use futures::StreamExt;
use kio::logger;
use log::{error, warn};
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{Consumer, DefaultConsumerContext};
use rdkafka::message::Message;

#[tokio::main]
pub async fn main() {
    logger::logger(false, None);
    let matches = App::new("kread")
        .version("0.1")
        .author("Josh Lane <me@joshualane.com>")
        .about("Spew Kafka messages to stdout")
        .arg(
            Arg::with_name("brokers")
                .short("b")
                .value_name("host[:port]")
                .default_value("localhost:9092")
                .multiple(true)
                .help("Broker URI authority"),
        )
        .arg(
            Arg::with_name("from")
                .short("f")
                .help("Start from offset inclusive"),
        )
        .arg(
            Arg::with_name("v")
                .short("v")
                .multiple(true)
                .help("Sets the level of verbosity"),
        )
        .arg(
            Arg::with_name("topics")
                .short("t")
                .value_name("foo")
                .multiple(true)
                .required(true),
        )
        .get_matches();

    let brokers: Vec<&str> = matches.values_of("brokers").unwrap().collect();
    let topics: Vec<&str> = matches.values_of("topics").unwrap().collect();

    let group_id = "mono";
    let log_level = match matches.occurrences_of("v") {
        0 => RDKafkaLogLevel::Error,
        1 => RDKafkaLogLevel::Warning,
        2 => RDKafkaLogLevel::Info,
        3 => RDKafkaLogLevel::Debug,
        _ => RDKafkaLogLevel::Warning,
    };

    let consumer: StreamConsumer<DefaultConsumerContext> = ClientConfig::new()
        .set("group.id", group_id)
        .set("debug", "all")
        .set("bootstrap.servers", brokers.join(",").as_str())
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("auto.offset.reset", "smallest")
        .set_log_level(log_level)
        .create_with_context(rdkafka::consumer::DefaultConsumerContext)
        .expect("Consumer creation failed");

    consumer
        .subscribe(&topics)
        .expect("Can't subscribe to specified topics");

    let mut message_stream = consumer.start();

    while let Some(message) = message_stream.next().await {
        match message {
            Err(e) => warn!("Kafka error: {}", e),
            Ok(m) => {
                let payload = match m.payload_view::<str>() {
                    None => "",
                    Some(Ok(s)) => s,
                    Some(Err(e)) => {
                        warn!("Error while deserializing message payload: {:?}", e);
                        ""
                    }
                };

                match serde_json::from_str::<serde_json::Value>(payload) {
                    Ok(body) => println!("{}", body),
                    Err(err) => error!("Failed to parse JSON body: {}", err),
                };
            }
        };
    }
}
