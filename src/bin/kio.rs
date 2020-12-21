extern crate clap;

use clap::{App, Arg, SubCommand};
use futures::StreamExt;
use kio::{client, logger};
use log::{error, warn};
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{Consumer, DefaultConsumerContext};
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::io::BufRead;
use std::time::Duration;

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
            Arg::with_name("v")
                .short("v")
                .multiple(true)
                .help("Sets the level of verbosity"),
        )
        .subcommand(
            SubCommand::with_name("tail")
                .about("Continuously read from a given set of topics")
                .arg(
                    Arg::with_name("topics")
                        .short("t")
                        .value_name("TOPIC")
                        .multiple(true)
                        .required(true),
                )
                .arg(
                    Arg::with_name("group")
                        .short("g")
                        .value_name("GROUP_ID")
                        .default_value("kio"),
                )
                .arg(
                    Arg::with_name("from")
                        .short("f")
                        .help("Start from offset inclusive"),
                ),
        )
        .subcommand(
            SubCommand::with_name("write")
                .about("Write an NLD set of messages to a given topic")
                .arg(
                    Arg::with_name("topic")
                        .short("t")
                        .value_name("TOPIC")
                        .required(true),
                ),
        )
        .subcommand(
            SubCommand::with_name("read")
                .about("Read a specific range of messages from a given topic")
                .arg(
                    Arg::with_name("topic")
                        .short("t")
                        .value_name("TOPIC")
                        .required(true),
                )
                .arg(
                    Arg::with_name("group")
                        .short("g")
                        .value_name("GROUP_ID")
                        .default_value("kio"),
                )
                .arg(
                    Arg::with_name("from")
                        .short("f")
                        .help("Start from offset inclusive"),
                ),
        )
        .get_matches();

    let brokers: Vec<&str> = matches.values_of("brokers").unwrap().collect();
    let log_level = match matches.occurrences_of("v") {
        0 => RDKafkaLogLevel::Error,
        1 => RDKafkaLogLevel::Warning,
        2 => RDKafkaLogLevel::Info,
        3 => RDKafkaLogLevel::Debug,
        _ => RDKafkaLogLevel::Warning,
    };

    match matches.subcommand() {
        ("tail", Some(tail_m)) => {
            let topics: Vec<&str> = tail_m.values_of("topics").unwrap().collect();
            let group = tail_m.value_of("group").unwrap();
            let config = client::config(group, brokers, log_level);
            tail(config, topics).await;
        }
        ("read", Some(read_m)) => {
            let topic = read_m.value_of("topic").unwrap();
            let group = read_m.value_of("group").unwrap();
            let config = client::config(group, brokers, log_level);
            read(config, topic).await;
        }
        ("write", Some(write_m)) => {
            let topic = write_m.value_of("topic").unwrap();
            let group = write_m.value_of("group").unwrap();
            let config = client::config(group, brokers, log_level);
            write(
                config,
                topic,
                std::io::stdin()
                    .lock()
                    .lines()
                    .filter_map(|l| l.ok()),
            )
            .await;
        }
        _ => {}
    };
}

async fn tail(config: ClientConfig, topics: Vec<&str>) {
    let consumer: StreamConsumer<DefaultConsumerContext> = config
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

async fn read(config: ClientConfig, topic: &str) {
    let consumer: StreamConsumer<DefaultConsumerContext> = config
        .create_with_context(rdkafka::consumer::DefaultConsumerContext)
        .expect("Consumer creation failed");

    consumer
        .seek(
            topic,
            1,
            rdkafka::Offset::from_raw(0),
            std::time::Duration::from_secs(60),
        )
        .expect("Failed to seek to offset");
    consumer
        .subscribe(&[topic])
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

async fn write(config: ClientConfig, topic: &str, messages: impl Iterator<Item = String>) {
    let producer: &FutureProducer = &config.create().expect("Producer creation failed");
    let futures = messages
        .map(|message| {
            async move {
                let record: FutureRecord<String, String> =
                    FutureRecord::to(topic).payload(&message);
                producer.send(record, Duration::from_secs(0)).await
            }
        })
        .collect::<Vec<_>>();

    for future in futures {
        future.await.unwrap();
    }
}
