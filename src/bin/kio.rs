extern crate clap;

use clap::{App, Arg, SubCommand};
use kio::{client, logger};
use log::{error, warn};
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::base_consumer::BaseConsumer;
use rdkafka::consumer::{Consumer, DefaultConsumerContext};
use rdkafka::message::Message;
// use rdkafka::producer::base_producer::BaseProducer;
// use std::io::BufRead;
use std::time::Duration;

pub fn main() {
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
            Arg::with_name("verbose")
                .short("v")
                .multiple(true)
                .help("Sets the level of verbosity"),
        )
        .arg(
            Arg::with_name("group")
                .short("g")
                .value_name("GROUP_ID")
                .default_value("kio"),
        )
        .arg(
            Arg::with_name("interval")
                .short("i")
                .value_name("POLL_INTERVAL")
                .default_value("5")
                .help("Interval in seconds to poll for new events"),
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
                    Arg::with_name("to")
                        .short("f")
                        .alias("e")
                        .help("End offset inclusive"),
                )
                .arg(
                    Arg::with_name("from")
                        .short("s")
                        .min_values(0)
                        .help("Starting offset inclusive"),
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

    let group = matches.value_of("group").unwrap();
    let config = client::config(group, brokers, log_level);
    let interval = matches
        .value_of("interval")
        .unwrap()
        .parse()
        .expect("from must be an integer");
    dbg!(matches.subcommand_name());

    match matches.subcommand() {
        ("tail", Some(tail_m)) => {
            let topics: Vec<&str> = tail_m.values_of("topics").unwrap().collect();
            tail(config, topics, interval);
        }
        ("read", Some(read_m)) => {
            let topic = read_m.value_of("topic").unwrap();
            let from = read_m
                .value_of("from")
                .unwrap()
                .parse()
                .expect("from must be an integer");
            let to = read_m
                .value_of("to")
                .unwrap()
                .parse()
                .expect("to must be an integer");
            read(config, topic, interval, from, to);
        }
        ("write", Some(_write_m)) => {
            // let topic = write_m.value_of("topic").unwrap();
            // write(
            //     config,
            //     topic,
            //     std::io::stdin().lock().lines().filter_map(|l| l.ok()),
            // );
        }
        _ => {}
    };
}

fn tail(config: ClientConfig, topics: Vec<&str>, interval: u64) {
    let consumer: BaseConsumer<DefaultConsumerContext> = config
        .create_with_context(rdkafka::consumer::DefaultConsumerContext)
        .expect("Consumer creation failed");

    consumer
        .subscribe(&topics)
        .expect("Can't subscribe to specified topics");

    loop {
        match consumer.poll(Duration::from_secs(interval)) {
            Some(message) => match message {
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
            },
            _ => {}
        }
    }
}

fn read(config: ClientConfig, topic: &str, interval: u64, from: i64, to: i64) {
    let consumer: BaseConsumer<DefaultConsumerContext> = config
        .create_with_context(rdkafka::consumer::DefaultConsumerContext)
        .expect("Consumer creation failed");

    consumer
        .subscribe(&[topic])
        .expect("Can't subscribe to specified topics");

    consumer
        .seek(
            topic,
            1,
            rdkafka::Offset::from_raw(from),
            std::time::Duration::from_secs(60),
        )
        .expect("Failed to seek to offset");

    loop {
        match consumer.poll(Duration::from_secs(interval)) {
            Some(message) => match message {
                Err(e) => warn!("Kafka error: {}", e),
                Ok(m) => {
                    if m.offset() > to {
                        break;
                    }
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
                        Err(err) => eprintln!("Failed to parse JSON body: {}", err),
                    };
                }
            },
            _ => {}
        }
    }
}

// fn write(config: ClientConfig, topic: &str, messages: impl Iterator<Item = String>) {
//     let producer: &BaseProducer = &config.create().expect("Producer creation failed");
// let futures = messages
//     .map(|message| {
//             let record: FutureRecord<String, String> =
//                 FutureRecord::to(topic).payload(&message);
//             producer.send(record, Duration::from_secs(0)).await
//     })
// }
