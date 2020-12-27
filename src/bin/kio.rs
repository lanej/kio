extern crate clap;
#[macro_use]
extern crate prettytable;

use clap::{App, Arg, SubCommand};
use kio::{client, logger};
use log::{error, info, warn};
use prettytable::Table;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::base_consumer::BaseConsumer;
use rdkafka::consumer::{Consumer, DefaultConsumerContext};
use rdkafka::message::Message;
use rdkafka::producer::base_producer::{BaseProducer, BaseRecord};
use rdkafka::{Offset, TopicPartitionList};
use std::io::BufRead;
use std::time::{Duration, Instant};

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
                )
                .arg(
                    Arg::with_name("buffer_size")
                        .short("s")
                        .value_name("UINT")
                        .default_value("100")
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
                        .short("e")
                        .value_name("OFFSET")
                        .help("End offset inclusive"),
                )
                .arg(
                    Arg::with_name("from")
                        .short("s")
                        .alias("b")
                        .default_value("0")
                        .value_name("OFFSET")
                        .help("Starting offset exclusive"),
                ),
        )
        .subcommand(
            SubCommand::with_name("partitions")
                .about("List partitions for a given topic")
                .arg(
                    Arg::with_name("topic")
                        .short("t")
                        .value_name("TOPIC")
                        .required(true),
                ),
        )
        .subcommand(SubCommand::with_name("topics").about("List topics"))
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

    match matches.subcommand() {
        ("tail", Some(tail_m)) => {
            let topics: Vec<&str> = tail_m.values_of("topics").unwrap().collect();
            tail(config, topics, interval);
        }
        // ("list" => Some(list_m) => { },
        ("read", Some(read_m)) => {
            let topic = read_m.value_of("topic").unwrap();
            let from: i64 = read_m
                .value_of("from")
                .unwrap()
                .parse()
                .expect("from must be an integer");
            let to: Option<i64> = read_m.value_of("to").map(|t| t.parse().unwrap());
            read(config, topic, interval, Some(from), to);
        }
        ("partitions", Some(partition_m)) => {
            let topic = partition_m.value_of("topic").unwrap();
            partitions(config, topic);
        }
        ("topics", Some(_topic_m)) => {
            let mut table = Table::new();
            table.add_row(row![bFg=>
                "Topic",
                "Partition ID",
                "Partition Leader",
                "Partition Replicas",
                "Low Watermark",
                "High Watermark",
            ]);
            topics(config).for_each(|topic| {
                table.add_row(row![
                    topic.name,
                    topic.partition_id,
                    topic.partition_leader_id,
                    topic.partition_replicas,
                    topic.low_watermark,
                    topic.high_watermark,
                ]);
            });

            table.print_tty(true);
        }
        ("write", Some(write_m)) => {
            let topic = write_m.value_of("topic").unwrap();
            let buffer_size: u64 = write_m
                .value_of("buffer_size")
                .unwrap()
                .parse()
                .expect("Buffer size must be an integer");

            assert!(buffer_size > 0, "buffer size must be greater than zero");

            write(
                config,
                topic,
                interval,
                std::io::stdin().lock().lines().filter_map(|l| l.ok()),
            );
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

fn read(mut config: ClientConfig, topic: &str, interval: u64, from: Option<i64>, to: Option<i64>) {
    let consumer: BaseConsumer<DefaultConsumerContext> = config
        .set("enable.partition.eof", "true")
        .create_with_context(rdkafka::consumer::DefaultConsumerContext)
        .expect("Consumer creation failed");

    let poll_interval = Duration::from_secs(interval);
    // FIXME: allow partition to be specified or calculated
    let partition_id = 0;
    let (min_offset, max_offset) = consumer
        .fetch_watermarks(topic, partition_id, poll_interval)
        .unwrap();

    let first_offset = match from {
        Some(from_offset) => {
            if from_offset < 0 {
                Offset::Offset(max_offset - from_offset)
            } else {
                Offset::Offset(std::cmp::max(from_offset, min_offset))
            }
        }
        None => Offset::Beginning,
    };
    let last_offset = to.unwrap_or(max_offset);

    let mut topic_partitions = TopicPartitionList::new();
    topic_partitions.add_partition_offset(topic, 0, first_offset);
    consumer.assign(&topic_partitions).unwrap();

    loop {
        match consumer.poll(poll_interval) {
            Some(message) => match message {
                Err(e) => warn!("Kafka error: {}", e),
                Ok(m) => {
                    if m.offset() >= last_offset {
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

fn write(config: ClientConfig, topic: &str, interval: u64, messages: impl Iterator<Item = String>) {
    let producer: BaseProducer = config.create().expect("Producer creation failed");
    let poll_interval = Duration::from_secs(interval);
    let mut next_flush = Instant::now() + poll_interval * 2;
    messages.for_each(|message| {
        producer
            .send(BaseRecord::<String, String>::to(topic).payload(&message))
            .expect("Failed to send message");

        if next_flush > Instant::now() {
            info!("flushing {} messages", producer.in_flight_count());
            producer.flush(poll_interval);
            next_flush = Instant::now() + poll_interval;
        }
    });

    info!("flushing {} messages", producer.in_flight_count());
    producer.flush(poll_interval);
}

fn partitions(config: ClientConfig, topic: &str) {
    let consumer: BaseConsumer<DefaultConsumerContext> = config
        .create_with_context(rdkafka::consumer::DefaultConsumerContext)
        .expect("Consumer creation failed");

    let metadata = consumer
        .fetch_metadata(Some(topic), Duration::from_secs(10))
        .unwrap();

    let mut table = Table::new();
    table.add_row(row![bFg=>
        "Partition ID",
        "Partition Leader",
        "Partition Replicas",
        "Low Watermark",
        "High Watermark",
    ]);

    metadata.topics().iter().for_each(|topic| {
        topic.partitions().iter().for_each(|partition| {
            let (low_watermark, high_watermark) = consumer
                .fetch_watermarks(topic.name(), partition.id(), Duration::from_secs(1))
                .unwrap();
            table.add_row(row![
                partition.id(),
                partition.leader(),
                partition.replicas().len(),
                low_watermark,
                high_watermark,
            ]);
        })
    });

    table.print_tty(true);
}

struct Topic {
    name: String,
    partition_id: i32,
    partition_leader_id: i32,
    partition_replicas: usize,
    low_watermark: i64,
    high_watermark: i64,
}

fn topics(config: ClientConfig) -> impl Iterator<Item = Topic> {
    let consumer: BaseConsumer<DefaultConsumerContext> = config
        .create_with_context(rdkafka::consumer::DefaultConsumerContext)
        .expect("Consumer creation failed");

    let metadata = consumer
        .fetch_metadata(None, Duration::from_secs(10))
        .unwrap();

    let topic_names = metadata
        .topics()
        .iter()
        .filter(|topic| !topic.name().starts_with("__"));

    topic_names
        .flat_map(|topic| {
            topic
                .partitions()
                .iter()
                .map(|partition| {
                    let (low_watermark, high_watermark) = consumer
                        .fetch_watermarks(
                            topic.name().into(),
                            partition.id(),
                            Duration::from_secs(1),
                        )
                        .unwrap();

                    Topic {
                        name: topic.name().into(),
                        partition_id: partition.id(),
                        partition_leader_id: partition.leader(),
                        partition_replicas: partition.replicas().len(),
                        low_watermark,
                        high_watermark,
                    }
                })
                .collect::<Vec<Topic>>()
        })
        .collect::<Vec<Topic>>()
        .into_iter()
}
