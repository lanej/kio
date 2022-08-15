extern crate clap;
#[macro_use]
extern crate prettytable;

use clap::{App, Arg, Command};
use kafka_io::{client, logger};
use log::{debug, info, warn};
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

    let app = App::new("kio")
        .version("0.1")
        .author("Josh Lane <me@joshualane.com>")
        .about("Spew Kafka messages to stdout")
        .arg(
            Arg::with_name("brokers")
                .short('b')
                .value_name("host[:port]")
                .default_value("localhost:9092")
                .takes_value(true)
                .help("Broker URI authority"),
        )
        .arg(
            Arg::with_name("verbose")
                .short('v')
                .value_name("UINT")
                .default_value("0")
                .takes_value(true)
                .required(false)
                .help("Sets the level of verbosity"),
        )
        .arg(
            Arg::with_name("group")
                .short('g')
                .value_name("GROUP_ID")
                .default_value("kio"),
        )
        .arg(
            Arg::with_name("interval")
                .short('i')
                .value_name("POLL_INTERVAL")
                .default_value("5")
                .help("Interval in seconds to poll for new events"),
        )
        .subcommand(
            Command::new("tail")
                .about("Continuously read from a given set of topics")
                .arg(
                    Arg::with_name("topics")
                        .short('t')
                        .value_name("TOPIC")
                        .multiple(true)
                        .takes_value(true)
                        .required(true),
                )
                .arg(
                    Arg::with_name("lines")
                        .short('n')
                        .value_name("UINT")
                        .default_value("0"),
                )
                .arg(
                    Arg::with_name("full")
                        .long("full")
                        .short('f')
                        .help("Encapsulate payload in the message metadata"),
                ),
        )
        .subcommand(
            Command::new("write")
                .about("Write an NLD set of messages to a given topic")
                .arg(
                    Arg::with_name("topic")
                        .short('t')
                        .value_name("TOPIC")
                        .required(true),
                )
                .arg(
                    Arg::with_name("buffer_size")
                        .short('s')
                        .value_name("UINT")
                        .default_value("100"),
                ),
        )
        .subcommand(
            Command::new("read")
                .about("Read a specific range of messages from a given topic")
                .arg(
                    Arg::with_name("topic")
                        .short('t')
                        .value_name("TOPIC")
                        .multiple(true)
                        .required(true),
                )
                .arg(
                    Arg::with_name("end")
                        .long("end")
                        .short('e')
                        .value_name("OFFSET")
                        .default_value("-1")
                        .allow_hyphen_values(true)
                        .help("End offset inclusive"),
                )
                .arg(
                    Arg::with_name("start")
                        .long("start")
                        .short('s')
                        .default_value("0")
                        .value_name("OFFSET")
                        .allow_hyphen_values(true)
                        .help("Starting offset exclusive"),
                )
                .arg(
                    Arg::with_name("full")
                        .long("full")
                        .short('f')
                        .help("Encapsulate payload in the message metadata"),
                ),
        )
        .subcommand(
            Command::new("partitions")
                .about("List partitions for a given topic")
                .arg(
                    Arg::with_name("topic")
                        .short('t')
                        .value_name("TOPIC")
                        .required(true),
                ),
        )
        .subcommand(Command::new("topics").about("List topics"));

    let matches = app.get_matches();

    let brokers: Vec<&str> = matches.values_of("brokers").unwrap().collect();
    let log_level = match matches
        .value_of("verbose")
        .unwrap()
        .parse()
        .expect("verbose to an integer")
    {
        0 => RDKafkaLogLevel::Error,
        1 => RDKafkaLogLevel::Warning,
        2 => RDKafkaLogLevel::Info,
        3 => RDKafkaLogLevel::Debug,
        _ => RDKafkaLogLevel::Warning,
    };

    let group = matches.value_of("group").unwrap();
    let mut config = client::config(group, brokers, log_level);
    let interval = matches
        .value_of("interval")
        .unwrap()
        .parse()
        .expect("from must be an integer");

    match matches.subcommand() {
        Some(("tail", tail_m)) => {
            let topics: Vec<&str> = tail_m.values_of("topics").unwrap().collect();
            let full = tail_m.is_present("full");
            tail(
                &config,
                topics,
                interval,
                full,
                OffsetRange::from((tail_m.value_of("lines"), None)),
            )
        }
        Some(("read", read_m)) => {
            read(
                &mut config,
                read_m.value_of("topic").expect("No topic specified"),
                interval,
                read_m.is_present("full"),
                OffsetRange::from((read_m.value_of("start"), read_m.value_of("end"))),
            );
        }
        Some(("partitions", partition_m)) => {
            let topic = partition_m.value_of("topic").unwrap();
            partitions(config, topic);
        }
        Some(("topics", _topic_m)) => {
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
        Some(("write", write_m)) => {
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

fn tail(config: &ClientConfig, topics: Vec<&str>, interval: u64, full: bool, range: OffsetRange) {
    let consumer: BaseConsumer<DefaultConsumerContext> = config
        .create_with_context(rdkafka::consumer::DefaultConsumerContext)
        .expect("Consumer creation failed");
    let poll_interval = Duration::from_secs(interval);
    let mut topic_partitions = TopicPartitionList::new();
    let metadata = consumer
        .fetch_metadata(None, Duration::from_secs(10))
        .unwrap();

    topics.iter().for_each(|topic| {
        let (min_offset, max_offset) = consumer
            // FIXME: hard-coded partition should be variable
            .fetch_watermarks(topic, 0, poll_interval)
            .unwrap();

        let (seek_to, _stop_at) = range.offsets(min_offset, max_offset);
        let leader = metadata
            .topics()
            .iter()
            .find_map(|mtopic| {
                mtopic
                    .partitions()
                    .iter()
                    .find(|partition| partition.id() == partition.leader())
            })
            .expect("Couldn't find leader");

        // TODO: partition can be specified
        topic_partitions.add_partition_offset(topic, leader.id(), seek_to);
    });
    consumer.assign(&topic_partitions).unwrap();

    consumer
        .subscribe(&topics)
        .expect("Can't subscribe to specified topics");

    loop {
        match consumer.poll(poll_interval) {
            Some(message) => match message {
                Err(e) => warn!("Kafka error: {}", e),
                Ok(m) => {
                    let raw_payload = match m.payload_view::<str>() {
                        None => "",
                        Some(Ok(s)) => s,
                        Some(Err(e)) => {
                            warn!("Error while deserializing message payload: {:?}", e);
                            ""
                        }
                    };

                    if full {
                        match serde_json::from_str::<serde_json::Value>(raw_payload) {
                            Ok(payload) => {
                                let timestamp = match m.timestamp() {
                                    rdkafka::message::Timestamp::NotAvailable => String::new(),
                                    rdkafka::message::Timestamp::CreateTime(t)
                                    | rdkafka::message::Timestamp::LogAppendTime(t) => {
                                        format!("{}", t)
                                    }
                                };
                                println!(
                                    "{}",
                                    serde_json::json!({
                                        "key": m.key(),
                                        "partition": m.partition(),
                                        "offset": m.offset(),
                                        "timestamp": timestamp,
                                        "payload": payload,
                                    })
                                );
                            }
                            Err(err) => {
                                eprintln!("Failed to parse JSON body '{}': {}", raw_payload, err)
                            }
                        }
                    } else {
                        println!("{}\n", raw_payload)
                    }
                }
            },
            _ => {}
        }
    }
}

fn read(config: &mut ClientConfig, topic: &str, interval: u64, full: bool, range: OffsetRange) {
    let consumer: BaseConsumer<DefaultConsumerContext> = config
        .set("enable.partition.eof", "true")
        .create_with_context(rdkafka::consumer::DefaultConsumerContext)
        .expect("Consumer creation failed");

    let poll_interval = Duration::from_secs(interval);
    let metadata = consumer
        .fetch_metadata(None, Duration::from_secs(10))
        .unwrap();
    let leader = metadata
        .topics()
        .iter()
        .find_map(|mtopic| {
            mtopic
                .partitions()
                .iter()
                .find(|partition| partition.id() == partition.leader())
        })
        .expect("Couldn't find leader");

    let (min_offset, max_offset) = consumer
        .fetch_watermarks(topic, leader.id(), poll_interval)
        .unwrap();

    let (seek_to, stop_at) = range.offsets(min_offset, max_offset);

    let mut topic_partitions = TopicPartitionList::new();

    topic_partitions.add_partition_offset(topic, leader.id(), seek_to);
    consumer.assign(&topic_partitions).unwrap();

    loop {
        match consumer.poll(poll_interval) {
            Some(message) => match message {
                Err(e) => warn!("Kafka error: {}", e),
                Ok(m) => {
                    if m.offset() >= stop_at {
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

                    if full {
                        match serde_json::from_str::<serde_json::Value>(payload) {
                            Ok(body) => {
                                println!(
                                    "{}",
                                    serde_json::json!({
                                        "offset": m.offset(),
                                        "partition": m.partition(),
                                        "timestamp": m.timestamp().to_millis(),
                                        "payload": body
                                    })
                                );
                            }
                            Err(err) => {
                                eprintln!("Failed to parse JSON body '{}': {}", payload, err)
                            }
                        };
                    } else {
                        println!("{}", payload)
                    }
                }
            },
            _ => debug!("No messages received during last poll"),
        }
    }
}

fn write(config: ClientConfig, topic: &str, interval: u64, messages: impl Iterator<Item = String>) {
    let producer: BaseProducer = config.create().expect("Producer creation failed");
    let poll_interval = Duration::from_secs(interval);
    let mut next_flush = Instant::now() + poll_interval * 2;
    messages.for_each(|message| {
        debug!("Sending: '{}'", &message);
        producer
            .send(BaseRecord::<String, String>::to(topic).payload(&message))
            .expect("Failed to send message");

        if next_flush > Instant::now() {
            debug!("flushing {} messages", producer.in_flight_count());
            producer.flush(poll_interval);
            next_flush = Instant::now() + poll_interval;
        }
    });

    let in_flight_count = producer.in_flight_count();

    if in_flight_count > 0 {
        info!("flushing {} messages", in_flight_count);
    }
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

    metadata
        .topics()
        .iter()
        .filter(|topic| !topic.name().starts_with("__"))
        .map(|topic| {
            topic
                .partitions()
                .iter()
                .map(|partition| {
                    let (low_watermark, high_watermark) = (&consumer)
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
        .flatten()
        .collect::<Vec<Topic>>()
        .into_iter()
}

struct OffsetRange {
    start: OffsetPosition,
    end: OffsetPosition,
}

impl OffsetRange {
    fn offsets(&self, min: i64, max: i64) -> (Offset, i64) {
        let seek_to = match &self.start {
            OffsetPosition::Unspecified => Offset::Beginning,
            OffsetPosition::Positive(p) => Offset::Offset(min + p),
            OffsetPosition::Negative(p) => Offset::Offset(max - p - 2),
            OffsetPosition::Absolute(p) => Offset::Offset(*p),
        };

        let stop_at = match &self.end {
            OffsetPosition::Unspecified => max,
            OffsetPosition::Positive(p) => min + p,
            OffsetPosition::Negative(p) => max - p - 1,
            OffsetPosition::Absolute(p) => *p,
        };

        (seek_to, stop_at)
    }
}

#[derive(Debug)]
enum OffsetPosition {
    Unspecified,
    Positive(i64),
    Negative(i64),
    Absolute(i64),
}

impl std::convert::From<(Option<&str>, Option<&str>)> for OffsetRange {
    fn from((from, to): (Option<&str>, Option<&str>)) -> Self {
        OffsetRange {
            start: from
                .map(|v| match v.chars().nth(0).unwrap() {
                    '+' => {
                        OffsetPosition::Positive(v.get(1..).unwrap().parse().expect("valid number"))
                    }
                    '-' => {
                        OffsetPosition::Negative(v.get(1..).unwrap().parse().expect("valid number"))
                    }
                    _ => OffsetPosition::Absolute(v.parse().unwrap()),
                })
                .unwrap_or(OffsetPosition::Unspecified),
            end: to
                .map(|v| match v.chars().nth(0).unwrap() {
                    '+' => {
                        OffsetPosition::Positive(v.get(1..).unwrap().parse().expect("valid number"))
                    }
                    '-' => {
                        OffsetPosition::Negative(v.get(1..).unwrap().parse().expect("valid number"))
                    }
                    _ => OffsetPosition::Absolute(v.parse().expect("valid number")),
                })
                .unwrap_or(OffsetPosition::Unspecified),
        }
    }
}
