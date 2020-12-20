extern crate clap;

use clap::{App, Arg};
use log::info;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::message::OwnedHeaders;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::time::Duration;
use kio::logger;

#[tokio::main]
pub async fn main() {
    logger::logger(false, None);
    let matches = App::new("kwrite")
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
        .arg(
            Arg::with_name("topic")
                .short("t")
                .value_name("foo")
                .multiple(true)
                .required(true),
        )
        .get_matches();

    let brokers: Vec<&str> = matches.values_of("brokers").unwrap().collect();
    let topic: &str = matches.value_of("topic").unwrap();

    let producer: &FutureProducer = &ClientConfig::new()
        .set("bootstrap.servers", brokers.join(",").as_str())
        .set("message.timeout.ms", "5000")
        .set("debug", "all")
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        .set("statistics.interval.ms", "30000")
        .set("auto.offset.reset", "smallest")
        .set_log_level(RDKafkaLogLevel::Debug)
        .create()
        .expect("Producer creation failed");

    // This loop is non blocking: all messages will be sent one after the other, without waiting
    // for the results.
    let futures = (0..5)
        .map(|i| {
            async move {
                // The send operation on the topic returns a future, which will be
                // completed once the result or failure from Kafka is received.
                let delivery_status = producer
                    .send(
                        FutureRecord::to(topic)
                            .payload(&format!("Message {}", i))
                            .key(&format!("Key {}", i))
                            .headers(OwnedHeaders::new().add("header_key", "header_value")),
                        Duration::from_secs(0),
                    )
                    .await;

                // This will be executed when the result is received.
                info!("Delivery status for message {} received", i);
                delivery_status
            }
        })
        .collect::<Vec<_>>();

    // This loop will wait until all delivery statuses have been received.
    for future in futures {
        info!("Future completed. Result: {:?}", future.await);
    }
}
