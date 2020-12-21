use rdkafka::config::{ClientConfig, RDKafkaLogLevel};

pub fn config(group_id: &str, brokers: Vec<&str>, log_level: RDKafkaLogLevel) -> ClientConfig {
    ClientConfig::new()
        .set("group.id", group_id)
        .set("debug", "all")
        .set("bootstrap.servers", brokers.join(",").as_str())
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("auto.offset.reset", "smallest")
        .set_log_level(log_level)
        .to_owned()
}
