# Kio

Interact with Kafka via Cli

* Read or tail streams
* Write to stream
* Expose metadata

## Examples

**Last 3 messages on a given topic**

```json
kio -b kafka:9092 read -s -3 collectd | jq -c
[{"values":[36.5],"dstypes":["gauge"],"dsnames":["value"],"time":1660539487.832,"interval":10,"host":"nuc","plugin":"thermal","plugin_instance":"thermal_zone1","type":"temperature","type_instance":""}]
[{"values":[196.598768966493,0],"dstypes":["derive","derive"],"dsnames":["user","syst"],"time":1660539487.832,"interval":10,"host":"nuc","plugin":"memcached","plugin_instance":"","type":"ps_cputime","type_instance":""}]
[{"values":[0],"dstypes":["gauge"],"dsnames":["value"],"time":1660539487.832,"interval":10,"host":"nuc","plugin":"thermal","plugin_instance":"cooling_device0","type":"gauge","type_instance":""}]
```

**Write message to topic**

```json
echo '{"k":"v"}' | kio write kio
kio read -s -1 collectd | jq -c
{"k":"v"}
```

**Write with envelope**

Pass `-f`.  Requires JSON serialized data which is placed under `.payload`

```json
$ kio -b nuc:9092 read collectd -s -3 -f| jq -c
{"offset":913463667,"partition":0,"payload":[{"dsnames":["value"],"dstypes":["gauge"],"host":"nuc","interval":10,"plugin":"thermal","plugin_instance":"thermal_zone1","time":1660539487.832,"type":"temperature","type_instance":"","values":[36.5]}],"timestamp":1660539487832}
{"offset":913463668,"partition":0,"payload":[{"dsnames":["user","syst"],"dstypes":["derive","derive"],"host":"nuc","interval":10,"plugin":"memcached","plugin_instance":"","time":1660539487.832,"type":"ps_cputime","type_instance":"","values":[196.598768966493,0]}],"timestamp":1660539487832}
{"offset":913463669,"partition":0,"payload":[{"dsnames":["value"],"dstypes":["gauge"],"host":"nuc","interval":10,"plugin":"thermal","plugin_instance":"cooling_device0","time":1660539487.832,"type":"gauge","type_instance":"","values":[0]}],"timestamp":1660539487832}
```

## Install

`cargo install kafka-io`

## Usage

```
Interact with Kafka over stdout/stdin

USAGE:
    kio [OPTIONS] [SUBCOMMAND]

OPTIONS:
    -b <host[:port]>          Broker URI authority [default: localhost:9092]
    -g <GROUP_ID>             [default: kio]
    -h, --help                Print help information
    -i <POLL_INTERVAL>        Interval in seconds to poll for new events [default: 5]
    -v <UINT>                 Sets the level of verbosity [default: 0]
    -V, --version             Print version information

SUBCOMMANDS:
    help          Print this message or the help of the given subcommand(s)
    partitions    List partitions for a given topic
    read          Read a specific range of messages from a given topic
    tail          Continuously read from a given set of topics
    topics        List topics
    write         Write an NLD set of messages to a given topic
```

### Write

```
Write an NLD set of messages to a given topic

USAGE:
    kio write [OPTIONS] <TOPIC>

ARGS:
    <TOPIC>    Topic name

OPTIONS:
    -h, --help       Print help information
    -s <UINT>        Buffer size [default: 100]
```

Example: `echo '{"k":"v"}' | kio write topic`

### Read

```
Read a specific range of messages from a given topic

USAGE:
    kio read [OPTIONS] <TOPIC>...

ARGS:
    <TOPIC>...    Topic name

OPTIONS:
    -e, --end <OFFSET>      End offset inclusive [default: -1]
    -f, --full              Encapsulate payload in the message metadata
    -h, --help              Print help information
    -s, --start <OFFSET>    Starting offset exclusive [default: 0]
```

Example: `kio read topic | jq -c`

### Topics

```
List topics

USAGE:
    kio topics

OPTIONS:
    -h, --help    Print help information
```

Example:

```
$ cargo run -q --bin kio -- -b nuc:9092 topics
+----------+--------------+------------------+--------------------+---------------+----------------+
| Topic    | Partition ID | Partition Leader | Partition Replicas | Low Watermark | High Watermark |
+----------+--------------+------------------+--------------------+---------------+----------------+
| bar      | 0            | 0                | 1                  | 42            | 204            |
+----------+--------------+------------------+--------------------+---------------+----------------+
| bar      | 1            | 0                | 1                  | 47            | 188            |
+----------+--------------+------------------+--------------------+---------------+----------------+
| foo      | 0            | 0                | 1                  | 36            | 36             |
+----------+--------------+------------------+--------------------+---------------+----------------+
```

### Partitions

```
List partitions for a given topic

USAGE:
    kio partitions <TOPIC>

ARGS:
    <TOPIC>

OPTIONS:
    -h, --help    Print help information
```

Example:

```
$ kio partitions topics
+--------------+------------------+--------------------+---------------+----------------+
| Partition ID | Partition Leader | Partition Replicas | Low Watermark | High Watermark |
+--------------+------------------+--------------------+---------------+----------------+
| 0            | 0                | 1                  | 42            | 204            |
+--------------+------------------+--------------------+---------------+----------------+
| 1            | 0                | 1                  | 47            | 188            |
+--------------+------------------+--------------------+---------------+----------------+
