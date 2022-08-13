# Install

`cargo install kafka-io`

# Examples

Write

# Usage

```
USAGE:
    kio [FLAGS] [OPTIONS] [SUBCOMMAND]

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information
    -v               Sets the level of verbosity

OPTIONS:
    -b <host[:port]>...        Broker URI authority [default: localhost:9092]
    -g <GROUP_ID>               [default: kio]
    -i <POLL_INTERVAL>         Interval in seconds to poll for new events [default: 5]

SUBCOMMANDS:
    help          Prints this message or the help of the given subcommand(s)
    partitions    List partitions for a given topic
    read          Read a specific range of messages from a given topic
    tail          Continuously read from a given set of topics
    topics        List topics
    write         Write an NLD set of messages to a given topic
```

## Write

```
Write an NLD set of messages to a given topic

USAGE:
    kio write -s <UINT> -t <TOPIC>

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
    -s <UINT>         Buffer size [default: 100]
    -t <TOPIC>        Topic name
```

## Read

```
kio-read 
Read a specific range of messages from a given topic

USAGE:
    kio read --end <OFFSET> --start <OFFSET> -t <TOPIC>...

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
    -e, --end <OFFSET>      End offset inclusive [default: -1]
    -s, --start <OFFSET>    Starting offset exclusive [default: 0]
    -t <TOPIC>...           Topic name
```
