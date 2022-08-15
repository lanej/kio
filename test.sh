#!/usr/bin/env sh
#
echo '{"foo":true}\
{"bar":false}' | env RUST_LOG=debug cargo run -q --bin kio  -- -b nuc:9092 -v 3 write -t kio
cargo run -q --bin kio -- -b nuc:9092 read -t kio -s -2 | jq -c
#test 3 -eq $(cargo run --bin kio -- -b nuc:9092 read -t kio -e -2 | wc -l)
