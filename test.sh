#!/usr/bin/env sh
#
echo "{\"foo\":true}
{\"bar\":false}" | tee | env RUST_LOG=debug cargo run -q --bin kio  -- -b nuc:9092 write -t kio
test 3 -eq $(cargo run -q --bin kio -- -b nuc:9092 read -t kio -s -2 | jq -c | wc -l)
