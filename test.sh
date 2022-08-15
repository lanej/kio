#!/usr/bin/env sh
#
echo "{\"foo\":true}
{\"bar\":false}" | cargo run -q --bin kio  -- -b nuc:9092 write kio
test 3 -eq $(cargo run -q --bin kio -- -b nuc:9092 read kio -s -2 | jq -c | wc -l)
