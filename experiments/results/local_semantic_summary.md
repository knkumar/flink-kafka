# Local semantic harness summary

This summary covers only the in-process semantic harness. It does not contain Flink or Kafka Streams measurements.

| Workload | Events | Keys | Seed | Output records | Verification | Missing | Unexpected | Duplicates |
| --- | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: |
| identity | 1000 | 100 | 7 | 1000 | True | 0 | 0 | 0 |
| filter_map | 1000 | 100 | 7 | 513 | True | 0 | 0 | 0 |
| tumbling_count | 1000 | 100 | 7 | 199 | True | 0 | 0 | 0 |
| sliding_sum | 1000 | 100 | 7 | 1099 | True | 0 | 0 | 0 |
| stream_stream_join | 1000 | 100 | 7 | 11024 | True | 0 | 0 | 0 |
