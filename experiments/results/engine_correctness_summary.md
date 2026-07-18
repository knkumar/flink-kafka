| Engine | Workload ID | Workload | Run label | Expected | Actual | Missing | Unexpected | Duplicates | Passed |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| flink | w1 | identity |  | 1000 | 1000 | 0 | 0 | 0 | True |
| flink | w1 | identity |  | 100 | 100 | 0 | 0 | 0 | True |
| flink | w1 | identity | rate10 | 100 | 100 | 0 | 0 | 0 | True |
| flink | w1 | identity | rate40 | 100 | 100 | 0 | 0 | 0 | True |
| flink | w1 | identity | repeat1 | 100 | 100 | 0 | 0 | 0 | True |
| flink | w1 | identity | stability_100 | 180000 | 180000 | 0 | 0 | 0 | True |
| flink | w1 | identity | repeat1 | 1000 | 1000 | 0 | 0 | 0 | True |
| flink | w2 | filter_map |  | 513 | 513 | 0 | 0 | 0 | True |
| flink | w2 | filter_map |  | 56 | 56 | 0 | 0 | 0 | True |
| flink | w2 | filter_map | rate10 | 56 | 56 | 0 | 0 | 0 | True |
| flink | w2 | filter_map | rate40 | 56 | 56 | 0 | 0 | 0 | True |
| flink | w2 | filter_map | repeat1 | 56 | 56 | 0 | 0 | 0 | True |
| flink | w2 | filter_map | stability_100 | 89906 | 89906 | 0 | 0 | 0 | True |
| flink | w2 | filter_map | repeat1 | 513 | 513 | 0 | 0 | 0 | True |
| flink | w3 | tumbling_count |  | 199 | 199 | 0 | 0 | 0 | True |
| flink | w3 | tumbling_count |  | 71 | 71 | 0 | 0 | 0 | True |
| flink | w3 | tumbling_count | rate10 | 71 | 71 | 0 | 0 | 0 | True |
| flink | w3 | tumbling_count | rate40 | 71 | 71 | 0 | 0 | 0 | True |
| flink | w3 | tumbling_count | repeat1 | 71 | 71 | 0 | 0 | 0 | True |
| flink | w3 | tumbling_count | stability_100 | 29924 | 29924 | 0 | 0 | 0 | True |
| flink | w3 | tumbling_count |  | 29924 | 0 | 29924 | 0 | 0 | False |
| flink | w3 | tumbling_count | tuning_checkpoint10s | 71 | 71 | 0 | 0 | 0 | True |
| flink | w3 | tumbling_count |  | 71 | 0 | 71 | 0 | 0 | False |
| flink | w3 | tumbling_count | tuning_control | 71 | 71 | 0 | 0 | 0 | True |
| flink | w3 | tumbling_count |  | 71 | 0 | 71 | 0 | 0 | False |
| flink | w3 | tumbling_count | repeat1 | 199 | 199 | 0 | 0 | 0 | True |
| flink | w4 | sliding_sum |  | 1099 | 1099 | 0 | 0 | 0 | True |
| flink | w4 | sliding_sum |  | 710 | 710 | 0 | 0 | 0 | True |
| flink | w4 | sliding_sum | rate10 | 710 | 710 | 0 | 0 | 0 | True |
| flink | w4 | sliding_sum | rate40 | 710 | 710 | 0 | 0 | 0 | True |
| flink | w4 | sliding_sum | repeat1 | 710 | 710 | 0 | 0 | 0 | True |
| flink | w4 | sliding_sum | stability_100 | 30900 | 30900 | 0 | 0 | 0 | True |
| flink | w4 | sliding_sum | repeat1 | 1099 | 1099 | 0 | 0 | 0 | True |
| flink | w5 | stream_stream_join |  | 11024 | 11024 | 0 | 0 | 0 | True |
| flink | w5 | stream_stream_join |  | 186 | 186 | 0 | 0 | 0 | True |
| flink | w5 | stream_stream_join | rate10 | 186 | 186 | 0 | 0 | 0 | True |
| flink | w5 | stream_stream_join | rate40 | 186 | 186 | 0 | 0 | 0 | True |
| flink | w5 | stream_stream_join | repeat1 | 186 | 186 | 0 | 0 | 0 | True |
| flink | w5 | stream_stream_join | repeat1 | 11024 | 11024 | 0 | 0 | 0 | True |
| kafka_streams | w1 | identity |  | 1000 | 1000 | 0 | 0 | 0 | True |
| kafka_streams | w1 | identity |  | 100 | 100 | 0 | 0 | 0 | True |
| kafka_streams | w1 | identity | rate10 | 100 | 100 | 0 | 0 | 0 | True |
| kafka_streams | w1 | identity | rate40 | 100 | 100 | 0 | 0 | 0 | True |
| kafka_streams | w1 | identity | repeat1 | 100 | 100 | 0 | 0 | 0 | True |
| kafka_streams | w1 | identity | resource_metrics | 100 | 100 | 0 | 0 | 0 | True |
| kafka_streams | w1 | identity | stability_100 | 180000 | 180000 | 0 | 0 | 0 | True |
| kafka_streams | w1 | identity | repeat1 | 1000 | 1000 | 0 | 0 | 0 | True |
| kafka_streams | w2 | filter_map |  | 513 | 513 | 0 | 0 | 0 | True |
| kafka_streams | w2 | filter_map |  | 56 | 56 | 0 | 0 | 0 | True |
| kafka_streams | w2 | filter_map | rate10 | 56 | 56 | 0 | 0 | 0 | True |
| kafka_streams | w2 | filter_map | rate40 | 56 | 56 | 0 | 0 | 0 | True |
| kafka_streams | w2 | filter_map | repeat1 | 56 | 56 | 0 | 0 | 0 | True |
| kafka_streams | w2 | filter_map | resource_metrics | 56 | 56 | 0 | 0 | 0 | True |
| kafka_streams | w2 | filter_map | stability_100 | 89906 | 89906 | 0 | 0 | 0 | True |
| kafka_streams | w2 | filter_map | repeat1 | 513 | 513 | 0 | 0 | 0 | True |
| kafka_streams | w3 | tumbling_count |  | 199 | 199 | 0 | 0 | 0 | True |
| kafka_streams | w3 | tumbling_count |  | 71 | 71 | 0 | 0 | 0 | True |
| kafka_streams | w3 | tumbling_count | rate10 | 71 | 71 | 0 | 0 | 0 | True |
| kafka_streams | w3 | tumbling_count | rate40 | 71 | 71 | 0 | 0 | 0 | True |
| kafka_streams | w3 | tumbling_count | repeat1 | 71 | 71 | 0 | 0 | 0 | True |
| kafka_streams | w3 | tumbling_count | resource_metrics | 71 | 71 | 0 | 0 | 0 | True |
| kafka_streams | w3 | tumbling_count | stability_100 | 29924 | 29924 | 0 | 0 | 0 | True |
| kafka_streams | w3 | tumbling_count | tuning_commit10s | 71 | 71 | 0 | 0 | 0 | True |
| kafka_streams | w3 | tumbling_count | tuning_control | 71 | 71 | 0 | 0 | 0 | True |
| kafka_streams | w3 | tumbling_count | repeat1 | 199 | 199 | 0 | 0 | 0 | True |
| kafka_streams | w4 | sliding_sum |  | 1099 | 1099 | 0 | 0 | 0 | True |
| kafka_streams | w4 | sliding_sum |  | 710 | 710 | 0 | 0 | 0 | True |
| kafka_streams | w4 | sliding_sum | rate10 | 710 | 710 | 0 | 0 | 0 | True |
| kafka_streams | w4 | sliding_sum | rate40 | 710 | 710 | 0 | 0 | 0 | True |
| kafka_streams | w4 | sliding_sum | repeat1 | 710 | 710 | 0 | 0 | 0 | True |
| kafka_streams | w4 | sliding_sum | resource_metrics | 710 | 710 | 0 | 0 | 0 | True |
| kafka_streams | w4 | sliding_sum | stability_100 | 30900 | 30900 | 0 | 0 | 0 | True |
| kafka_streams | w4 | sliding_sum | repeat1 | 1099 | 1099 | 0 | 0 | 0 | True |
| kafka_streams | w5 | stream_stream_join |  | 11024 | 11024 | 0 | 0 | 0 | True |
| kafka_streams | w5 | stream_stream_join |  | 186 | 186 | 0 | 0 | 0 | True |
| kafka_streams | w5 | stream_stream_join | rate10 | 186 | 186 | 0 | 0 | 0 | True |
| kafka_streams | w5 | stream_stream_join | rate40 | 186 | 186 | 0 | 0 | 0 | True |
| kafka_streams | w5 | stream_stream_join | repeat1 | 186 | 186 | 0 | 0 | 0 | True |
| kafka_streams | w5 | stream_stream_join | stability_100 | 1093417 | 1093417 | 0 | 0 | 0 | True |
| kafka_streams | w5 | stream_stream_join | repeat1 | 11024 | 11024 | 0 | 0 | 0 | True |
