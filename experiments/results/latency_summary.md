| Engine | Workload ID | Workload | Run label | Rate | Produced | Expected | Matched | Passed | p50 ms | p95 ms | p99 ms | p99 t1-t0 | p99 t2-t1 | p99 t3-t2 |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| flink |  | identity | failure_broker_kill | 20.0 | 2000 | 2000 | 2000 | True | 1081.573 | 5743.883 | 9743.444 | 8783.129 | 408.0 | 1009.846 |
| flink |  | identity | failure_jvm_kill | 20.0 | 2000 | 2000 | 2000 | True | 1096.542 | 3632.18 | 7557.46 | 1922.476 | 7232.0 | 996.364 |
| flink |  | identity | failure_node_loss | 20.0 | 2000 | 2000 | 2000 | True | 1105.865 | 5209.004 | 9208.617 | 1613.659 | 8931.0 | 1062.098 |
| flink | w1 | identity |  | 20.0 | 100 | 100 | 100 | True | 1668.467 | 3288.828 | 3488.793 |  |  |  |
| flink | w1 | identity | rate10 | 10.0 | 100 | 100 | 100 | True | 1260.707 | 2060.664 | 2460.697 |  |  |  |
| flink | w1 | identity | rate40 | 40.0 | 100 | 100 | 100 | True | 1990.019 | 3114.001 | 3214.316 |  |  |  |
| flink | w1 | identity | repeat1 | 20.0 | 100 | 100 | 100 | True | 948.056 | 2447.962 | 2647.962 |  |  |  |
| flink | w1 | identity | stability_100 | 100.0 | 180000 | 180000 | 180000 | True | 1012.721 | 1702.92 | 1882.096 | 1000.26 | 4.0 | 1001.017 |
| flink | w2 | filter_map |  | 20.0 | 100 | 56 | 56 | True | 1400.69 | 3200.614 | 3350.702 |  |  |  |
| flink | w2 | filter_map | rate10 | 10.0 | 100 | 56 | 56 | True | 1332.854 | 2332.809 | 2632.06 |  |  |  |
| flink | w2 | filter_map | rate40 | 40.0 | 100 | 56 | 56 | True | 2088.45 | 3213.16 | 3287.256 |  |  |  |
| flink | w2 | filter_map | repeat1 | 20.0 | 100 | 56 | 56 | True | 900.849 | 2611.81 | 2761.075 |  |  |  |
| flink | w2 | filter_map | stability_100 | 100.0 | 180000 | 89906 | 89906 | True | 1007.794 | 1688.498 | 1874.898 | 999.917 | 4.0 | 996.966 |
| flink | w3 | tumbling_count |  | 20.0 | 101 | 71 | 71 | True | 2167.446 | 5068.23 | 5317.375 |  |  |  |
| flink | w3 | tumbling_count | rate10 | 10.0 | 101 | 71 | 71 | True | 4228.258 | 10028.509 | 10527.388 |  |  |  |
| flink | w3 | tumbling_count | rate40 | 40.0 | 101 | 71 | 71 | True | 2077.073 | 3527.95 | 3651.222 |  |  |  |
| flink | w3 | tumbling_count | repeat1 | 20.0 | 101 | 71 | 71 | True | 2509.091 | 5409.743 | 5658.188 |  |  |  |
| flink | w3 | tumbling_count | stability_100 | 100.0 | 180001 | 29924 | 29924 | True | 1789.284 | 4084.197 | 5506.696 | 991.721 | 4287.0 | 968.593 |
| flink | w3 | tumbling_count | tuning_checkpoint10s | 20.0 | 101 | 71 | 71 | True | 4993.134 | 7893.233 | 8142.178 | 2486.709 | 2728.0 | 2954.468 |
| flink | w3 | tumbling_count | tuning_control | 20.0 | 101 | 71 | 71 | True | 2293.651 | 5194.313 | 5442.462 | 2539.632 | 2655.0 | 271.83 |
| flink | w4 | sliding_sum |  | 20.0 | 101 | 710 | 710 | True | 2638.442 | 5538.424 | 5786.186 |  |  |  |
| flink | w4 | sliding_sum | rate10 | 10.0 | 101 | 710 | 710 | True | 4091.697 | 9891.631 | 10390.27 |  |  |  |
| flink | w4 | sliding_sum | rate40 | 40.0 | 101 | 710 | 710 | True | 1965.928 | 3415.926 | 3540.11 |  |  |  |
| flink | w4 | sliding_sum | repeat1 | 20.0 | 101 | 710 | 710 | True | 2359.98 | 5259.953 | 5507.883 |  |  |  |
| flink | w4 | sliding_sum | stability_100 | 100.0 | 180001 | 30900 | 30900 | True | 1840.766 | 4179.25 | 5751.375 | 1000.428 | 5080.0 | 994.935 |
| flink | w5 | stream_stream_join |  | 20.0 | 200 | 186 | 186 | True | 805.6 | 2321.768 | 2521.762 |  |  |  |
| flink | w5 | stream_stream_join | rate10 | 10.0 | 200 | 186 | 186 | True | 1135.848 | 2039.22 | 2439.144 |  |  |  |
| flink | w5 | stream_stream_join | rate40 | 40.0 | 200 | 186 | 186 | True | 1531.194 | 2705.081 | 2805.058 |  |  |  |
| flink | w5 | stream_stream_join | repeat1 | 20.0 | 200 | 186 | 186 | True | 1547.947 | 3059.302 | 3259.235 |  |  |  |
| kafka_streams |  | identity | failure_broker_kill | 20.0 | 2000 | 2000 | 2000 | True | 1318.962 | 6439.641 | 9892.336 | 8454.993 | 108.0 | 9387.298 |
| kafka_streams |  | identity | failure_jvm_kill | 20.0 | 2000 | 2000 | 2000 | True | 1623.856 | 39977.178 | 43976.654 | 1381.329 | 42944.0 | 1019.231 |
| kafka_streams |  | identity | failure_node_loss | 20.0 | 2000 | 2000 | 2000 | True | 1316.88 | 40225.322 | 44224.997 | 1574.641 | 43240.0 | 960.554 |
| kafka_streams | w1 | identity |  | 20.0 | 100 | 100 | 100 | True | 1182.021 | 2622.002 | 2821.55 |  |  |  |
| kafka_streams | w1 | identity | rate10 | 10.0 | 100 | 100 | 100 | True | 1022.525 | 2334.453 | 2734.456 |  |  |  |
| kafka_streams | w1 | identity | rate40 | 40.0 | 100 | 100 | 100 | True | 1704.546 | 2828.505 | 2928.505 |  |  |  |
| kafka_streams | w1 | identity | repeat1 | 20.0 | 100 | 100 | 100 | True | 1226.389 | 2633.999 | 2833.992 |  |  |  |
| kafka_streams | w1 | identity | resource_metrics | 20.0 | 100 | 100 | 100 | True | 1526.48 | 3726.034 | 3926.008 | 2653.391 | 226.0 | 1104.589 |
| kafka_streams | w1 | identity | stability_100 | 100.0 | 180000 | 180000 | 180000 | True | 1216.313 | 1852.678 | 1967.434 | 1000.655 | 6.0 | 1008.637 |
| kafka_streams | w2 | filter_map |  | 20.0 | 100 | 56 | 56 | True | 1226.708 | 2843.058 | 2993.158 |  |  |  |
| kafka_streams | w2 | filter_map | rate10 | 10.0 | 100 | 56 | 56 | True | 1221.64 | 2526.477 | 2825.823 |  |  |  |
| kafka_streams | w2 | filter_map | rate40 | 40.0 | 100 | 56 | 56 | True | 1765.886 | 2890.619 | 2964.982 |  |  |  |
| kafka_streams | w2 | filter_map | repeat1 | 20.0 | 100 | 56 | 56 | True | 1265.364 | 2665.192 | 2814.333 |  |  |  |
| kafka_streams | w2 | filter_map | resource_metrics | 20.0 | 100 | 56 | 56 | True | 1778.035 | 3660.216 | 3809.273 | 2671.535 | 76.0 | 1115.738 |
| kafka_streams | w2 | filter_map | stability_100 | 100.0 | 180000 | 89906 | 89906 | True | 999.378 | 1749.052 | 1928.279 | 1000.495 | 5.0 | 1009.256 |
| kafka_streams | w3 | tumbling_count |  | 20.0 | 101 | 71 | 71 | True | 3973.097 | 6873.202 | 7123.28 |  |  |  |
| kafka_streams | w3 | tumbling_count | rate10 | 10.0 | 101 | 71 | 71 | True | 5331.984 | 11132.103 | 11631.267 |  |  |  |
| kafka_streams | w3 | tumbling_count | rate40 | 40.0 | 101 | 71 | 71 | True | 2368.306 | 3818.504 | 3942.612 |  |  |  |
| kafka_streams | w3 | tumbling_count | repeat1 | 20.0 | 101 | 71 | 71 | True | 3595.147 | 6495.271 | 6744.307 |  |  |  |
| kafka_streams | w3 | tumbling_count | resource_metrics | 20.0 | 101 | 71 | 71 | True | 3881.205 | 6781.175 | 7030.591 | 2502.856 | 4485.0 | 54.386 |
| kafka_streams | w3 | tumbling_count | stability_100 | 100.0 | 180001 | 29924 | 29924 | True | 2876.071 | 5147.158 | 6576.362 | 991.937 | 6083.0 | 18.953 |
| kafka_streams | w3 | tumbling_count | tuning_commit10s | 20.0 | 101 | 71 | 71 | True | 21946.167 | 24846.162 | 25095.501 | 2439.672 | 22616.0 | 57.547 |
| kafka_streams | w3 | tumbling_count | tuning_control | 20.0 | 101 | 71 | 71 | True | 3080.8 | 5980.766 | 6230.042 | 2469.855 | 3715.0 | 60.099 |
| kafka_streams | w4 | sliding_sum |  | 20.0 | 101 | 710 | 710 | True | 3964.038 | 6864.071 | 7113.249 |  |  |  |
| kafka_streams | w4 | sliding_sum | rate10 | 10.0 | 101 | 710 | 710 | True | 4865.798 | 10665.881 | 11164.941 |  |  |  |
| kafka_streams | w4 | sliding_sum | rate40 | 40.0 | 101 | 710 | 710 | True | 2433.497 | 3883.589 | 4006.722 |  |  |  |
| kafka_streams | w4 | sliding_sum | repeat1 | 20.0 | 101 | 710 | 710 | True | 3191.806 | 6091.807 | 6338.27 |  |  |  |
| kafka_streams | w4 | sliding_sum | resource_metrics | 20.0 | 101 | 710 | 710 | True | 3164.033 | 6063.919 | 6311.03 | 2459.594 | 3750.0 | 127.792 |
| kafka_streams | w4 | sliding_sum | stability_100 | 100.0 | 180001 | 30900 | 30900 | True | 2926.241 | 5243.404 | 6813.603 | 999.179 | 6249.0 | 65.444 |
| kafka_streams | w5 | stream_stream_join |  | 20.0 | 200 | 186 | 186 | True | 2544.645 | 3896.173 | 4096.137 |  |  |  |
| kafka_streams | w5 | stream_stream_join | rate10 | 10.0 | 200 | 186 | 186 | True | 1835.608 | 3035.445 | 3435.417 |  |  |  |
| kafka_streams | w5 | stream_stream_join | rate40 | 40.0 | 200 | 186 | 186 | True | 2195.37 | 3338.27 | 3438.284 |  |  |  |
| kafka_streams | w5 | stream_stream_join | repeat1 | 20.0 | 200 | 186 | 186 | True | 2540.653 | 3898.778 | 4098.726 |  |  |  |
| kafka_streams | w5 | stream_stream_join | stability_100 | 100.0 | 24000 | 1093417 | 1093417 | True | 2172.128 | 2762.18 | 2913.538 | 1001.382 | 965.0 | 1107.523 |
