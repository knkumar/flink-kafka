| Engine | Workload ID | Workload | Run label | Rate | Produced | Expected | Matched | Passed | p50 ms | p95 ms | p99 ms | p50 semantic_wait | p95 semantic_wait | p99 semantic_wait | p50 engine_compute | p95 engine_compute | p99 engine_compute | p50 visibility | p95 visibility | p99 visibility | p99 write_to_input_append_latency | p99 input_append_to_result_emission_latency | p99 l_visibility | p99 l_closure |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| flink |  | identity | failure_broker_kill | 20.0 | 2000 | 2000 | 2000 | True | 1096.124 | 1841.978 | 2323.333 |  |  |  |  |  |  |  |  |  | 1489.048 | 125.0 | 1065.306 |  |
| flink |  | identity | failure_broker_kill_trial2 | 20.0 | 2000 | 2000 | 2000 | True | 1110.107 | 1852.954 | 2442.643 |  |  |  |  |  |  |  |  |  | 1406.429 | 84.0 | 1057.434 |  |
| flink |  | identity | failure_broker_kill_trial3 | 20.0 | 2000 | 2000 | 2000 | True | 1102.664 | 1809.871 | 2338.033 |  |  |  |  |  |  |  |  |  | 1454.591 | 84.0 | 1011.579 |  |
| flink |  | identity | failure_broker_kill_trial4 | 20.0 | 2000 | 2000 | 2000 | True | 1119.878 | 1873.143 | 2307.188 |  |  |  |  |  |  |  |  |  | 1417.952 | 99.0 | 1121.739 |  |
| flink |  | identity | failure_broker_kill_trial5 | 20.0 | 2000 | 2000 | 2000 | True | 1143.464 | 1897.446 | 2615.79 |  |  |  |  |  |  |  |  |  | 1463.39 | 108.0 | 1120.593 |  |
| flink |  | identity | failure_changelog_restore | 20.0 | 2000 | 2000 | 2000 | True | 1134.238 | 1888.645 | 2568.458 |  |  |  |  |  |  |  |  |  | 1798.46 | 115.0 | 1075.387 |  |
| flink |  | identity | failure_changelog_restore_trial2 | 20.0 | 2000 | 2000 | 2000 | True | 1152.738 | 1860.693 | 2071.245 |  |  |  |  |  |  |  |  |  | 1484.542 | 84.0 | 1124.964 |  |
| flink |  | identity | failure_changelog_restore_trial3 | 20.0 | 2000 | 2000 | 2000 | True | 1184.453 | 1894.897 | 2118.417 |  |  |  |  |  |  |  |  |  | 1445.075 | 112.0 | 1257.714 |  |
| flink |  | identity | failure_changelog_restore_trial4 | 20.0 | 2000 | 2000 | 2000 | True | 1160.331 | 1868.151 | 2541.707 |  |  |  |  |  |  |  |  |  | 1379.085 | 94.0 | 1078.62 |  |
| flink |  | identity | failure_changelog_restore_trial5 | 20.0 | 2000 | 2000 | 2000 | True | 1128.862 | 1875.392 | 2321.526 |  |  |  |  |  |  |  |  |  | 1525.227 | 109.0 | 1037.714 |  |
| flink |  | identity | failure_jvm_kill | 20.0 | 2000 | 2000 | 2000 | True | 1216.058 | 6917.511 | 10917.44 |  |  |  |  |  |  |  |  |  | 1862.086 | 10305.0 | 1118.757 |  |
| flink |  | identity | failure_jvm_kill_trial2 | 20.0 | 2000 | 2000 | 2000 | True | 1205.79 | 7147.897 | 11146.903 |  |  |  |  |  |  |  |  |  | 1397.935 | 10657.0 | 1079.26 |  |
| flink |  | identity | failure_jvm_kill_trial3 | 20.0 | 2000 | 2000 | 2000 | True | 1217.88 | 6852.813 | 10852.314 |  |  |  |  |  |  |  |  |  | 1458.636 | 10056.0 | 1043.566 |  |
| flink |  | identity | failure_jvm_kill_trial4 | 20.0 | 2000 | 2000 | 2000 | True | 1219.691 | 7157.812 | 11155.639 |  |  |  |  |  |  |  |  |  | 1520.66 | 10323.0 | 1071.656 |  |
| flink |  | identity | failure_jvm_kill_trial5 | 20.0 | 2000 | 2000 | 2000 | True | 1229.004 | 6929.838 | 10928.374 |  |  |  |  |  |  |  |  |  | 1385.933 | 10169.0 | 1043.65 |  |
| flink |  | identity | failure_kraft_failover | 20.0 | 2000 | 2000 | 2000 | True | 1105.452 | 1852.723 | 2342.289 |  |  |  |  |  |  |  |  |  | 1596.31 | 78.0 | 1104.319 |  |
| flink |  | identity | failure_kraft_failover_trial2 | 20.0 | 2000 | 2000 | 2000 | True | 1099.064 | 1804.273 | 2331.281 |  |  |  |  |  |  |  |  |  | 1401.791 | 85.0 | 1049.877 |  |
| flink |  | identity | failure_kraft_failover_trial3 | 20.0 | 2000 | 2000 | 2000 | True | 1097.942 | 1877.358 | 2327.341 |  |  |  |  |  |  |  |  |  | 1445.222 | 110.0 | 1138.538 |  |
| flink |  | identity | failure_kraft_failover_trial4 | 20.0 | 2000 | 2000 | 2000 | True | 1140.381 | 1883.285 | 2616.185 |  |  |  |  |  |  |  |  |  | 1560.844 | 110.0 | 1132.065 |  |
| flink |  | identity | failure_kraft_failover_trial5 | 20.0 | 2000 | 2000 | 2000 | True | 1117.247 | 1817.611 | 2330.55 |  |  |  |  |  |  |  |  |  | 1385.058 | 89.0 | 1061.554 |  |
| flink |  | identity | failure_node_loss | 20.0 | 2000 | 2000 | 2000 | True | 1199.588 | 6376.663 | 10376.616 |  |  |  |  |  |  |  |  |  | 1825.041 | 9556.0 | 1139.172 |  |
| flink |  | identity | failure_node_loss_trial2 | 20.0 | 2000 | 2000 | 2000 | True | 1213.794 | 7870.592 | 11869.81 |  |  |  |  |  |  |  |  |  | 1516.986 | 10879.0 | 1124.707 |  |
| flink |  | identity | failure_node_loss_trial3 | 20.0 | 2000 | 2000 | 2000 | True | 1252.071 | 6725.187 | 10724.169 |  |  |  |  |  |  |  |  |  | 1416.772 | 10224.0 | 1092.725 |  |
| flink |  | identity | failure_node_loss_trial4 | 20.0 | 2000 | 2000 | 2000 | True | 1200.645 | 6518.284 | 10518.167 |  |  |  |  |  |  |  |  |  | 1337.465 | 8456.0 | 1071.617 |  |
| flink |  | identity | failure_node_loss_trial5 | 20.0 | 2000 | 2000 | 2000 | True | 1149.515 | 7335.497 | 11334.636 |  |  |  |  |  |  |  |  |  | 1508.157 | 10587.0 | 1040.538 |  |
| flink |  | identity | failure_s3_throttling | 20.0 | 2000 | 2000 | 2000 | True | 1194.525 | 6795.101 | 10793.615 |  |  |  |  |  |  |  |  |  | 1606.144 | 10106.0 | 1036.746 |  |
| flink |  | identity | failure_s3_throttling_trial2 | 20.0 | 2000 | 2000 | 2000 | True | 1165.23 | 7590.738 | 11240.339 |  |  |  |  |  |  |  |  |  | 1566.361 | 9856.0 | 10930.968 |  |
| flink |  | identity | failure_s3_throttling_trial3 | 20.0 | 2000 | 2000 | 2000 | True | 1201.823 | 7262.291 | 10937.455 |  |  |  |  |  |  |  |  |  | 1390.679 | 9544.0 | 10628.762 |  |
| flink |  | identity | failure_s3_throttling_trial4 | 20.0 | 2000 | 2000 | 2000 | True | 1209.07 | 6920.494 | 10587.8 |  |  |  |  |  |  |  |  |  | 1513.91 | 10116.0 | 1098.738 |  |
| flink |  | identity | failure_s3_throttling_trial5 | 20.0 | 2000 | 2000 | 2000 | True | 1136.357 | 6903.379 | 10576.413 |  |  |  |  |  |  |  |  |  | 1412.119 | 9181.0 | 10267.281 |  |
| flink |  | sliding_sum | failure_broker_kill | 20.0 | 2001 | 1286 | 1286 | True | 3933.85 | 17133.598 | 21878.263 |  |  |  |  |  |  |  |  |  | 1002.769 | 20850.0 | 786.26 | 1784420828167.0 |
| flink |  | sliding_sum | failure_broker_kill_trial2 | 20.0 | 2001 | 1286 | 1286 | True | 3835.967 | 17181.446 | 21731.719 |  |  |  |  |  |  |  |  |  | 1002.826 | 21095.0 | 778.269 | 1784567235830.0 |
| flink |  | sliding_sum | failure_broker_kill_trial3 | 20.0 | 2001 | 1286 | 1286 | True | 3746.53 | 17091.966 | 21642.607 |  |  |  |  |  |  |  |  |  | 1001.808 | 21193.0 | 735.178 | 1784579669487.0 |
| flink |  | sliding_sum | failure_broker_kill_trial4 | 20.0 | 2001 | 1286 | 1286 | True | 4490.188 | 17695.423 | 22439.753 |  |  |  |  |  |  |  |  |  | 1001.472 | 20971.0 | 887.48 | 1784535313596.0 |
| flink |  | sliding_sum | failure_broker_kill_trial5 | 20.0 | 2001 | 1286 | 1286 | True | 4592.118 | 17791.469 | 22536.666 |  |  |  |  |  |  |  |  |  | 1002.227 | 21322.0 | 1064.409 | 1784561069456.0 |
| flink |  | sliding_sum | failure_changelog_restore | 20.0 | 2001 | 1286 | 1286 | True | 4150.718 | 17255.201 | 21806.153 |  |  |  |  |  |  |  |  |  | 1001.477 | 20828.0 | 915.736 | 1784417821681.0 |
| flink |  | sliding_sum | failure_changelog_restore_trial2 | 20.0 | 2001 | 1286 | 1286 | True | 4108.32 | 17188.006 | 21741.051 |  |  |  |  |  |  |  |  |  | 1001.461 | 21115.0 | 1120.241 | 1784543694248.0 |
| flink |  | sliding_sum | failure_changelog_restore_trial3 | 20.0 | 2001 | 1286 | 1286 | True | 3980.24 | 17325.973 | 21876.046 |  |  |  |  |  |  |  |  |  | 1002.855 | 21096.0 | 770.996 | 1784542958939.0 |
| flink |  | sliding_sum | failure_changelog_restore_trial4 | 20.0 | 2001 | 1286 | 1286 | True | 4205.813 | 17351.107 | 21901.324 |  |  |  |  |  |  |  |  |  | 1002.388 | 21037.0 | 1029.315 | 1784575885487.0 |
| flink |  | sliding_sum | failure_changelog_restore_trial5 | 20.0 | 2001 | 1286 | 1286 | True | 3912.356 | 17018.384 | 21570.767 |  |  |  |  |  |  |  |  |  | 1002.714 | 21065.0 | 999.302 | 1784534173245.0 |
| flink |  | sliding_sum | failure_jvm_kill | 20.0 | 5001 | 1789 | 1789 | True | 4405.761 | 12852.598 | 19563.717 |  |  |  |  |  |  |  |  |  | 1001.683 | 18071.0 | 985.676 | 1784513984591.0 |
| flink |  | sliding_sum | failure_jvm_kill_trial2 | 20.0 | 2001 | 1286 | 1286 | True | 4184.878 | 17575.895 | 22127.397 |  |  |  |  |  |  |  |  |  | 1001.595 | 21152.0 | 1100.096 | 1784532295309.0 |
| flink |  | sliding_sum | failure_jvm_kill_trial3 | 20.0 | 2001 | 1286 | 1286 | True | 4313.471 | 17561.823 | 22307.784 |  |  |  |  |  |  |  |  |  | 1001.701 | 21123.0 | 902.205 | 1784525788774.0 |
| flink |  | sliding_sum | failure_jvm_kill_trial4 | 20.0 | 2001 | 1286 | 1286 | True | 4306.336 | 17555.414 | 22300.735 |  |  |  |  |  |  |  |  |  | 1001.488 | 21033.0 | 876.075 | 1784565328907.0 |
| flink |  | sliding_sum | failure_jvm_kill_trial5 | 20.0 | 2001 | 1286 | 1286 | True | 4232.63 | 17432.553 | 22177.133 |  |  |  |  |  |  |  |  |  | 1001.479 | 21139.0 | 1042.812 | 1784576285768.0 |
| flink |  | sliding_sum | failure_kraft_failover | 20.0 | 2001 | 1286 | 1286 | True | 4116.332 | 17461.629 | 22012.82 |  |  |  |  |  |  |  |  |  | 1002.163 | 20870.0 | 815.582 | 1784407912546.0 |
| flink |  | sliding_sum | failure_kraft_failover_trial2 | 20.0 | 2001 | 1286 | 1286 | True | 4006.449 | 17349.326 | 21901.447 |  |  |  |  |  |  |  |  |  | 1002.964 | 21151.0 | 1002.135 | 1784564929560.0 |
| flink |  | sliding_sum | failure_kraft_failover_trial3 | 20.0 | 2001 | 1286 | 1286 | True | 4210.639 | 17367.811 | 22107.648 |  |  |  |  |  |  |  |  |  | 1001.73 | 20194.0 | 965.611 | 1784585828203.0 |
| flink |  | sliding_sum | failure_kraft_failover_trial4 | 20.0 | 2001 | 1286 | 1286 | True | 3971.601 | 17021.744 | 21573.838 |  |  |  |  |  |  |  |  |  | 1002.276 | 21275.0 | 943.883 | 1784578556400.0 |
| flink |  | sliding_sum | failure_kraft_failover_trial5 | 20.0 | 2001 | 1286 | 1286 | True | 3953.402 | 17299.927 | 21850.278 |  |  |  |  |  |  |  |  |  | 1002.633 | 21168.0 | 818.429 | 1784588488655.0 |
| flink |  | sliding_sum | failure_node_loss | 20.0 | 5001 | 42275 | 42275 | True | 71587.559 | 203236.078 | 236635.271 |  |  |  |  |  |  |  |  |  | 1002.351 | 235339.0 | 937.362 | 1784508536616.0 |
| flink |  | sliding_sum | failure_node_loss_trial2 | 20.0 | 2001 | 1286 | 1286 | True | 4114.518 | 17264.226 | 22006.105 |  |  |  |  |  |  |  |  |  | 1002.476 | 20964.0 | 1032.018 | 1784530732888.0 |
| flink |  | sliding_sum | failure_node_loss_trial3 | 20.0 | 2001 | 1286 | 1286 | True | 4473.674 | 17723.703 | 22468.35 |  |  |  |  |  |  |  |  |  | 1001.858 | 21113.0 | 952.344 | 1784581599898.0 |
| flink |  | sliding_sum | failure_node_loss_trial4 | 20.0 | 2001 | 1286 | 1286 | True | 3961.897 | 17185.403 | 21736.412 |  |  |  |  |  |  |  |  |  | 1002.07 | 21066.0 | 1063.63 | 1784539104971.0 |
| flink |  | sliding_sum | failure_node_loss_trial5 | 20.0 | 2001 | 1286 | 1286 | True | 4124.217 | 17452.524 | 22003.976 |  |  |  |  |  |  |  |  |  | 1001.328 | 20996.0 | 1083.576 | 1784571027802.0 |
| flink |  | sliding_sum | failure_s3_throttling | 20.0 | 2001 | 1286 | 1286 | True | 4423.144 | 17176.447 | 21726.781 |  |  |  |  |  |  |  |  |  | 1001.802 | 20943.0 | 958.156 | 1784396765766.0 |
| flink |  | sliding_sum | failure_s3_throttling_trial2 | 20.0 | 2001 | 1286 | 1286 | True | 4453.558 | 17654.268 | 22400.336 |  |  |  |  |  |  |  |  |  | 1002.284 | 20490.0 | 1049.953 | 1784527318676.0 |
| flink |  | sliding_sum | failure_s3_throttling_trial3 | 20.0 | 2001 | 1286 | 1286 | True | 4061.229 | 17402.579 | 21954.0 |  |  |  |  |  |  |  |  |  | 1001.704 | 21109.0 | 817.798 | 1784557701199.0 |
| flink |  | sliding_sum | failure_s3_throttling_trial4 | 20.0 | 2001 | 1286 | 1286 | True | 3988.528 | 17118.403 | 21669.244 |  |  |  |  |  |  |  |  |  | 1001.152 | 20944.0 | 898.436 | 1784563389124.0 |
| flink |  | sliding_sum | failure_s3_throttling_trial5 | 20.0 | 2001 | 1286 | 1286 | True | 4330.901 | 17624.473 | 22174.794 |  |  |  |  |  |  |  |  |  | 1001.831 | 21190.0 | 1109.981 | 1784589598310.0 |
| flink |  | tumbling_count | failure_broker_kill | 20.0 | 2001 | 386 | 386 | True | 4423.342 | 13670.409 | 23071.189 |  |  |  |  |  |  |  |  |  | 1003.023 | 22154.0 | 771.931 | 1784413239768.0 |
| flink |  | tumbling_count | failure_broker_kill_trial2 | 20.0 | 2001 | 386 | 386 | True | 4525.475 | 13750.297 | 23284.491 |  |  |  |  |  |  |  |  |  | 1002.282 | 22268.0 | 1078.237 | 1784540505692.0 |
| flink |  | tumbling_count | failure_broker_kill_trial3 | 20.0 | 2001 | 386 | 386 | True | 4144.911 | 13441.792 | 22705.976 |  |  |  |  |  |  |  |  |  | 1002.223 | 21171.0 | 684.115 | 1784589827960.0 |
| flink |  | tumbling_count | failure_broker_kill_trial4 | 20.0 | 2001 | 386 | 386 | True | 4278.249 | 13358.589 | 22959.377 |  |  |  |  |  |  |  |  |  | 1001.533 | 22183.0 | 842.455 | 1784523323488.0 |
| flink |  | tumbling_count | failure_broker_kill_trial5 | 20.0 | 2001 | 386 | 386 | True | 4232.695 | 13415.524 | 22786.102 |  |  |  |  |  |  |  |  |  | 1002.066 | 21155.0 | 850.402 | 1784581829395.0 |
| flink |  | tumbling_count | failure_changelog_restore | 20.0 | 2001 | 386 | 386 | True | 4250.825 | 13317.467 | 22873.588 |  |  |  |  |  |  |  |  |  | 1002.208 | 22215.0 | 841.796 | 1784402192021.0 |
| flink |  | tumbling_count | failure_changelog_restore_trial2 | 20.0 | 2001 | 386 | 386 | True | 4559.293 | 13452.823 | 22890.956 |  |  |  |  |  |  |  |  |  | 1001.375 | 22080.0 | 1025.103 | 1784579526685.0 |
| flink |  | tumbling_count | failure_changelog_restore_trial3 | 20.0 | 2001 | 386 | 386 | True | 4357.154 | 13638.57 | 22908.433 |  |  |  |  |  |  |  |  |  | 1002.008 | 22177.0 | 733.704 | 1784557530662.0 |
| flink |  | tumbling_count | failure_changelog_restore_trial4 | 20.0 | 2001 | 386 | 386 | True | 4434.683 | 13642.695 | 23035.386 |  |  |  |  |  |  |  |  |  | 1002.938 | 21205.0 | 951.301 | 1784535543095.0 |
| flink |  | tumbling_count | failure_changelog_restore_trial5 | 20.0 | 2001 | 386 | 386 | True | 4273.702 | 13423.705 | 22866.758 |  |  |  |  |  |  |  |  |  | 1002.437 | 22107.0 | 956.368 | 1784542428018.0 |
| flink |  | tumbling_count | failure_jvm_kill | 20.0 | 5001 | 886 | 886 | True | 4773.933 | 15083.168 | 21720.375 |  |  |  |  |  |  |  |  |  | 1002.615 | 20079.0 | 1087.752 | 1784513885213.0 |
| flink |  | tumbling_count | failure_jvm_kill_trial2 | 20.0 | 2001 | 386 | 386 | True | 4449.409 | 13320.619 | 22751.906 |  |  |  |  |  |  |  |  |  | 1002.614 | 22127.0 | 969.796 | 1784563195657.0 |
| flink |  | tumbling_count | failure_jvm_kill_trial3 | 20.0 | 2001 | 386 | 386 | True | 4847.601 | 13990.064 | 23471.169 |  |  |  |  |  |  |  |  |  | 1002.364 | 22153.0 | 1107.941 | 1784539333965.0 |
| flink |  | tumbling_count | failure_jvm_kill_trial4 | 20.0 | 2001 | 386 | 386 | True | 4228.479 | 13106.984 | 22549.505 |  |  |  |  |  |  |  |  |  | 1002.466 | 21862.0 | 781.996 | 1784529785626.0 |
| flink |  | tumbling_count | failure_jvm_kill_trial5 | 20.0 | 2001 | 386 | 386 | True | 4280.899 | 13532.385 | 22881.043 |  |  |  |  |  |  |  |  |  | 1003.096 | 21232.0 | 704.628 | 1784568570710.0 |
| flink |  | tumbling_count | failure_kraft_failover | 20.0 | 2001 | 386 | 386 | True | 4306.946 | 13506.799 | 22938.01 |  |  |  |  |  |  |  |  |  | 1001.931 | 22103.0 | 986.894 | 1784423280825.0 |
| flink |  | tumbling_count | failure_kraft_failover_trial2 | 20.0 | 2001 | 386 | 386 | True | 4304.516 | 13570.08 | 22868.505 |  |  |  |  |  |  |  |  |  | 1003.206 | 21058.0 | 920.307 | 1784553769743.0 |
| flink |  | tumbling_count | failure_kraft_failover_trial3 | 20.0 | 2001 | 386 | 386 | True | 3965.383 | 12822.478 | 22302.572 |  |  |  |  |  |  |  |  |  | 1002.322 | 21889.0 | 551.92 | 1784566700905.0 |
| flink |  | tumbling_count | failure_kraft_failover_trial4 | 20.0 | 2001 | 386 | 386 | True | 4491.138 | 13769.814 | 23063.44 |  |  |  |  |  |  |  |  |  | 1002.96 | 21214.0 | 909.434 | 1784547684231.0 |
| flink |  | tumbling_count | failure_kraft_failover_trial5 | 20.0 | 2001 | 386 | 386 | True | 4019.961 | 13169.875 | 22569.94 |  |  |  |  |  |  |  |  |  | 1002.629 | 21062.0 | 729.905 | 1784552636068.0 |
| flink |  | tumbling_count | failure_node_loss | 20.0 | 5001 | 4871 | 4871 | True | 15532.82 | 29759.111 | 31145.943 |  |  |  |  |  |  |  |  |  | 1002.682 | 29602.0 | 1048.891 | 1784505256214.0 |
| flink |  | tumbling_count | failure_node_loss_trial2 | 20.0 | 2001 | 386 | 386 | True | 4456.742 | 13329.281 | 22756.545 |  |  |  |  |  |  |  |  |  | 1002.943 | 21155.0 | 949.897 | 1784559431953.0 |
| flink |  | tumbling_count | failure_node_loss_trial3 | 20.0 | 2001 | 386 | 386 | True | 4311.465 | 13461.561 | 22419.785 |  |  |  |  |  |  |  |  |  | 1002.032 | 21088.0 | 1086.402 | 1784540105200.0 |
| flink |  | tumbling_count | failure_node_loss_trial4 | 20.0 | 2001 | 386 | 386 | True | 4500.873 | 13346.743 | 22824.371 |  |  |  |  |  |  |  |  |  | 1002.998 | 21209.0 | 1051.732 | 1784590569375.0 |
| flink |  | tumbling_count | failure_node_loss_trial5 | 20.0 | 2001 | 386 | 386 | True | 4333.5 | 13483.971 | 22910.885 |  |  |  |  |  |  |  |  |  | 1002.722 | 21206.0 | 1005.318 | 1784530562294.0 |
| flink |  | tumbling_count | failure_s3_throttling | 20.0 | 2001 | 386 | 386 | True | 6172.28 | 17707.351 | 26906.227 |  |  |  |  |  |  |  |  |  | 1001.687 | 25912.0 | 899.637 | 1784419537521.0 |
| flink |  | tumbling_count | failure_s3_throttling_trial2 | 20.0 | 2001 | 386 | 386 | True | 4220.212 | 13337.97 | 22880.429 |  |  |  |  |  |  |  |  |  | 1002.255 | 21149.0 | 1030.989 | 1784587589923.0 |
| flink |  | tumbling_count | failure_s3_throttling_trial3 | 20.0 | 2001 | 386 | 386 | True | 4362.488 | 13259.985 | 22690.744 |  |  |  |  |  |  |  |  |  | 1002.953 | 21127.0 | 1003.488 | 1784574257598.0 |
| flink |  | tumbling_count | failure_s3_throttling_trial4 | 20.0 | 2001 | 386 | 386 | True | 4514.865 | 13385.639 | 22851.519 |  |  |  |  |  |  |  |  |  | 1002.998 | 21071.0 | 1128.542 | 1784571256246.0 |
| flink |  | tumbling_count | failure_s3_throttling_trial5 | 20.0 | 2001 | 386 | 386 | True | 4384.799 | 13141.924 | 22721.997 |  |  |  |  |  |  |  |  |  | 1003.073 | 21158.0 | 937.643 | 1784535142602.0 |
| flink | w1 | identity |  | 20.0 | 100 | 100 | 100 | True | 1668.467 | 3288.828 | 3488.793 |  |  |  |  |  |  |  |  |  |  |  |  |  |
| flink | w1 | identity | rate10 | 10.0 | 100 | 100 | 100 | True | 1260.707 | 2060.664 | 2460.697 |  |  |  |  |  |  |  |  |  |  |  |  |  |
| flink | w1 | identity | rate40 | 40.0 | 100 | 100 | 100 | True | 1990.019 | 3114.001 | 3214.316 |  |  |  |  |  |  |  |  |  |  |  |  |  |
| flink | w1 | identity | repeat1 | 20.0 | 100 | 100 | 100 | True | 948.056 | 2447.962 | 2647.962 |  |  |  |  |  |  |  |  |  |  |  |  |  |
| flink | w1 | identity | stability_100 | 100.0 | 180000 | 180000 | 180000 | True | 1012.721 | 1702.92 | 1882.096 |  |  |  |  |  |  |  |  |  |  |  |  |  |
| flink | w2 | filter_map |  | 20.0 | 100 | 56 | 56 | True | 1400.69 | 3200.614 | 3350.702 |  |  |  |  |  |  |  |  |  |  |  |  |  |
| flink | w2 | filter_map | rate10 | 10.0 | 100 | 56 | 56 | True | 1332.854 | 2332.809 | 2632.06 |  |  |  |  |  |  |  |  |  |  |  |  |  |
| flink | w2 | filter_map | rate40 | 40.0 | 100 | 56 | 56 | True | 2088.45 | 3213.16 | 3287.256 |  |  |  |  |  |  |  |  |  |  |  |  |  |
| flink | w2 | filter_map | repeat1 | 20.0 | 100 | 56 | 56 | True | 900.849 | 2611.81 | 2761.075 |  |  |  |  |  |  |  |  |  |  |  |  |  |
| flink | w2 | filter_map | stability_100 | 100.0 | 180000 | 89906 | 89906 | True | 1007.794 | 1688.498 | 1874.898 |  |  |  |  |  |  |  |  |  |  |  |  |  |
| flink | w3 | tumbling_count |  | 20.0 | 101 | 71 | 71 | True | 2167.446 | 5068.23 | 5317.375 |  |  |  |  |  |  |  |  |  |  |  |  |  |
| flink | w3 | tumbling_count | rate10 | 10.0 | 101 | 71 | 71 | True | 4228.258 | 10028.509 | 10527.388 |  |  |  |  |  |  |  |  |  |  |  |  |  |
| flink | w3 | tumbling_count | rate40 | 40.0 | 101 | 71 | 71 | True | 2077.073 | 3527.95 | 3651.222 |  |  |  |  |  |  |  |  |  |  |  |  |  |
| flink | w3 | tumbling_count | repeat1 | 20.0 | 101 | 71 | 71 | True | 2509.091 | 5409.743 | 5658.188 |  |  |  |  |  |  |  |  |  |  |  |  |  |
| flink | w3 | tumbling_count | stability_100 | 100.0 | 180001 | 29924 | 29924 | True | 1789.284 | 4084.197 | 5506.696 |  |  |  |  |  |  |  |  |  |  |  |  |  |
| flink | w3 | tumbling_count | tuning_checkpoint10s | 20.0 | 101 | 71 | 71 | True | 4993.134 | 7893.233 | 8142.178 |  |  |  |  |  |  |  |  |  |  |  |  |  |
| flink | w3 | tumbling_count | tuning_control | 20.0 | 101 | 71 | 71 | True | 2293.651 | 5194.313 | 5442.462 |  |  |  |  |  |  |  |  |  |  |  |  |  |
| flink | w4 | sliding_sum |  | 20.0 | 101 | 710 | 710 | True | 2638.442 | 5538.424 | 5786.186 |  |  |  |  |  |  |  |  |  |  |  |  |  |
| flink | w4 | sliding_sum | rate10 | 10.0 | 101 | 710 | 710 | True | 4091.697 | 9891.631 | 10390.27 |  |  |  |  |  |  |  |  |  |  |  |  |  |
| flink | w4 | sliding_sum | rate40 | 40.0 | 101 | 710 | 710 | True | 1965.928 | 3415.926 | 3540.11 |  |  |  |  |  |  |  |  |  |  |  |  |  |
| flink | w4 | sliding_sum | repeat1 | 20.0 | 101 | 710 | 710 | True | 2359.98 | 5259.953 | 5507.883 |  |  |  |  |  |  |  |  |  |  |  |  |  |
| flink | w4 | sliding_sum | stability_100 | 100.0 | 180001 | 30900 | 30900 | True | 1840.766 | 4179.25 | 5751.375 |  |  |  |  |  |  |  |  |  |  |  |  |  |
| flink | w5 | stream_stream_join |  | 20.0 | 200 | 186 | 186 | True | 947.613 | 2323.925 | 3024.034 |  |  |  |  |  |  |  |  |  | 2263.375 | 81.0 | 687.67 | 1784429719369.0 |
| flink | w5 | stream_stream_join | rate10 | 10.0 | 200 | 186 | 186 | True | 1135.848 | 2039.22 | 2439.144 |  |  |  |  |  |  |  |  |  |  |  |  |  |
| flink | w5 | stream_stream_join | rate40 | 40.0 | 200 | 186 | 186 | True | 1531.194 | 2705.081 | 2805.058 |  |  |  |  |  |  |  |  |  |  |  |  |  |
| flink | w5 | stream_stream_join | repeat1 | 20.0 | 200 | 186 | 186 | True | 1547.947 | 3059.302 | 3259.235 |  |  |  |  |  |  |  |  |  |  |  |  |  |
| kafka_streams |  | identity | failure_broker_kill | 20.0 | 2000 | 2000 | 2000 | True | 1230.207 | 1780.979 | 2063.339 |  |  |  |  |  |  |  |  |  | 1594.694 | 61.0 | 834.607 |  |
| kafka_streams |  | identity | failure_broker_kill_trial2 | 20.0 | 2000 | 2000 | 2000 | True | 717.534 | 1221.131 | 2563.624 |  |  |  |  |  |  |  |  |  | 1386.675 | 112.0 | 1115.681 |  |
| kafka_streams |  | identity | failure_broker_kill_trial3 | 20.0 | 2000 | 2000 | 2000 | True | 1277.877 | 1969.066 | 2417.79 |  |  |  |  |  |  |  |  |  | 1413.392 | 49.0 | 1118.016 |  |
| kafka_streams |  | identity | failure_broker_kill_trial4 | 20.0 | 2000 | 2000 | 2000 | True | 1526.763 | 2025.822 | 2615.64 |  |  |  |  |  |  |  |  |  | 1553.599 | 67.0 | 1068.76 |  |
| kafka_streams |  | identity | failure_broker_kill_trial5 | 20.0 | 2000 | 2000 | 2000 | True | 889.703 | 1839.494 | 2558.769 |  |  |  |  |  |  |  |  |  | 1600.044 | 50.0 | 1239.398 |  |
| kafka_streams |  | identity | failure_changelog_restore | 20.0 | 2000 | 2000 | 2000 | True | 927.86 | 1421.504 | 1997.848 |  |  |  |  |  |  |  |  |  | 1543.82 | 59.0 | 437.479 |  |
| kafka_streams |  | identity | failure_changelog_restore_trial2 | 20.0 | 2000 | 2000 | 2000 | True | 1427.31 | 1924.194 | 2448.514 |  |  |  |  |  |  |  |  |  | 1551.877 | 82.0 | 968.012 |  |
| kafka_streams |  | identity | failure_changelog_restore_trial3 | 20.0 | 2000 | 2000 | 2000 | True | 1227.337 | 1721.568 | 2046.926 |  |  |  |  |  |  |  |  |  | 1352.929 | 67.0 | 735.793 |  |
| kafka_streams |  | identity | failure_changelog_restore_trial4 | 20.0 | 2000 | 2000 | 2000 | True | 1527.692 | 2022.529 | 2431.112 |  |  |  |  |  |  |  |  |  | 1356.219 | 50.0 | 1034.007 |  |
| kafka_streams |  | identity | failure_changelog_restore_trial5 | 20.0 | 2000 | 2000 | 2000 | True | 1227.064 | 1975.024 | 2413.475 |  |  |  |  |  |  |  |  |  | 1427.466 | 60.0 | 1048.67 |  |
| kafka_streams |  | identity | failure_jvm_kill | 20.0 | 2000 | 2000 | 2000 | True | 1386.088 | 39541.017 | 43540.175 |  |  |  |  |  |  |  |  |  | 1474.199 | 42373.0 | 1136.974 |  |
| kafka_streams |  | identity | failure_jvm_kill_trial2 | 20.0 | 2000 | 2000 | 2000 | True | 1527.186 | 40762.53 | 44762.108 |  |  |  |  |  |  |  |  |  | 1422.727 | 43571.0 | 1153.385 |  |
| kafka_streams |  | identity | failure_jvm_kill_trial3 | 20.0 | 2000 | 2000 | 2000 | True | 1589.919 | 41250.198 | 45249.707 |  |  |  |  |  |  |  |  |  | 1419.843 | 44127.0 | 1099.808 |  |
| kafka_streams |  | identity | failure_jvm_kill_trial4 | 20.0 | 2000 | 2000 | 2000 | True | 1434.529 | 40664.463 | 44663.921 |  |  |  |  |  |  |  |  |  | 1693.006 | 43554.0 | 1088.912 |  |
| kafka_streams |  | identity | failure_jvm_kill_trial5 | 20.0 | 2000 | 2000 | 2000 | True | 1738.106 | 40227.867 | 44227.41 |  |  |  |  |  |  |  |  |  | 1393.185 | 43204.0 | 1036.962 |  |
| kafka_streams |  | identity | failure_kraft_failover | 20.0 | 2000 | 2000 | 2000 | True | 827.901 | 1323.792 | 2901.506 |  |  |  |  |  |  |  |  |  | 1591.074 | 279.0 | 1077.924 |  |
| kafka_streams |  | identity | failure_kraft_failover_trial2 | 20.0 | 2000 | 2000 | 2000 | True | 1377.564 | 1897.745 | 2099.961 |  |  |  |  |  |  |  |  |  | 1397.098 | 61.0 | 1067.671 |  |
| kafka_streams |  | identity | failure_kraft_failover_trial3 | 20.0 | 2000 | 2000 | 2000 | True | 1425.161 | 1922.836 | 2176.063 |  |  |  |  |  |  |  |  |  | 1392.793 | 62.0 | 978.77 |  |
| kafka_streams |  | identity | failure_kraft_failover_trial4 | 20.0 | 2000 | 2000 | 2000 | True | 1381.363 | 1882.873 | 2116.25 |  |  |  |  |  |  |  |  |  | 1410.1 | 62.0 | 935.152 |  |
| kafka_streams |  | identity | failure_kraft_failover_trial5 | 20.0 | 2000 | 2000 | 2000 | True | 1501.951 | 1996.156 | 2360.001 |  |  |  |  |  |  |  |  |  | 1383.772 | 60.0 | 1092.736 |  |
| kafka_streams |  | identity | failure_node_loss | 20.0 | 2000 | 2000 | 2000 | True | 1482.897 | 39185.105 | 43184.776 |  |  |  |  |  |  |  |  |  | 1472.06 | 42202.0 | 1046.782 |  |
| kafka_streams |  | identity | failure_node_loss_trial2 | 20.0 | 2000 | 2000 | 2000 | True | 1873.22 | 40340.164 | 44340.1 |  |  |  |  |  |  |  |  |  | 1423.368 | 43158.0 | 1160.737 |  |
| kafka_streams |  | identity | failure_node_loss_trial3 | 20.0 | 2000 | 2000 | 2000 | True | 1821.818 | 39265.708 | 43265.319 |  |  |  |  |  |  |  |  |  | 1457.334 | 42213.0 | 1063.883 |  |
| kafka_streams |  | identity | failure_node_loss_trial4 | 20.0 | 2000 | 2000 | 2000 | True | 1878.796 | 40051.482 | 44050.919 |  |  |  |  |  |  |  |  |  | 1428.633 | 42939.0 | 1186.882 |  |
| kafka_streams |  | identity | failure_node_loss_trial5 | 20.0 | 2000 | 2000 | 2000 | True | 1630.049 | 38759.823 | 42758.298 |  |  |  |  |  |  |  |  |  | 1354.85 | 41657.0 | 1073.455 |  |
| kafka_streams |  | identity | failure_s3_throttling | 20.0 | 2000 | 2000 | 2000 | True | 1219.519 | 1920.316 | 2036.042 |  |  |  |  |  |  |  |  |  | 1521.352 | 71.0 | 1032.651 |  |
| kafka_streams |  | identity | failure_s3_throttling_trial2 | 20.0 | 2000 | 2000 | 2000 | True | 1428.425 | 1922.945 | 2304.927 |  |  |  |  |  |  |  |  |  | 1421.766 | 37.0 | 946.236 |  |
| kafka_streams |  | identity | failure_s3_throttling_trial3 | 20.0 | 2000 | 2000 | 2000 | True | 1527.77 | 2022.926 | 2385.796 |  |  |  |  |  |  |  |  |  | 1408.738 | 73.0 | 1039.135 |  |
| kafka_streams |  | identity | failure_s3_throttling_trial4 | 20.0 | 2000 | 2000 | 2000 | True | 1329.972 | 1824.931 | 2229.745 |  |  |  |  |  |  |  |  |  | 1449.905 | 60.0 | 840.569 |  |
| kafka_streams |  | identity | failure_s3_throttling_trial5 | 20.0 | 2000 | 2000 | 2000 | True | 642.675 | 1132.501 | 2648.648 |  |  |  |  |  |  |  |  |  | 1483.11 | 101.0 | 1108.538 |  |
| kafka_streams |  | sliding_sum | failure_broker_kill | 20.0 | 2001 | 1286 | 1286 | True | 4926.381 | 18126.06 | 22871.165 |  |  |  |  |  |  |  |  |  | 1001.388 | 22239.0 | 86.848 | 1784415261644.0 |
| kafka_streams |  | sliding_sum | failure_broker_kill_trial2 | 20.0 | 2001 | 1286 | 1286 | True | 6112.675 | 18921.873 | 23492.978 |  |  |  |  |  |  |  |  |  | 1001.314 | 22907.0 | 126.021 | 1784561837926.0 |
| kafka_streams |  | sliding_sum | failure_broker_kill_trial3 | 20.0 | 2001 | 1286 | 1286 | True | 5439.742 | 20170.166 | 23869.98 |  |  |  |  |  |  |  |  |  | 1002.575 | 23174.0 | 95.406 | 1784569902681.0 |
| kafka_streams |  | sliding_sum | failure_broker_kill_trial4 | 20.0 | 2001 | 1286 | 1286 | True | 5596.485 | 21296.698 | 25427.757 |  |  |  |  |  |  |  |  |  | 1001.67 | 24483.0 | 97.063 | 1784545228670.0 |
| kafka_streams |  | sliding_sum | failure_broker_kill_trial5 | 20.0 | 2001 | 1286 | 1286 | True | 6151.717 | 18959.015 | 23511.294 |  |  |  |  |  |  |  |  |  | 1002.821 | 23128.0 | 125.839 | 1784548991573.0 |
| kafka_streams |  | sliding_sum | failure_changelog_restore | 20.0 | 2001 | 1286 | 1286 | True | 6946.469 | 20440.893 | 24091.369 |  |  |  |  |  |  |  |  |  | 1002.649 | 22672.0 | 1075.872 | 1784421935162.0 |
| kafka_streams |  | sliding_sum | failure_changelog_restore_trial2 | 20.0 | 2001 | 1286 | 1286 | True | 6600.484 | 20103.388 | 23755.328 |  |  |  |  |  |  |  |  |  | 1001.844 | 22491.0 | 1069.115 | 1784570256654.0 |
| kafka_streams |  | sliding_sum | failure_changelog_restore_trial3 | 20.0 | 2001 | 1286 | 1286 | True | 6826.928 | 20325.722 | 23977.4 |  |  |  |  |  |  |  |  |  | 1001.468 | 23361.0 | 1066.147 | 1784547850291.0 |
| kafka_streams |  | sliding_sum | failure_changelog_restore_trial4 | 20.0 | 2001 | 1286 | 1286 | True | 6634.291 | 20138.929 | 23791.183 |  |  |  |  |  |  |  |  |  | 1001.938 | 22383.0 | 1068.851 | 1784549378652.0 |
| kafka_streams |  | sliding_sum | failure_changelog_restore_trial5 | 20.0 | 2001 | 1286 | 1286 | True | 7224.551 | 20728.452 | 24380.251 |  |  |  |  |  |  |  |  |  | 1001.721 | 23119.0 | 1061.284 | 1784563784975.0 |
| kafka_streams |  | sliding_sum | failure_jvm_kill_trial2 | 20.0 | 2001 | 1286 | 1286 | False | 6088.723 | 42121.486 | 49771.008 |  |  |  |  |  |  |  |  |  | 1002.785 | 48739.0 | 137.167 | 1784609369809.0 |
| kafka_streams |  | sliding_sum | failure_jvm_kill_trial3 | 20.0 | 2001 | 1286 | 1286 | False | 5401.438 | 42669.66 | 50319.623 |  |  |  |  |  |  |  |  |  | 1002.793 | 49236.0 | 136.806 | 1784613824799.0 |
| kafka_streams |  | sliding_sum | failure_jvm_kill_trial4 | 20.0 | 2001 | 1286 | 1286 | False | 6285.903 | 42540.638 | 50190.574 |  |  |  |  |  |  |  |  |  | 1001.979 | 49132.0 | 160.553 | 1784607129626.0 |
| kafka_streams |  | sliding_sum | failure_jvm_kill_trial5 | 20.0 | 2001 | 1286 | 1286 | False | 6209.553 | 42974.221 | 50623.837 |  |  |  |  |  |  |  |  |  | 1001.381 | 49858.0 | 165.414 | 1784605492389.0 |
| kafka_streams |  | sliding_sum | failure_kraft_failover | 20.0 | 2001 | 1286 | 1286 | True | 5254.974 | 18462.382 | 23204.108 |  |  |  |  |  |  |  |  |  | 1001.768 | 22598.0 | 129.725 | 1784420055559.0 |
| kafka_streams |  | sliding_sum | failure_kraft_failover_trial2 | 20.0 | 2001 | 1286 | 1286 | True | 5251.131 | 18508.206 | 22923.318 |  |  |  |  |  |  |  |  |  | 1001.378 | 22127.0 | 128.912 | 1784559577072.0 |
| kafka_streams |  | sliding_sum | failure_kraft_failover_trial3 | 20.0 | 2001 | 1286 | 1286 | True | 5506.646 | 18314.741 | 22865.736 |  |  |  |  |  |  |  |  |  | 1002.799 | 22496.0 | 116.393 | 1784538004444.0 |
| kafka_streams |  | sliding_sum | failure_kraft_failover_trial4 | 20.0 | 2001 | 1286 | 1286 | True | 5960.54 | 18765.523 | 23317.369 |  |  |  |  |  |  |  |  |  | 1002.29 | 22488.0 | 110.576 | 1784521994271.0 |
| kafka_streams |  | sliding_sum | failure_kraft_failover_trial5 | 20.0 | 2001 | 1286 | 1286 | True | 5919.795 | 18725.056 | 23275.91 |  |  |  |  |  |  |  |  |  | 1002.798 | 22910.0 | 94.466 | 1784531930858.0 |
| kafka_streams |  | sliding_sum | failure_node_loss_trial2 | 20.0 | 2001 | 1286 | 1286 | False | 6065.196 | 42687.238 | 50336.774 |  |  |  |  |  |  |  |  |  | 1001.489 | 49529.0 | 157.177 | 1784604945961.0 |
| kafka_streams |  | sliding_sum | failure_node_loss_trial3 | 20.0 | 2001 | 1286 | 1286 | True | 5745.258 | 42905.044 | 50555.039 |  |  |  |  |  |  |  |  |  | 1001.606 | 49483.0 | 124.422 | 1784613279319.0 |
| kafka_streams |  | sliding_sum | failure_node_loss_trial4 | 20.0 | 2001 | 1286 | 1286 | False | 6251.548 | 42216.342 | 49865.948 |  |  |  |  |  |  |  |  |  | 1001.553 | 48944.0 | 176.218 | 1784608249815.0 |
| kafka_streams |  | sliding_sum | failure_node_loss_trial5 | 20.0 | 2001 | 1286 | 1286 | False | 5731.49 | 42869.987 | 50519.873 |  |  |  |  |  |  |  |  |  | 1002.912 | 49575.0 | 96.945 | 1784611583993.0 |
| kafka_streams |  | sliding_sum | failure_s3_throttling | 20.0 | 2001 | 1286 | 1286 | True | 4945.06 | 18107.676 | 22658.528 |  |  |  |  |  |  |  |  |  | 1001.518 | 22035.0 | 84.052 | 1784416714309.0 |
| kafka_streams |  | sliding_sum | failure_s3_throttling_trial2 | 20.0 | 2001 | 1286 | 1286 | True | 5141.055 | 18341.812 | 23083.788 |  |  |  |  |  |  |  |  |  | 1002.856 | 22660.0 | 133.464 | 1784580065177.0 |
| kafka_streams |  | sliding_sum | failure_s3_throttling_trial3 | 20.0 | 2001 | 1286 | 1286 | True | 5412.4 | 18612.782 | 23358.239 |  |  |  |  |  |  |  |  |  | 1001.215 | 23044.0 | 112.45 | 1784558067956.0 |
| kafka_streams |  | sliding_sum | failure_s3_throttling_trial4 | 20.0 | 2001 | 1286 | 1286 | True | 4686.1 | 17841.885 | 22583.946 |  |  |  |  |  |  |  |  |  | 1001.395 | 22259.0 | 76.428 | 1784540301042.0 |
| kafka_streams |  | sliding_sum | failure_s3_throttling_trial5 | 20.0 | 2001 | 1286 | 1286 | True | 4978.402 | 18328.399 | 22879.97 |  |  |  |  |  |  |  |  |  | 1001.77 | 22356.0 | 78.631 | 1784524254997.0 |
| kafka_streams |  | tumbling_count | failure_broker_kill | 20.0 | 2001 | 386 | 386 | True | 5267.266 | 14243.809 | 23750.781 |  |  |  |  |  |  |  |  |  | 1002.171 | 22693.0 | 79.325 | 1784421039895.0 |
| kafka_streams |  | tumbling_count | failure_broker_kill_trial2 | 20.0 | 2001 | 386 | 386 | True | 5337.986 | 14680.936 | 24074.641 |  |  |  |  |  |  |  |  |  | 1002.201 | 23089.0 | 54.815 | 1784545450540.0 |
| kafka_streams |  | tumbling_count | failure_broker_kill_trial3 | 20.0 | 2001 | 386 | 386 | True | 6681.784 | 17526.688 | 26726.598 |  |  |  |  |  |  |  |  |  | 1002.754 | 26302.0 | 127.018 | 1784581066574.0 |
| kafka_streams |  | tumbling_count | failure_broker_kill_trial4 | 20.0 | 2001 | 386 | 386 | True | 6714.564 | 23214.842 | 33014.432 |  |  |  |  |  |  |  |  |  | 1003.16 | 32648.0 | 66.907 | 1784549203502.0 |
| kafka_streams |  | tumbling_count | failure_broker_kill_trial5 | 20.0 | 2001 | 386 | 386 | True | 7272.145 | 23112.905 | 32562.328 |  |  |  |  |  |  |  |  |  | 1001.861 | 32055.0 | 111.439 | 1784526768484.0 |
| kafka_streams |  | tumbling_count | failure_changelog_restore | 20.0 | 2001 | 386 | 386 | True | 5611.779 | 15135.957 | 24735.51 |  |  |  |  |  |  |  |  |  | 1002.555 | 24050.0 | 87.552 | 1784416601807.0 |
| kafka_streams |  | tumbling_count | failure_changelog_restore_trial2 | 20.0 | 2001 | 386 | 386 | True | 5236.341 | 14436.327 | 24036.3 |  |  |  |  |  |  |  |  |  | 1001.906 | 23284.0 | 95.351 | 1784524459895.0 |
| kafka_streams |  | tumbling_count | failure_changelog_restore_trial3 | 20.0 | 2001 | 386 | 386 | True | 5330.405 | 14482.662 | 24082.198 |  |  |  |  |  |  |  |  |  | 1002.805 | 23386.0 | 84.064 | 1784587955085.0 |
| kafka_streams |  | tumbling_count | failure_changelog_restore_trial4 | 20.0 | 2001 | 386 | 386 | True | 5358.113 | 14888.055 | 24488.017 |  |  |  |  |  |  |  |  |  | 1002.316 | 24085.0 | 107.519 | 1784559033672.0 |
| kafka_streams |  | tumbling_count | failure_changelog_restore_trial5 | 20.0 | 2001 | 386 | 386 | True | 5475.743 | 14686.844 | 24286.806 |  |  |  |  |  |  |  |  |  | 1001.998 | 23589.0 | 82.232 | 1784568936614.0 |
| kafka_streams |  | tumbling_count | failure_jvm_kill | 20.0 | 5001 | 3777 | 3777 | True | 16078.76 | 40848.867 | 52450.696 |  |  |  |  |  |  |  |  |  | 1002.952 | 52082.0 | 112.151 | 1784515251074.0 |
| kafka_streams |  | tumbling_count | failure_jvm_kill_trial2 | 20.0 | 2001 | 386 | 386 | False | 11616.696 | 47918.373 | 58219.04 |  |  |  |  |  |  |  |  |  | 1002.542 | 58006.0 | 116.454 | 1784609396159.0 |
| kafka_streams |  | tumbling_count | failure_jvm_kill_trial3 | 20.0 | 2001 | 386 | 386 | False | 11408.753 | 47753.052 | 58053.875 |  |  |  |  |  |  |  |  |  | 1002.232 | 57695.0 | 112.472 | 1784611637513.0 |
| kafka_streams |  | tumbling_count | failure_jvm_kill_trial4 | 20.0 | 2001 | 386 | 386 | False | 11221.192 | 47268.748 | 57569.317 |  |  |  |  |  |  |  |  |  | 1001.484 | 57358.0 | 116.193 | 1784608303421.0 |
| kafka_streams |  | tumbling_count | failure_jvm_kill_trial5 | 20.0 | 2001 | 386 | 386 | False | 11033.232 | 47077.2 | 57381.632 |  |  |  |  |  |  |  |  |  | 1001.878 | 57059.0 | 123.073 | 1784612757486.0 |
| kafka_streams |  | tumbling_count | failure_kraft_failover | 20.0 | 2001 | 386 | 386 | True | 4778.02 | 13687.541 | 23287.114 |  |  |  |  |  |  |  |  |  | 1002.335 | 22889.0 | 92.384 | 1784419168830.0 |
| kafka_streams |  | tumbling_count | failure_kraft_failover_trial2 | 20.0 | 2001 | 386 | 386 | True | 7723.55 | 17223.662 | 26823.552 |  |  |  |  |  |  |  |  |  | 1002.738 | 26270.0 | 156.598 | 1784587219195.0 |
| kafka_streams |  | tumbling_count | failure_kraft_failover_trial3 | 20.0 | 2001 | 386 | 386 | True | 6686.483 | 17293.446 | 26493.436 |  |  |  |  |  |  |  |  |  | 1002.673 | 26152.0 | 96.265 | 1784554536937.0 |
| kafka_streams |  | tumbling_count | failure_kraft_failover_trial4 | 20.0 | 2001 | 386 | 386 | True | 6232.437 | 15766.893 | 25366.863 |  |  |  |  |  |  |  |  |  | 1002.772 | 24619.0 | 73.366 | 1784584118947.0 |
| kafka_streams |  | tumbling_count | failure_kraft_failover_trial5 | 20.0 | 2001 | 386 | 386 | True | 6971.317 | 16759.884 | 25959.789 |  |  |  |  |  |  |  |  |  | 1002.61 | 25652.0 | 111.193 | 1784574630561.0 |
| kafka_streams |  | tumbling_count | failure_node_loss | 20.0 | 5001 | 886 | 886 | True | 6130.406 | 29223.081 | 38323.065 |  |  |  |  |  |  |  |  |  | 1001.25 | 37382.0 | 126.756 | 1784515916895.0 |
| kafka_streams |  | tumbling_count | failure_node_loss_trial2 | 20.0 | 2001 | 386 | 386 | False | 11095.024 | 47137.638 | 57442.592 |  |  |  |  |  |  |  |  |  | 1001.47 | 57160.0 | 134.911 | 1784611090627.0 |
| kafka_streams |  | tumbling_count | failure_node_loss_trial3 | 20.0 | 2001 | 386 | 386 | False | 11632.548 | 47981.292 | 58282.247 |  |  |  |  |  |  |  |  |  | 1002.508 | 58009.0 | 128.749 | 1784606638673.0 |
| kafka_streams |  | tumbling_count | failure_node_loss_trial4 | 20.0 | 2001 | 386 | 386 | False | 11030.709 | 47078.969 | 57379.564 |  |  |  |  |  |  |  |  |  | 1002.086 | 57059.0 | 123.607 | 1784613331802.0 |
| kafka_streams |  | tumbling_count | failure_node_loss_trial5 | 20.0 | 2001 | 386 | 386 | False | 11819.71 | 48167.884 | 58468.074 |  |  |  |  |  |  |  |  |  | 1001.561 | 58157.0 | 116.31 | 1784615518771.0 |
| kafka_streams |  | tumbling_count | failure_s3_throttling | 20.0 | 2001 | 386 | 386 | True | 5465.864 | 14565.872 | 23510.924 |  |  |  |  |  |  |  |  |  | 1002.127 | 23235.0 | 82.025 | 1784407016154.0 |
| kafka_streams |  | tumbling_count | failure_s3_throttling_trial2 | 20.0 | 2001 | 386 | 386 | True | 5698.477 | 14761.422 | 24048.478 |  |  |  |  |  |  |  |  |  | 1002.863 | 23092.0 | 94.108 | 1784529416541.0 |
| kafka_streams |  | tumbling_count | failure_s3_throttling_trial3 | 20.0 | 2001 | 386 | 386 | True | 5539.292 | 14538.807 | 24036.32 |  |  |  |  |  |  |  |  |  | 1002.168 | 23091.0 | 85.934 | 1784550768754.0 |
| kafka_streams |  | tumbling_count | failure_s3_throttling_trial4 | 20.0 | 2001 | 386 | 386 | True | 5466.274 | 14997.25 | 24597.946 |  |  |  |  |  |  |  |  |  | 1002.283 | 23994.0 | 57.465 | 1784528679941.0 |
| kafka_streams |  | tumbling_count | failure_s3_throttling_trial5 | 20.0 | 2001 | 386 | 386 | True | 5396.922 | 14772.159 | 24069.416 |  |  |  |  |  |  |  |  |  | 1001.83 | 23089.0 | 73.043 | 1784561299661.0 |
| kafka_streams | w1 | identity |  | 20.0 | 100 | 100 | 100 | True | 1182.021 | 2622.002 | 2821.55 |  |  |  |  |  |  |  |  |  |  |  |  |  |
| kafka_streams | w1 | identity | rate10 | 10.0 | 100 | 100 | 100 | True | 1022.525 | 2334.453 | 2734.456 |  |  |  |  |  |  |  |  |  |  |  |  |  |
| kafka_streams | w1 | identity | rate40 | 40.0 | 100 | 100 | 100 | True | 1704.546 | 2828.505 | 2928.505 |  |  |  |  |  |  |  |  |  |  |  |  |  |
| kafka_streams | w1 | identity | repeat1 | 20.0 | 100 | 100 | 100 | True | 1226.389 | 2633.999 | 2833.992 |  |  |  |  |  |  |  |  |  |  |  |  |  |
| kafka_streams | w1 | identity | resource_metrics | 20.0 | 100 | 100 | 100 | True | 1526.48 | 3726.034 | 3926.008 |  |  |  |  |  |  |  |  |  |  |  |  |  |
| kafka_streams | w1 | identity | saturation_part1_rate100 | 100.0 | 3000 | 3000 | 3000 | True | 1106.843 | 1956.247 | 2781.207 |  |  |  |  |  |  |  |  |  | 2254.513 | 103.0 | 1120.693 |  |
| kafka_streams | w1 | identity | stability_100 | 100.0 | 180000 | 180000 | 180000 | True | 1216.313 | 1852.678 | 1967.434 |  |  |  |  |  |  |  |  |  |  |  |  |  |
| kafka_streams | w2 | filter_map |  | 20.0 | 100 | 56 | 56 | True | 1226.708 | 2843.058 | 2993.158 |  |  |  |  |  |  |  |  |  |  |  |  |  |
| kafka_streams | w2 | filter_map | rate10 | 10.0 | 100 | 56 | 56 | True | 1221.64 | 2526.477 | 2825.823 |  |  |  |  |  |  |  |  |  |  |  |  |  |
| kafka_streams | w2 | filter_map | rate40 | 40.0 | 100 | 56 | 56 | True | 1765.886 | 2890.619 | 2964.982 |  |  |  |  |  |  |  |  |  |  |  |  |  |
| kafka_streams | w2 | filter_map | repeat1 | 20.0 | 100 | 56 | 56 | True | 1265.364 | 2665.192 | 2814.333 |  |  |  |  |  |  |  |  |  |  |  |  |  |
| kafka_streams | w2 | filter_map | resource_metrics | 20.0 | 100 | 56 | 56 | True | 1778.035 | 3660.216 | 3809.273 |  |  |  |  |  |  |  |  |  |  |  |  |  |
| kafka_streams | w2 | filter_map | stability_100 | 100.0 | 180000 | 89906 | 89906 | True | 999.378 | 1749.052 | 1928.279 |  |  |  |  |  |  |  |  |  |  |  |  |  |
| kafka_streams | w3 | tumbling_count |  | 20.0 | 101 | 71 | 71 | True | 3973.097 | 6873.202 | 7123.28 |  |  |  |  |  |  |  |  |  |  |  |  |  |
| kafka_streams | w3 | tumbling_count | rate10 | 10.0 | 101 | 71 | 71 | True | 5331.984 | 11132.103 | 11631.267 |  |  |  |  |  |  |  |  |  |  |  |  |  |
| kafka_streams | w3 | tumbling_count | rate40 | 40.0 | 101 | 71 | 71 | True | 2368.306 | 3818.504 | 3942.612 |  |  |  |  |  |  |  |  |  |  |  |  |  |
| kafka_streams | w3 | tumbling_count | repeat1 | 20.0 | 101 | 71 | 71 | True | 3595.147 | 6495.271 | 6744.307 |  |  |  |  |  |  |  |  |  |  |  |  |  |
| kafka_streams | w3 | tumbling_count | resource_metrics | 20.0 | 101 | 71 | 71 | True | 3881.205 | 6781.175 | 7030.591 |  |  |  |  |  |  |  |  |  |  |  |  |  |
| kafka_streams | w3 | tumbling_count | stability_100 | 100.0 | 180001 | 29924 | 29924 | True | 2876.071 | 5147.158 | 6576.362 |  |  |  |  |  |  |  |  |  |  |  |  |  |
| kafka_streams | w3 | tumbling_count | tuning_commit10s | 20.0 | 101 | 71 | 71 | True | 21946.167 | 24846.162 | 25095.501 |  |  |  |  |  |  |  |  |  |  |  |  |  |
| kafka_streams | w3 | tumbling_count | tuning_control | 20.0 | 101 | 71 | 71 | True | 3080.8 | 5980.766 | 6230.042 |  |  |  |  |  |  |  |  |  |  |  |  |  |
| kafka_streams | w4 | sliding_sum |  | 20.0 | 101 | 710 | 710 | True | 3964.038 | 6864.071 | 7113.249 |  |  |  |  |  |  |  |  |  |  |  |  |  |
| kafka_streams | w4 | sliding_sum | rate10 | 10.0 | 101 | 710 | 710 | True | 4865.798 | 10665.881 | 11164.941 |  |  |  |  |  |  |  |  |  |  |  |  |  |
| kafka_streams | w4 | sliding_sum | rate40 | 40.0 | 101 | 710 | 710 | True | 2433.497 | 3883.589 | 4006.722 |  |  |  |  |  |  |  |  |  |  |  |  |  |
| kafka_streams | w4 | sliding_sum | repeat1 | 20.0 | 101 | 710 | 710 | True | 3191.806 | 6091.807 | 6338.27 |  |  |  |  |  |  |  |  |  |  |  |  |  |
| kafka_streams | w4 | sliding_sum | resource_metrics | 20.0 | 101 | 710 | 710 | True | 3164.033 | 6063.919 | 6311.03 |  |  |  |  |  |  |  |  |  |  |  |  |  |
| kafka_streams | w4 | sliding_sum | stability_100 | 100.0 | 180001 | 30900 | 30900 | True | 2926.241 | 5243.404 | 6813.603 |  |  |  |  |  |  |  |  |  |  |  |  |  |
| kafka_streams | w5 | stream_stream_join |  | 20.0 | 200 | 186 | 186 | True | 2544.645 | 3896.173 | 4096.137 |  |  |  |  |  |  |  |  |  |  |  |  |  |
| kafka_streams | w5 | stream_stream_join | rate10 | 10.0 | 200 | 186 | 186 | True | 1835.608 | 3035.445 | 3435.417 |  |  |  |  |  |  |  |  |  |  |  |  |  |
| kafka_streams | w5 | stream_stream_join | rate40 | 40.0 | 200 | 186 | 186 | True | 2195.37 | 3338.27 | 3438.284 |  |  |  |  |  |  |  |  |  |  |  |  |  |
| kafka_streams | w5 | stream_stream_join | repeat1 | 20.0 | 200 | 186 | 186 | True | 2540.653 | 3898.778 | 4098.726 |  |  |  |  |  |  |  |  |  |  |  |  |  |
| kafka_streams | w5 | stream_stream_join | stability_100 | 100.0 | 24000 | 1093417 | 1093417 | True | 2172.128 | 2762.18 | 2913.538 |  |  |  |  |  |  |  |  |  |  |  |  |  |
