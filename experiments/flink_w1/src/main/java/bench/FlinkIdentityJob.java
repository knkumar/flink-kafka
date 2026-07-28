package bench;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public final class FlinkIdentityJob {
    private static final long TUMBLING_WINDOW_MS = 60_000L;
    private static final long SLIDING_WINDOW_MS = 600_000L;
    private static final long SLIDE_MS = 60_000L;
    private static final long JOIN_WINDOW_MS = 600_000L;

    private FlinkIdentityJob() {
    }

    public static void main(String[] args) throws Exception {
        String bootstrapServers = env("BOOTSTRAP_SERVERS", "kafka:9092");
        String groupId = env("GROUP_ID", "stream-state-bench-flink-w1");
        String inputTopic = env("INPUT_TOPIC", "bench-w1-input");
        String leftInputTopic = env("LEFT_INPUT_TOPIC", "bench-w5-left-input");
        String rightInputTopic = env("RIGHT_INPUT_TOPIC", "bench-w5-right-input");
        String outputTopic = env("OUTPUT_TOPIC", "bench-w1-flink-output");
        String transactionalPrefix = env("TRANSACTIONAL_ID_PREFIX", "stream-state-bench-flink-w1");
        String workload = env("WORKLOAD", "identity");
        boolean sourceBounded = Boolean.parseBoolean(env("SOURCE_BOUNDED", "true"));
        long checkpointIntervalMs = Long.parseLong(env("CHECKPOINT_INTERVAL_MS", "1000"));

        org.apache.flink.configuration.Configuration conf = new org.apache.flink.configuration.Configuration();
        String topologyId = "stream-state-bench-flink-" + workload;
        conf.setString("s3.access-key", "minioadmin");
        conf.setString("s3.secret-key", "minioadmin");
        conf.setString("s3.endpoint", "http://minio:9000");
        conf.setString("s3.path.style.access", "true");
        conf.setString("state.checkpoints.dir", "s3://flink-checkpoints/" + topologyId);
        
        try {
            org.apache.flink.core.fs.FileSystem.initialize(conf, null);
            org.apache.flink.core.fs.Path cpPath = new org.apache.flink.core.fs.Path("s3://flink-checkpoints/" + topologyId);
            org.apache.flink.core.fs.FileSystem fs = cpPath.getFileSystem();
            if (fs.exists(cpPath)) {
                org.apache.flink.core.fs.FileStatus[] jobDirs = fs.listStatus(cpPath);
                if (jobDirs != null) {
                    long maxId = -1;
                    org.apache.flink.core.fs.Path latestChk = null;
                    for (org.apache.flink.core.fs.FileStatus jDir : jobDirs) {
                        if (!jDir.isDir()) continue;
                        org.apache.flink.core.fs.FileStatus[] chkDirs = fs.listStatus(jDir.getPath());
                        if (chkDirs != null) {
                            for (org.apache.flink.core.fs.FileStatus cDir : chkDirs) {
                                String name = cDir.getPath().getName();
                                if (name.startsWith("chk-")) {
                                    try {
                                        long id = Long.parseLong(name.substring(4));
                                        if (id > maxId) {
                                            maxId = id;
                                            latestChk = cDir.getPath();
                                        }
                                    } catch (Exception ignored) {}
                                }
                            }
                        }
                    }
                    if (latestChk != null) {
                        System.out.println("Resuming from checkpoint: " + latestChk.toString());
                        org.apache.flink.runtime.jobgraph.SavepointRestoreSettings settings = 
                            org.apache.flink.runtime.jobgraph.SavepointRestoreSettings.forPath(latestChk.toString());
                        org.apache.flink.runtime.jobgraph.SavepointRestoreSettings.toConfiguration(settings, conf);
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("Failed to parse checkpoints from S3: " + e.getMessage());
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        int parallelism = Integer.parseInt(System.getenv().getOrDefault("PARALLELISM", "1"));
        env.setParallelism(parallelism);
        env.enableCheckpointing(checkpointIntervalMs, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(30_000);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(outputTopic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix(transactionalPrefix)
                .setProperty("transaction.timeout.ms", String.valueOf(Duration.ofMinutes(5).toMillis()))
                .build();

        if ("stream_stream_join".equals(workload)) {
            KafkaSource<String> leftSource = source(bootstrapServers, leftInputTopic, groupId + "-left", sourceBounded);
            KafkaSource<String> rightSource = source(bootstrapServers, rightInputTopic, groupId + "-right", sourceBounded);
            WatermarkStrategy<String> joinWatermarks = WatermarkStrategy
                    .<String>forBoundedOutOfOrderness(java.time.Duration.ofSeconds(2))
                    .withTimestampAssigner((value, timestamp) -> Long.parseLong(parse(value)[3]));
            DataStream<String> left = env.fromSource(leftSource, joinWatermarks, "left-kafka-source");
            DataStream<String> right = env.fromSource(rightSource, joinWatermarks, "right-kafka-source");
            left.keyBy(value -> parse(value)[1])
                    .intervalJoin(right.keyBy(value -> parse(value)[1]))
                    .between(Duration.ofMillis(-JOIN_WINDOW_MS), Duration.ofMillis(JOIN_WINDOW_MS))
                    .process(new JoinFunction())
                    .sinkTo(sink)
                    .name("kafka-sink");
            env.execute("stream-state-bench-flink-" + workload);
            return;
        }

        KafkaSource<String> source = source(bootstrapServers, inputTopic, groupId, sourceBounded);
        WatermarkStrategy<String> watermarkStrategy = isWindowed(workload)
                ? WatermarkStrategy.<String>forMonotonousTimestamps()
                        .withTimestampAssigner((value, timestamp) -> Long.parseLong(parse(value)[3]))
                : WatermarkStrategy.noWatermarks();

        DataStream<String> input = env.fromSource(source, watermarkStrategy, "kafka-source");
        if ("tumbling_count".equals(workload)) {
            input.filter(value -> !isTick(value))
                    .keyBy(value -> parse(value)[1])
                    .window(TumblingEventTimeWindows.of(Duration.ofMillis(TUMBLING_WINDOW_MS)))
                    .process(new TumblingCountWindow())
                    .sinkTo(sink)
                    .name("kafka-sink");
        } else if ("sliding_sum".equals(workload)) {
            input.filter(value -> !isTick(value))
                    .keyBy(value -> parse(value)[1])
                    .window(SlidingEventTimeWindows.of(
                            Duration.ofMillis(SLIDING_WINDOW_MS),
                            Duration.ofMillis(SLIDE_MS)))
                    .process(new SlidingSumWindow())
                    .sinkTo(sink)
                    .name("kafka-sink");
        } else {
            input.process(new org.apache.flink.streaming.api.functions.ProcessFunction<String, String>() {
                        @Override
                        public void processElement(String value, Context ctx, Collector<String> out) {
                            long wmMs = ctx.timerService().currentWatermark();
                            long teMs = System.currentTimeMillis();
                            collect(workload, value, out, wmMs, teMs);
                        }
                    })
                    .returns(String.class)
                    .sinkTo(sink)
                    .name("kafka-sink");
        }
        env.execute("stream-state-bench-flink-" + workload);
    }

    private static KafkaSource<String> source(String bootstrapServers, String topic, String groupId, boolean bounded) {
        if (bounded) {
            return KafkaSource.<String>builder()
                    .setBootstrapServers(bootstrapServers)
                    .setTopics(topic)
                    .setGroupId(groupId)
                    .setStartingOffsets(OffsetsInitializer.earliest())
                    .setBounded(OffsetsInitializer.latest())
                    .setValueOnlyDeserializer(new SimpleStringSchema())
                    .setProperty("commit.offsets.on.checkpoint", "true")
                    .build();
        }
        return KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperty("commit.offsets.on.checkpoint", "true")
                .build();
    }

    static String toOutputJson(String eventLine) {
        return toIdentityJson(eventLine, 0, System.currentTimeMillis());
    }

    static void collect(String workload, String eventLine, Collector<String> out, long wmMs, long teMs) {
        if ("identity".equals(workload)) {
            out.collect(toIdentityJson(eventLine, wmMs, teMs));
            return;
        }
        if ("filter_map".equals(workload)) {
            String[] fields = parse(eventLine);
            int payload = Integer.parseInt(fields[2]);
            if (payload % 2 == 0) {
                out.collect(toFilterMapJson(fields[0], Integer.parseInt(fields[1]), payload, wmMs, teMs));
            }
            return;
        }
        throw new IllegalArgumentException("Unsupported workload: " + workload);
    }

    private static boolean isWindowed(String workload) {
        return "tumbling_count".equals(workload) || "sliding_sum".equals(workload);
    }

    private static String toTumblingCountJson(String keyText, long windowStart, long windowEnd, List<String> eventIds, long wmMs, long teMs) {
        Collections.sort(eventIds);
        int key = Integer.parseInt(keyText);
        return "{\"output_id\":\"tc-" + key + "-" + windowStart + "\","
                + "\"key\":" + key + ","
                + "\"window_start_ms\":" + windowStart + ","
                + "\"window_end_ms\":" + windowEnd + ","
                + "\"value\":" + eventIds.size() + ","
                + "\"t_e_ms\":" + teMs + ",\"wm_ms\":" + wmMs + ",\"t2_ms\":" + System.currentTimeMillis() + ","
                + "\"source_event_ids\":" + toJsonStringArray(eventIds) + "}";
    }

    private static String toSlidingSumJson(String keyText, long windowStart, long windowEnd, long sum, List<String> eventIds, long wmMs, long teMs) {
        Collections.sort(eventIds);
        int key = Integer.parseInt(keyText);
        return "{\"output_id\":\"ss-" + key + "-" + windowStart + "\","
                + "\"key\":" + key + ","
                + "\"window_start_ms\":" + windowStart + ","
                + "\"window_end_ms\":" + windowEnd + ","
                + "\"value\":" + sum + ","
                + "\"t_e_ms\":" + teMs + ",\"wm_ms\":" + wmMs + ",\"t2_ms\":" + System.currentTimeMillis() + ","
                + "\"source_event_ids\":" + toJsonStringArray(eventIds) + "}";
    }

    private static String toJoinJson(String leftLine, String rightLine, long wmMs, long teMs) {
        String[] left = parse(leftLine);
        String[] right = parse(rightLine);
        String leftId = left[0];
        String rightId = right[0];
        String first = leftId.compareTo(rightId) <= 0 ? leftId : rightId;
        String second = leftId.compareTo(rightId) <= 0 ? rightId : leftId;
        int key = Integer.parseInt(left[1]);
        long leftTime = Long.parseLong(left[3]);
        long rightTime = Long.parseLong(right[3]);
        long windowStart = Math.min(leftTime, rightTime);
        long windowEnd = Math.max(leftTime, rightTime);
        long value = Long.parseLong(left[2]) + Long.parseLong(right[2]);
        return "{\"output_id\":\"join-" + escape(first) + "-" + escape(second) + "\","
                + "\"key\":" + key + ","
                + "\"window_start_ms\":" + windowStart + ","
                + "\"window_end_ms\":" + windowEnd + ","
                + "\"value\":" + value + ","
                + "\"t_e_ms\":" + teMs + ",\"wm_ms\":" + wmMs + ",\"t2_ms\":" + System.currentTimeMillis() + ","
                + "\"source_event_ids\":[\"" + escape(leftId) + "\",\"" + escape(rightId) + "\"]}";
    }

    private static boolean isTick(String eventLine) {
        return "__tick__".equals(parse(eventLine)[0]);
    }

    private static String toIdentityJson(String eventLine, long wmMs, long teMs) {
        String[] fields = eventLine.split("\\t", -1);
        validate(fields);
        String eventId = fields[0];
        int key = Integer.parseInt(fields[1]);
        int payload = Integer.parseInt(fields[2]);
        return "{\"output_id\":\"" + escape(eventId) + "\","
                + "\"key\":" + key + ","
                + "\"window_start_ms\":null,"
                + "\"window_end_ms\":null,"
                + "\"value\":" + payload + ","
                + "\"t_e_ms\":" + teMs + ",\"wm_ms\":" + wmMs + ",\"t2_ms\":" + System.currentTimeMillis() + ","
                + "\"source_event_ids\":[\"" + escape(eventId) + "\"]}";
    }

    private static String toFilterMapJson(String eventId, int key, int payload, long wmMs, long teMs) {
        return "{\"output_id\":\"fm-" + escape(eventId) + "\","
                + "\"key\":" + key + ","
                + "\"window_start_ms\":null,"
                + "\"window_end_ms\":null,"
                + "\"value\":" + (payload * 2) + ","
                + "\"t_e_ms\":" + teMs + ",\"wm_ms\":" + wmMs + ",\"t2_ms\":" + System.currentTimeMillis() + ","
                + "\"source_event_ids\":[\"" + escape(eventId) + "\"]}";
    }

    private static String[] parse(String eventLine) {
        String[] fields = eventLine.split("\\t", -1);
        validate(fields);
        return fields;
    }

    private static void validate(String[] fields) {
        if (fields.length != 4) {
            throw new IllegalArgumentException("Expected 4 tab-separated fields, got " + fields.length);
        }
    }

    private static String env(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }

    private static String escape(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static String toJsonStringArray(List<String> values) {
        StringBuilder builder = new StringBuilder("[");
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) {
                builder.append(",");
            }
            builder.append("\"").append(escape(values.get(i))).append("\"");
        }
        builder.append("]");
        return builder.toString();
    }

    private static final class TumblingCountWindow extends ProcessWindowFunction<String, String, String, TimeWindow> {
        @Override
        public void process(String key, Context context, Iterable<String> input, Collector<String> out) {
            List<String> eventIds = new ArrayList<>();
            for (String line : input) {
                eventIds.add(parse(line)[0]);
            }
            long wmMs = context.currentWatermark();
            long teMs = System.currentTimeMillis();
            out.collect(toTumblingCountJson(key, context.window().getStart(), context.window().getEnd(), eventIds, wmMs, teMs));
        }
    }

    private static final class SlidingSumWindow extends ProcessWindowFunction<String, String, String, TimeWindow> {
        @Override
        public void process(String key, Context context, Iterable<String> input, Collector<String> out) {
            List<String> eventIds = new ArrayList<>();
            long sum = 0L;
            for (String line : input) {
                String[] fields = parse(line);
                eventIds.add(fields[0]);
                sum += Long.parseLong(fields[2]);
            }
            long wmMs = context.currentWatermark();
            long teMs = System.currentTimeMillis();
            out.collect(toSlidingSumJson(key, context.window().getStart(), context.window().getEnd(), sum, eventIds, wmMs, teMs));
        }
    }

    private static final class JoinFunction extends ProcessJoinFunction<String, String, String> {
        @Override
        public void processElement(String left, String right, Context context, Collector<String> out) {
            long wmMs = context.getTimestamp();
            long teMs = System.currentTimeMillis();
            out.collect(toJoinJson(left, right, wmMs, teMs));
        }
    }
}
