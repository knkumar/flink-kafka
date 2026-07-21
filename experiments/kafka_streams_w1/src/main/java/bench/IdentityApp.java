package bench;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.WindowStore;

public final class IdentityApp {
    private static final long TUMBLING_WINDOW_MS = 60_000L;
    private static final long SLIDING_WINDOW_MS = 600_000L;
    private static final long SLIDE_MS = 60_000L;
    private static final long JOIN_WINDOW_MS = 600_000L;

    private IdentityApp() {
    }

    public static void main(String[] args) throws InterruptedException {
        String bootstrapServers = env("BOOTSTRAP_SERVERS", "kafka:9092");
        String applicationId = env("APPLICATION_ID", "stream-state-bench-w1-identity");
        String inputTopic = env("INPUT_TOPIC", "bench-w1-input");
        String leftInputTopic = env("LEFT_INPUT_TOPIC", "bench-w5-left-input");
        String rightInputTopic = env("RIGHT_INPUT_TOPIC", "bench-w5-right-input");
        String outputTopic = env("OUTPUT_TOPIC", "bench-w1-output");
        String workload = env("WORKLOAD", "identity");
        String commitIntervalMs = env("COMMIT_INTERVAL_MS", "1000");
        String cacheMaxBytes = env("CACHE_MAX_BYTES", "10485760");

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, commitIntervalMs);
        
        int parallelism = Integer.parseInt(System.getenv().getOrDefault("PARALLELISM", "1"));
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, parallelism);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, cacheMaxBytes);

        StreamsBuilder builder = new StreamsBuilder();
        if ("tumbling_count".equals(workload)) {
            buildTumblingCountTopology(builder, inputTopic, outputTopic);
        } else if ("sliding_sum".equals(workload)) {
            buildSlidingSumTopology(builder, inputTopic, outputTopic);
        } else if ("stream_stream_join".equals(workload)) {
            buildStreamJoinTopology(builder, leftInputTopic, rightInputTopic, outputTopic);
        } else {
            builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()))
                    .flatMapValues(value -> transform(workload, value))
                    .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));
        }

        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, props);
        CountDownLatch stop = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close(Duration.ofSeconds(10));
            stop.countDown();
        }, "streams-shutdown"));

        streams.start();
        stop.await();
    }

    static String toOutputJson(String eventLine) {
        return toIdentityJson(eventLine);
    }

    static List<String> transform(String workload, String eventLine) {
        if ("identity".equals(workload)) {
            return Collections.singletonList(toIdentityJson(eventLine));
        }
        if ("filter_map".equals(workload)) {
            String[] fields = parse(eventLine);
            int payload = Integer.parseInt(fields[2]);
            if (payload % 2 != 0) {
                return Collections.emptyList();
            }
            return Collections.singletonList(toFilterMapJson(fields[0], Integer.parseInt(fields[1]), payload));
        }
        throw new IllegalArgumentException("Unsupported workload: " + workload);
    }

    private static void buildTumblingCountTopology(StreamsBuilder builder, String inputTopic, String outputTopic) {
        builder.stream(
                        inputTopic,
                        Consumed.with(Serdes.String(), Serdes.String()).withTimestampExtractor(new EventTimeExtractor()))
                .selectKey((key, value) -> isTick(value) ? "__tick__" : parse(value)[1])
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMillis(TUMBLING_WINDOW_MS)))
                .aggregate(
                        () -> "",
                        (key, value, aggregate) -> appendEventId(aggregate, parse(value)[0]),
                        Materialized.<String, String, WindowStore<Bytes, byte[]>>as("tumbling-count-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.String()))
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
                .toStream()
                .filter((windowedKey, value) -> !"__tick__".equals(windowedKey.key()))
                .mapValues(IdentityApp::toTumblingCountJson)
                .selectKey((windowedKey, value) -> windowedKey.key())
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));
    }

    private static void buildStreamJoinTopology(
            StreamsBuilder builder,
            String leftInputTopic,
            String rightInputTopic,
            String outputTopic) {
        Consumed<String, String> consumed = Consumed.with(Serdes.String(), Serdes.String())
                .withTimestampExtractor(new EventTimeExtractor());
        KStream<String, String> left = builder.stream(leftInputTopic, consumed)
                .selectKey((key, value) -> parse(value)[1]);
        KStream<String, String> right = builder.stream(rightInputTopic, consumed)
                .selectKey((key, value) -> parse(value)[1]);
        left.join(
                        right,
                        IdentityApp::toJoinJson,
                        JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMillis(JOIN_WINDOW_MS)),
                        StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.String()))
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));
    }

    private static void buildSlidingSumTopology(StreamsBuilder builder, String inputTopic, String outputTopic) {
        builder.stream(
                        inputTopic,
                        Consumed.with(Serdes.String(), Serdes.String()).withTimestampExtractor(new EventTimeExtractor()))
                .selectKey((key, value) -> isTick(value) ? "__tick__" : parse(value)[1])
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMillis(SLIDING_WINDOW_MS))
                        .advanceBy(Duration.ofMillis(SLIDE_MS)))
                .aggregate(
                        () -> "",
                        (key, value, aggregate) -> appendPayloadAndEventId(aggregate, value),
                        Materialized.<String, String, WindowStore<Bytes, byte[]>>as("sliding-sum-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.String()))
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
                .toStream()
                .filter((windowedKey, value) -> !"__tick__".equals(windowedKey.key()))
                .mapValues(IdentityApp::toSlidingSumJson)
                .selectKey((windowedKey, value) -> windowedKey.key())
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));
    }

    private static String toIdentityJson(String eventLine) {
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
                + "\"t2_ms\":" + System.currentTimeMillis() + ","
                + "\"source_event_ids\":[\"" + escape(eventId) + "\"]}";
    }

    private static String toFilterMapJson(String eventId, int key, int payload) {
        return "{\"output_id\":\"fm-" + escape(eventId) + "\","
                + "\"key\":" + key + ","
                + "\"window_start_ms\":null,"
                + "\"window_end_ms\":null,"
                + "\"value\":" + (payload * 2) + ","
                + "\"t2_ms\":" + System.currentTimeMillis() + ","
                + "\"source_event_ids\":[\"" + escape(eventId) + "\"]}";
    }

    private static String toTumblingCountJson(Windowed<String> windowedKey, String eventIds) {
        List<String> ids = new ArrayList<>();
        for (String eventId : eventIds.split("\\n")) {
            if (!eventId.isBlank()) {
                ids.add(eventId);
            }
        }
        Collections.sort(ids);
        int key = Integer.parseInt(windowedKey.key());
        long windowStart = windowedKey.window().start();
        long windowEnd = windowedKey.window().end();
        return "{\"output_id\":\"tc-" + key + "-" + windowStart + "\","
                + "\"key\":" + key + ","
                + "\"window_start_ms\":" + windowStart + ","
                + "\"window_end_ms\":" + windowEnd + ","
                + "\"value\":" + ids.size() + ","
                + "\"t2_ms\":" + System.currentTimeMillis() + ","
                + "\"source_event_ids\":" + toJsonStringArray(ids) + "}";
    }

    private static String toSlidingSumJson(Windowed<String> windowedKey, String aggregate) {
        String[] lines = aggregate.split("\\n");
        long sum = Long.parseLong(lines[0]);
        List<String> ids = new ArrayList<>();
        for (int i = 1; i < lines.length; i++) {
            if (!lines[i].isBlank()) {
                ids.add(lines[i]);
            }
        }
        Collections.sort(ids);
        int key = Integer.parseInt(windowedKey.key());
        long windowStart = windowedKey.window().start();
        long windowEnd = windowedKey.window().end();
        return "{\"output_id\":\"ss-" + key + "-" + windowStart + "\","
                + "\"key\":" + key + ","
                + "\"window_start_ms\":" + windowStart + ","
                + "\"window_end_ms\":" + windowEnd + ","
                + "\"value\":" + sum + ","
                + "\"t2_ms\":" + System.currentTimeMillis() + ","
                + "\"source_event_ids\":" + toJsonStringArray(ids) + "}";
    }

    private static String toJoinJson(String leftLine, String rightLine) {
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
                + "\"t2_ms\":" + System.currentTimeMillis() + ","
                + "\"source_event_ids\":[\"" + escape(leftId) + "\",\"" + escape(rightId) + "\"]}";
    }

    private static String appendEventId(String aggregate, String eventId) {
        if (aggregate == null || aggregate.isBlank()) {
            return eventId;
        }
        return aggregate + "\n" + eventId;
    }

    private static String appendPayloadAndEventId(String aggregate, String eventLine) {
        String[] fields = parse(eventLine);
        long payload = Long.parseLong(fields[2]);
        if (aggregate == null || aggregate.isBlank()) {
            return payload + "\n" + fields[0];
        }
        String[] lines = aggregate.split("\\n", -1);
        long sum = Long.parseLong(lines[0]) + payload;
        StringBuilder builder = new StringBuilder(Long.toString(sum));
        for (int i = 1; i < lines.length; i++) {
            if (!lines[i].isBlank()) {
                builder.append("\n").append(lines[i]);
            }
        }
        builder.append("\n").append(fields[0]);
        return builder.toString();
    }

    private static boolean isTick(String eventLine) {
        return "__tick__".equals(parse(eventLine)[0]);
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

    private static final class EventTimeExtractor implements TimestampExtractor {
        @Override
        public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
            Object value = record.value();
            if (!(value instanceof String)) {
                return partitionTime;
            }
            return Long.parseLong(parse((String) value)[3]);
        }
    }
}
