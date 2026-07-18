import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.configuration.Configuration;

public class test3 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setString("state.checkpoints.dir", "file:///tmp/flink-checkpoints");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        System.out.println("Success!");
    }
}
