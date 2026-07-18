import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;

public class test4 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        SavepointRestoreSettings settings = SavepointRestoreSettings.forPath("file:///tmp/chk-123");
        SavepointRestoreSettings.toConfiguration(settings, conf);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        System.out.println("Success!");
    }
}
