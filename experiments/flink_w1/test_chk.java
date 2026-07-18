import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;

public class test_chk {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        SavepointRestoreSettings settings = SavepointRestoreSettings.forPath("file:///home/kiran/projects/flink/experiments/flink_w1/checkpoints/2129cab69695b8045bda01ab454c761f/chk-37");
        SavepointRestoreSettings.toConfiguration(settings, conf);
        System.out.println("Path: " + settings.getRestorePath());
        System.out.println("Conf: " + conf.getString("execution.savepoint.path", "none"));
    }
}
