import java.lang.reflect.Method;

public class test2 {
    public static void main(String[] args) throws Exception {
        Class<?> clazz = Class.forName("org.apache.flink.streaming.api.environment.CheckpointConfig");
        for (Method m : clazz.getMethods()) {
            if (m.getName().toLowerCase().contains("check")) {
                System.out.println(m.getName());
                for (Class<?> p : m.getParameterTypes()) {
                    System.out.println("  - " + p.getName());
                }
            }
        }
    }
}
