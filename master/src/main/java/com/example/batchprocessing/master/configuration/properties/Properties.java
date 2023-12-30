package com.example.batchprocessing.master.configuration.properties;

import com.example.batchprocessing.master.configuration.util.JavaUtil;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import java.util.HashMap;
import java.util.Map;

@Configuration("properties")
public class Properties {

    private static final Map<String, Object> config = new HashMap<String, Object>();

    public Properties(Environment environment) {

        config.put("batch.job-launcher.schedule-with-fixed-delay", environment.getProperty("batch.job-launcher.schedule-with-fixed-delay"));
        config.put("retry.max-attempts", environment.getProperty("retry.max-attempts"));
        config.put("retry.fixed-backoff", environment.getProperty("retry.fixed-backoff"));

        config.put("datasource1.url", environment.getProperty("datasource1.url"));
        config.put("datasource1.driver-class-name", environment.getProperty("datasource1.driver-class-name"));
        config.put("datasource1.username", environment.getProperty("datasource1.username"));
        config.put("datasource1.password", environment.getProperty("datasource1.password"));
        config.put("datasource1.maximum.pool.size", environment.getProperty("datasource1.maximum.pool.size"));
        config.put("datasource1.maximum.lifetime", environment.getProperty("datasource1.maximum.lifetime"));


        config.put("datasource2.url", environment.getProperty("datasource2.url"));
        config.put("datasource2.driver-class-name", environment.getProperty("datasource2.driver-class-name"));
        config.put("datasource2.username", environment.getProperty("datasource2.username"));
        config.put("datasource2.password", environment.getProperty("datasource2.password"));
        config.put("datasource2.maximum.pool.size", environment.getProperty("datasource2.maximum.pool.size"));
        config.put("datasource2.maximum.lifetime", environment.getProperty("datasource2.maximum.lifetime"));

        config.put("rabbitmq.host", environment.getProperty("rabbitmq.host"));
        config.put("rabbitmq.port", environment.getProperty("rabbitmq.port"));
        config.put("rabbitmq.username", environment.getProperty("rabbitmq.username"));
        config.put("rabbitmq.password", environment.getProperty("rabbitmq.password"));

        config.put("rabbitmq.topic-exchange", environment.getProperty("rabbitmq.topic-exchange"));
        config.put("rabbitmq.queue-one", environment.getProperty("rabbitmq.queue-one"));
        config.put("rabbitmq.queue-two", environment.getProperty("rabbitmq.queue-two"));
        config.put("rabbitmq.queue-three", environment.getProperty("rabbitmq.queue-three"));
        config.put("rabbitmq.queue-four", environment.getProperty("rabbitmq.queue-four"));
        config.put("rabbitmq.queue-five", environment.getProperty("rabbitmq.queue-five"));
        config.put("rabbitmq.queue-six", environment.getProperty("rabbitmq.queue-six"));
    }

    private static Object get(String key) {
        return config.get(key);
    }

    private static <T> T get(String key, Class<T> T) {
        return (T) config.get(key);
    }


    public static Long getBatchJobLauncherScheduleWithFixedDelay() {
        return JavaUtil.getLong(get("batch.job-launcher.schedule-with-fixed-delay", String.class), 60l);
    }
    public static Integer getRetryMaxAttempts() {
        return JavaUtil.getInteger(get("retry.max-attempts", String.class), 100);
    }

    public static Integer getRetryFixedBackoff() {
        return JavaUtil.getInteger(get("retry.fixed-backoff", String.class), 10);
    }

    public static String getDatasourceOneUrl() {
        return get("datasource1.url", String.class);
    }

    public static String getDatasourceOneDriverClassName() {
        return get("datasource1.driver-class-name", String.class);
    }

    public static String getDatasourceOneUsername() {
        return get("datasource1.username", String.class);
    }

    public static String getDatasourceOnePassword() {
        return get("datasource1.password", String.class);
    }

    public static Integer getDatasourceOneMaximumPoolSize() {
        return JavaUtil.getInteger(get("datasource1.maximum.pool.size", String.class), 1);
    }

    public static Long getDatasourceOneMaximumLifeTime() {
        return JavaUtil.getLong(get("datasource1.maximum.lifetime", String.class), 30l);
    }
    public static String getDatasourceTwoUrl() {
        return get("datasource2.url", String.class);
    }

    public static String getDatasourceTwoDriverClassName() {
        return get("datasource2.driver-class-name", String.class);
    }

    public static String getDatasourceTwoUsername() {
        return get("datasource2.username", String.class);
    }

    public static String getDatasourceTwoPassword() {
        return get("datasource2.password", String.class);
    }

    public static Integer getDatasourceTwoMaximumPoolSize() {
        return JavaUtil.getInteger(get("datasource2.maximum.pool.size", String.class), 1);
    }

    public static Long getDatasourceTwoMaximumLifeTime() {
        return JavaUtil.getLong(get("datasource2.maximum.lifetime", String.class), 30l);
    }

    public static String getRabbitmqHost() {
        return get("rabbitmq.host", String.class);
    }

    public static Integer getRabbitmqPort() {
        return JavaUtil.getInteger(get("rabbitmq.port", String.class), 0);
    }

    public static String getRabbitmqUsername() {
        return get("rabbitmq.username", String.class);
    }

    public static String getRabbitmqPassword() {
        return get("rabbitmq.password", String.class);
    }

    public static String getRabbitmqTopicExchange() {
        return get("rabbitmq.topic-exchange", String.class);
    }

    public static String getRabbitmqQueueOne() {
        return get("rabbitmq.queue-one", String.class);
    }

    public static String getRabbitmqQueueTwo() {
        return get("rabbitmq.queue-two", String.class);
    }

    public static String getRabbitmqQueueThree() {
        return get("rabbitmq.queue-three", String.class);
    }

    public static String getRabbitmqQueueFour() {
        return get("rabbitmq.queue-four", String.class);
    }

    public static String getRabbitmqQueueFive() {
        return get("rabbitmq.queue-five", String.class);
    }

    public static String getRabbitmqQueueSix() {
        return get("rabbitmq.queue-six", String.class);
    }


}
