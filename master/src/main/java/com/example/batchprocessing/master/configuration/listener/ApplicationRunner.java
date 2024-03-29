package com.example.batchprocessing.master.configuration.listener;

import com.example.batchprocessing.master.configuration.rabbitmq.Receiver;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.TaskExecutorJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.jdbc.core.JdbcTemplate;

@Configuration(proxyBeanMethods = false)
public class ApplicationRunner {

    final RabbitTemplate rabbitTemplate;
    final Receiver receiver;
    final JdbcTemplate jdbcTemplate;

    private final JobRepository jobRepository;

    public static final String videoGameSalesTableScript = """
            CREATE TABLE IF NOT EXISTS video_game_sales
            (
                `rank`         int,
                `name`         text NOT NULL,
                `platform`     text NOT NULL,
                `year`         int,
                `genre`        text NOT NULL,
                `publisher`    text,
                `na_sales`     numeric(4,2),
                `eu_sales`     numeric(4,2),
                `jp_sales`     numeric(4,2),
                `other_sales`  numeric(4,2),
                `global_sales` numeric(4,2),
                PRIMARY KEY (`name`(100), `platform`(100), `year`, `genre`(100))
            )
            """;
    public static final String yearPlatformReportTableScript = """
            CREATE TABLE IF NOT EXISTS year_platform_report
            (
                year     int,
                platform text,
                sales    numeric(8, 2),
                unique (year, platform)
            );
            """;

    public static final String remoteYearPlatformReportTableScript = """
            CREATE TABLE IF NOT EXISTS remote_year_platform_report
            (
                year     int,
                platform text,
                sales    numeric(8, 2),
                unique (year, platform)
            );
            """;

    public static final String remoteVideoGameSalesTableScript = """
            CREATE TABLE IF NOT EXISTS remote_video_game_sales
            (
                `rank`         int,
                `name`         text NOT NULL,
                `platform`     text NOT NULL,
                `year`         int,
                `genre`        text NOT NULL,
                `publisher`    text,
                `na_sales`     numeric(4,2),
                `eu_sales`     numeric(4,2),
                `jp_sales`     numeric(4,2),
                `other_sales`  numeric(4,2),
                `global_sales` numeric(4,2),
                PRIMARY KEY (`name`(100), `platform`(100), `year`, `genre`(100))
            )
            """;

    ///private final JobLauncher jobLauncher;

    public ApplicationRunner(
            @Qualifier("batchDestinationJdbcTemplate") JdbcTemplate jdbcTemplate,
            JobRepository jobRepository,
            RabbitTemplate rabbitTemplate,
            Receiver receiver
    ) {
        this.jdbcTemplate = jdbcTemplate;
        this.jobRepository = jobRepository;
        this.rabbitTemplate = rabbitTemplate;
        this.receiver = receiver;
    }


    //@Bean
    //@Primary
    //@Qualifier("taskExecutor")
    TaskExecutor taskExecutor() {
        return new SimpleAsyncTaskExecutor();
    }

    // for running multiple jobs as async
    //@Bean
    //@Qualifier("jobLauncherTwo")
    public JobLauncher jobLauncherTwo(@Qualifier("taskExecutor") TaskExecutor taskExecutor) throws Exception {
        TaskExecutorJobLauncher jobLauncher = new TaskExecutorJobLauncher();
        jobLauncher.setJobRepository(jobRepository);
        jobLauncher.setTaskExecutor(taskExecutor);
        jobLauncher.afterPropertiesSet();
        return jobLauncher;
    }


    @Bean
    @Qualifier("runner")
    org.springframework.boot.ApplicationRunner runner() {
        return (applicationArguments) -> {

            System.out.println("applicationArguments");
            jdbcTemplate.execute(videoGameSalesTableScript);
            jdbcTemplate.execute(yearPlatformReportTableScript);
            jdbcTemplate.execute(remoteYearPlatformReportTableScript);
            jdbcTemplate.execute(remoteVideoGameSalesTableScript);
            //rabbitTemplate.convertAndSend(Properties.getRabbitmqTopicExchange(), Properties.getRabbitmqTopicExchangeRoutingKeyOne(), "Hello from RabbitMQ by requests!".getBytes(StandardCharsets.UTF_8));
            //rabbitTemplate.convertAndSend(Properties.getRabbitmqTopicExchange(), Properties.getRabbitmqTopicExchangeRoutingKeyTwo(), "Hello from RabbitMQ by replies!".getBytes(StandardCharsets.UTF_8));

        };

    }

}
