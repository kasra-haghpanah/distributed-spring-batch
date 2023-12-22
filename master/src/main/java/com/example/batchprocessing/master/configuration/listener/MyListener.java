package com.example.batchprocessing.master.configuration.listener;

import com.example.batchprocessing.master.configuration.rabbitmq.Receiver;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.DependsOn;
import org.springframework.jdbc.core.JdbcTemplate;

@DependsOn("properties")
@CoffeeSoftwareComponent
public class MyListener implements ApplicationListener<ApplicationReadyEvent> {

    final RabbitTemplate rabbitTemplate;
    final Receiver receiver;
    final JdbcTemplate jdbcTemplate;

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

    public MyListener(
            RabbitTemplate rabbitTemplate,
            Receiver receiver,
            @Qualifier("batchDestinationJdbcTemplate") JdbcTemplate jdbcTemplate
    ) {
        this.rabbitTemplate = rabbitTemplate;
        this.receiver = receiver;
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public void onApplicationEvent(ApplicationReadyEvent event) {
        System.out.println("Hello, from a custom component!");
        jdbcTemplate.execute(videoGameSalesTableScript);
        jdbcTemplate.execute(yearPlatformReportTableScript);
        jdbcTemplate.execute(remoteYearPlatformReportTableScript);
        jdbcTemplate.execute(remoteVideoGameSalesTableScript);
        //rabbitTemplate.convertAndSend(Properties.getRabbitmqTopicExchange(), Properties.getRabbitmqTopicExchangeRoutingKeyOne(), "Hello from RabbitMQ by requests!".getBytes(StandardCharsets.UTF_8));
        //rabbitTemplate.convertAndSend(Properties.getRabbitmqTopicExchange(), Properties.getRabbitmqTopicExchangeRoutingKeyTwo(), "Hello from RabbitMQ by replies!".getBytes(StandardCharsets.UTF_8));
    }
}
