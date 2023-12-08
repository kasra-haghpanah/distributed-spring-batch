package com.example.batchprocessing.slave.batch;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.amqp.dsl.Amqp;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.MessageChannels;
import org.springframework.messaging.PollableChannel;

@Configuration
class SlaveConfiguration {

    private final ObjectMapper objectMapper;

    SlaveConfiguration(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Bean
    @Qualifier("requests")// outbound
    DirectChannel requests() {
        return MessageChannels.direct().getObject();
    }


    @Bean
    @Qualifier("replies")
    PollableChannel replies() {
        return MessageChannels.queue().getObject();
    }
    @Bean
    IntegrationFlow inbound(
            @Qualifier("requests") DirectChannel requests,
            ConnectionFactory connectionFactory
    ) {
        return IntegrationFlow
                .from(Amqp.inboundAdapter(connectionFactory, "requests"))
                .channel(requests)
                .get();
    }

    @Bean
    IntegrationFlow outboundReplies(
            //@WorkerOutboundChunkChannel
            @Qualifier("replies") PollableChannel replies,
            AmqpTemplate template
    ) {
        return IntegrationFlow //
                .from(replies)
                .handle(Amqp.outboundAdapter(template).routingKey("replies"))
                .get();
    }

    private YearReport deserializeYearReportJson(String json) {
        try {
            return objectMapper.readValue(json, YearReport.class);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("oops! couldn't parse the JSON!", e);
        }
    }

    private static void doSomethingTimeIntensive(YearReport yearReport) {
        System.out.println("====================");
        System.out.println("got yearReport");
        System.out.println(yearReport.toString());
    }

    @Bean
        //@WorkerItemProcessor
    ItemProcessor<String, YearReport> itemProcessor() {
        return yearReportJson -> {
            System.out.println(">> processing YearReport JSON: " + yearReportJson);
            Thread.sleep(5);
            return deserializeYearReportJson(yearReportJson);
        };
    }

    @Bean
        //@WorkerItemWriter
    ItemWriter<YearReport> writer() {
        return chunk -> chunk.getItems().forEach(SlaveConfiguration::doSomethingTimeIntensive);
    }
}




