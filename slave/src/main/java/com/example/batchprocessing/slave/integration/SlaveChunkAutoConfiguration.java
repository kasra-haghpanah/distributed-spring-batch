package com.example.batchprocessing.slave.integration;

import com.example.batchprocessing.slave.configuration.jackson.Customer;
import com.example.batchprocessing.slave.configuration.properties.Properties;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.batch.integration.chunk.RemoteChunkingWorkerBuilder;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.amqp.dsl.Amqp;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.MessageChannels;
import org.springframework.messaging.MessageChannel;

import java.util.List;

//@EnableBatchIntegration
//@EnableBatchProcessing
@Configuration
@ConditionalOnProperty(value = "bootiful.batch.chunk.slave", havingValue = "true")
class SlaveChunkAutoConfiguration {


    @Bean
    @Qualifier("slaveInboundChunkChannel")
    DirectChannel slaveRequestsMessageChannel() {
        return MessageChannels.direct().getObject();
    }

    @Bean
    IntegrationFlow inboundAmqpIntegrationFlow(
            @Qualifier("rabbitMQConnectionFactory") ConnectionFactory connectionFactory,
            @Qualifier("slaveInboundChunkChannel") MessageChannel inbound

    ) {
        return IntegrationFlow//
                .from(Amqp.inboundAdapter(connectionFactory, Properties.getRabbitmqQueueOne()))//requests
                .channel(inbound)//
                .get();
    }

    @Bean
    @Qualifier("slaveOutboundChunkChannel")
    DirectChannel slaveRepliesMessageChannel() {
        return MessageChannels.direct().getObject();
    }

    @Bean
    IntegrationFlow outboundAmqpIntegrationFlow(
            AmqpTemplate template,
            @Qualifier("slaveOutboundChunkChannel") MessageChannel outbound
    ) {
        return IntegrationFlow //
                .from(outbound)//
                .handle(Amqp.outboundAdapter(template).routingKey(Properties.getRabbitmqTopicExchangeRoutingKeyTwo()))//replies
                .get();
    }


    @Bean
    @Qualifier("remoteChunkingWorkerBuilder")
    RemoteChunkingWorkerBuilder remoteChunkingWorkerBuilder() {
        return new RemoteChunkingWorkerBuilder();
    }

    @Bean
    public IntegrationFlow workerFlow(
            @Qualifier("remoteChunkingWorkerBuilder") RemoteChunkingWorkerBuilder workerBuilder,
            @Qualifier("slaveInboundChunkChannel") DirectChannel inbound,
            @Qualifier("slaveOutboundChunkChannel") DirectChannel outbound,
            @Qualifier("slaveItemProcessor") ItemProcessor<?, ?> itemProcessor
    ) {
        return workerBuilder
                .itemProcessor(itemProcessor)
                .itemWriter(this::itemWriter)
                .inputChannel(inbound) // requests received from the manager
                .outputChannel(outbound) // replies sent to the manager
                .build();
    }

    @Bean
    @Qualifier("slaveItemProcessor")
    ItemProcessor<?, Customer> itemProcessor(ObjectMapper objectMapper) {
        return item -> {
            System.out.println(item);
            Customer customer = objectMapper.readValue((String) item, Customer.class);
            return customer;
        };
    }
    public void itemWriter(Chunk<Customer> chunk) {
        System.out.println("doing the long-running writing thing");
        List<Customer> items = chunk.getItems();
        for (var customer : items)
            System.out.println("itemWriter => " + customer);
    }

}
