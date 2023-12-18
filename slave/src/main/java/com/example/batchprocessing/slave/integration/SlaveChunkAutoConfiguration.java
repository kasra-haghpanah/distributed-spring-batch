package com.example.batchprocessing.slave.integration;

import com.example.batchprocessing.slave.configuration.jackson.Customer;
import com.example.batchprocessing.slave.configuration.properties.Properties;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.batch.integration.chunk.RemoteChunkingWorkerBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.integration.amqp.dsl.Amqp;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.MessageChannels;
import org.springframework.messaging.MessageChannel;

import java.text.MessageFormat;
import java.util.List;

//@EnableBatchIntegration
//@EnableBatchProcessing
@Configuration
@ConditionalOnProperty(value = "bootiful.batch.chunk.slave", havingValue = "true")
class SlaveChunkAutoConfiguration {


    @Bean
    @Primary
    @Qualifier("rabbitMQConnectionFactory")
    ConnectionFactory connectionFactory() {
        CachingConnectionFactory connectionFactory = new CachingConnectionFactory();
        connectionFactory.setAddresses(Properties.getRabbitmqHost());
        connectionFactory.setPort(Properties.getRabbitmqPort());
        connectionFactory.setUsername(Properties.getRabbitmqUsername());
        connectionFactory.setPassword(Properties.getRabbitmqPassword());
        return connectionFactory;
    }

    @Bean
    @Primary
    AmqpTemplate amqpTemplate(ConnectionFactory connectionFactory) {
        return new RabbitTemplate(connectionFactory);
    }

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
                .itemWriter(itemWriter)
                .inputChannel(inbound) // requests received from the manager
                .outputChannel(outbound) // replies sent to the manager
                .build();
    }

    @Bean
    @Qualifier("slaveItemProcessor")
    ItemProcessor<?, ?> itemProcessor(ObjectMapper objectMapper) {
        return item -> {
            System.out.println(item);

            List<Customer> customers = null;
            customers = objectMapper.readValue((String) item, new TypeReference<List<Customer>>() {
            });

            return customers;
        };
    }

//    @Bean
//    @Qualifier("slaveItemWriter")
    ItemWriter<?> itemWriter = chunk -> {
        System.out.println("doing the long-running writing thing");
        List<?> items = chunk.getItems();
        for (var i : items)
            System.out.println(MessageFormat.format("i={0}", i));
    };


    // Middleware beans setup omitted

/*    @Bean
    @Qualifier("slaveChunkProcessorChunkHandler")
    @ConditionalOnMissingBean
    @SuppressWarnings("unchecked")
    ChunkProcessorChunkHandler<?> slaveChunkProcessorChunkHandler(
            // todo make this optional
            // @Qualifier("slaveItemProcessor") ObjectProvider<ItemProcessor<Object, Object>> processor,
            @Qualifier("slaveItemProcessor") ItemProcessor<?, ?> itemProcessor,
            @Qualifier("slaveItemWriter") ItemWriter<?> itemWriter
    ) {
        var chunkProcessorChunkHandler = new ChunkProcessorChunkHandler<>();
        chunkProcessorChunkHandler.setChunkProcessor(new SimpleChunkProcessor(itemProcessor, itemWriter));
        return chunkProcessorChunkHandler;
    }

    // todo connect this with rabbitmq or kafka or something real so I can setup a worker node
    @Bean
    @SuppressWarnings("unchecked")
    IntegrationFlow chunkProcessorChunkHandlerIntegrationFlow(
            @Qualifier("slaveChunkProcessorChunkHandler") ChunkProcessorChunkHandler<Object> chunkProcessorChunkHandler,
            @Qualifier("slaveInboundChunkChannel") DirectChannel inbound//,
            //@Qualifier("slaveOutboundChunkChannel") DirectChannel outbound
    ) {
        return IntegrationFlow//
                .from(inbound)//
                .handle(message -> {
                    try {
                        var payload = message.getPayload();
                        if (payload instanceof ChunkRequest<?> cr) {
                            var chunkResponse = chunkProcessorChunkHandler.handleChunk((ChunkRequest<Object>) cr);
                            outbound.send(MessageBuilder.withPayload(chunkResponse).build());
                        }
                        Assert.state(payload instanceof ChunkRequest<?>, "the payload must be an instance of ChunkRequest!");
                    } //
                    catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                })//
                .get();
    }*/

}
