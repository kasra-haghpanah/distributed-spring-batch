package com.example.batchprocessing.slave.integration;

import com.example.batchprocessing.slave.configuration.properties.Properties;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.batch.core.step.item.SimpleChunkProcessor;
import org.springframework.batch.integration.chunk.ChunkProcessorChunkHandler;
import org.springframework.batch.integration.chunk.ChunkRequest;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.amqp.dsl.Amqp;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.MessageChannels;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.Assert;

import java.text.MessageFormat;
import java.util.List;


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
            ConnectionFactory connectionFactory,
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
    @Qualifier("slaveItemProcessor")
    ItemProcessor<Object, Object> itemProcessor() {
        return item -> {
            System.out.println(item);
            return item;
        };
    }

    @Bean
    @Qualifier("slaveItemWriter")
    ItemWriter<Object> itemWriter() {
        return chunk -> {
            System.out.println("doing the long-running writing thing");
            List<?> items = chunk.getItems();
            for (var i : items)
                System.out.println(MessageFormat.format("i={0}", i));
        };
    }

    @Bean
    @Qualifier("slaveChunkProcessorChunkHandler")
    @ConditionalOnMissingBean
    @SuppressWarnings("unchecked")
    ChunkProcessorChunkHandler<?> slaveChunkProcessorChunkHandler(
            // todo make this optional
            // @Qualifier("slaveItemProcessor") ObjectProvider<ItemProcessor<Object, Object>> processor,
            @Qualifier("slaveItemProcessor") ItemProcessor<?, ?> processor,
            @Qualifier("slaveItemWriter") ItemWriter<?> writer
    ) {
        var chunkProcessorChunkHandler = new ChunkProcessorChunkHandler<>();
        chunkProcessorChunkHandler.setChunkProcessor(new SimpleChunkProcessor(processor, writer));
        return chunkProcessorChunkHandler;
    }

    // todo connect this with rabbitmq or kafka or something real so I can setup a worker node
    @Bean
    @SuppressWarnings("unchecked")
    IntegrationFlow chunkProcessorChunkHandlerIntegrationFlow(
            @Qualifier("slaveChunkProcessorChunkHandler") ChunkProcessorChunkHandler<Object> chunkProcessorChunkHandler,
            @Qualifier("slaveInboundChunkChannel") DirectChannel inbound,
            @Qualifier("slaveOutboundChunkChannel") DirectChannel outbound
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
    }

}
