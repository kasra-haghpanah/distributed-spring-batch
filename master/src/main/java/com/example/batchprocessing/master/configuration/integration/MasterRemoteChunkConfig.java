package com.example.batchprocessing.master.configuration.integration;

import com.example.batchprocessing.master.configuration.properties.Properties;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.aot.hint.MemberCategory;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.RuntimeHintsRegistrar;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.integration.chunk.ChunkMessageChannelItemWriter;
import org.springframework.batch.integration.chunk.RemoteChunkingManagerStepBuilderFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportRuntimeHints;
import org.springframework.integration.amqp.dsl.Amqp;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.MessageChannels;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.Set;

@Configuration
@ConditionalOnProperty(value = "bootiful.batch.chunk.master", havingValue = "true")
@ImportRuntimeHints(MasterRemoteChunkConfig.Hints.class)
class MasterRemoteChunkConfig {

    static class Hints implements RuntimeHintsRegistrar {

        @Override
        public void registerHints(RuntimeHints hints, ClassLoader classLoader) {
            Set.of(ChunkMessageChannelItemWriter.class)
                    .forEach(c -> hints.reflection().registerType(c, MemberCategory.values()));
        }

    }
    @Bean
    RemoteChunkingManagerStepBuilderFactory customerRemoteChunkingManager(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new RemoteChunkingManagerStepBuilderFactory(jobRepository, transactionManager);
    }
    @Bean // Configure outbound flow (requests going to workers)
    @Qualifier("masterOutboundCustomerRequest")
    DirectChannel masterCustomerRequestMessageChannel() {
        return MessageChannels.direct().getObject();
    }
    @Bean
    IntegrationFlow outboundCustomerIntegrationFlow(AmqpTemplate amqpTemplate) {
        return IntegrationFlow //
                .from(masterCustomerRequestMessageChannel())//
                .handle(Amqp.outboundAdapter(amqpTemplate).routingKey(Properties.getRabbitmqQueueOne()))//requests
                .get();
    }
    @Bean // Configure inbound flow (replies coming from workers)
    @Qualifier("masterInboundCustomerReply")
    QueueChannel masterCustomerReplieMessageChannel() {
        return MessageChannels.queue().getObject();
    }
    @Bean
    IntegrationFlow inboundCustomerIntegrationFlow(ConnectionFactory connectionFactory) {
        return IntegrationFlow//
                .from(Amqp.inboundAdapter(connectionFactory, Properties.getRabbitmqQueueTwo()))//replies
                .channel(masterCustomerReplieMessageChannel())//
                .get();
    }

    @Bean // Configure outbound flow (requests going to workers)
    @Qualifier("masterOutboundYearReport")
    DirectChannel masterYearReportRequestMessageChannel() {
        return MessageChannels.direct().getObject();
    }
    @Bean
    IntegrationFlow outboundYearReportIntegrationFlow(AmqpTemplate amqpTemplate) {
        return IntegrationFlow //
                .from(masterYearReportRequestMessageChannel())//
                .handle(Amqp.outboundAdapter(amqpTemplate).routingKey(Properties.getRabbitmqQueueThree()))//requests
                .get();
    }

    @Bean // Configure inbound flow (replies coming from workers)
    @Qualifier("masterInboundYearReport")
    QueueChannel masterYearReportReplieMessageChannel() {
        return MessageChannels.queue().getObject();
    }
    @Bean
    IntegrationFlow inboundYearReportIntegrationFlow(ConnectionFactory connectionFactory) {
        return IntegrationFlow//
                .from(Amqp.inboundAdapter(connectionFactory, Properties.getRabbitmqQueueFour()))//replies
                .channel(masterYearReportReplieMessageChannel())//
                .get();
    }

}
