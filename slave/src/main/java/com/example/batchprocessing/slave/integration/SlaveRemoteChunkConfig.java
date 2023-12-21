package com.example.batchprocessing.slave.integration;

import com.example.batchprocessing.slave.configuration.properties.Properties;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.batch.integration.chunk.RemoteChunkingWorkerBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.integration.amqp.dsl.Amqp;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.MessageChannels;

@Configuration
public class SlaveRemoteChunkConfig {

    @Bean
    @Primary
    RemoteChunkingWorkerBuilder remoteChunkingWorkerBuilder() {
        return new RemoteChunkingWorkerBuilder();
    }
    @Bean
    @Qualifier("slaveInboundCustomerRequest")
    DirectChannel slaveInboundCustomerRequest() {
        return MessageChannels.direct().getObject();
    }

    @Bean
    IntegrationFlow inboundAmqpCustomerIntegrationFlow(ConnectionFactory connectionFactory) {
        return IntegrationFlow//
                .from(Amqp.inboundAdapter(connectionFactory, Properties.getRabbitmqQueueOne()))//requests
                .channel(slaveInboundCustomerRequest())//
                .get();
    }

    @Bean
    @Qualifier("slaveOutboundCustomerReply")
    DirectChannel slaveOutboundCustomerReply() {
        return MessageChannels.direct().getObject();
    }

    @Bean
    IntegrationFlow outboundAmqpCustomerIntegrationFlow(AmqpTemplate template) {
        return IntegrationFlow //
                .from(slaveOutboundCustomerReply())//
                .handle(Amqp.outboundAdapter(template).routingKey(Properties.getRabbitmqQueueTwo()))//replies
                .get();
    }

    @Bean
    @Qualifier("slaveInboundYearReportRequest")
    DirectChannel slaveInboundYearReportRequest() {
        return MessageChannels.direct().getObject();
    }

    @Bean
    IntegrationFlow inboundAmqpYearReportIntegrationFlow(ConnectionFactory connectionFactory) {
        return IntegrationFlow//
                .from(Amqp.inboundAdapter(connectionFactory, Properties.getRabbitmqQueueThree()))//requests
                .channel(slaveInboundYearReportRequest())//
                .get();
    }

    @Bean
    @Qualifier("slaveOutboundYearReportReply")
    DirectChannel slaveOutboundYearReportReply() {
        return MessageChannels.direct().getObject();
    }

    @Bean
    IntegrationFlow outboundAmqpYearReporIntegrationFlow(AmqpTemplate template) {
        return IntegrationFlow //
                .from(slaveOutboundYearReportReply())//
                .handle(Amqp.outboundAdapter(template).routingKey(Properties.getRabbitmqQueueFour()))//replies
                .get();
    }

}
