package com.example.batchprocessing.slave.integration;

import com.example.batchprocessing.slave.configuration.properties.Properties;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.integration.chunk.RemoteChunkingWorkerBuilder;
import org.springframework.batch.integration.partition.RemotePartitioningWorkerStepBuilderFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.integration.amqp.dsl.Amqp;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.MessageChannels;

@Configuration(proxyBeanMethods = false)
public class SlaveRemoteChunkConfig {

    @Bean
    @Primary
    RemoteChunkingWorkerBuilder remoteChunkingWorkerBuilder() {
        return new RemoteChunkingWorkerBuilder();
    }

    @Bean
    @Primary
    RemotePartitioningWorkerStepBuilderFactory remotePartitioningWorkerStepBuilderFactory(JobRepository jobRepository, JobExplorer jobExplorer) {
        return new RemotePartitioningWorkerStepBuilderFactory(jobRepository, jobExplorer);
    }

    @Bean
    @Qualifier("slaveInboundCustomerRequest")
    DirectChannel slaveInboundCustomerRequest() {
        return MessageChannels.direct().getObject();
    }

    @Bean
    IntegrationFlow inboundAmqpCustomerIntegrationFlow(
            ConnectionFactory connectionFactory,
            @Qualifier("slaveInboundCustomerRequest") DirectChannel slaveInboundCustomerRequest
    ) {
        return IntegrationFlow//
                .from(Amqp.inboundAdapter(connectionFactory, Properties.getRabbitmqQueueOne()))//requests
                .channel(slaveInboundCustomerRequest)//
                .get();
    }

    @Bean
    @Qualifier("slaveOutboundCustomerReply")
    DirectChannel slaveOutboundCustomerReply() {
        return MessageChannels.direct().getObject();
    }

    @Bean
    IntegrationFlow outboundAmqpCustomerIntegrationFlow(
            AmqpTemplate template,
            @Qualifier("slaveOutboundCustomerReply")DirectChannel slaveOutboundCustomerReply
            ) {
        return IntegrationFlow //
                .from( slaveOutboundCustomerReply)//
                .handle(Amqp.outboundAdapter(template).routingKey(Properties.getRabbitmqQueueTwo()))//replies
                .get();
    }

    @Bean
    @Qualifier("slaveInboundYearReportRequest")
    DirectChannel slaveInboundYearReportRequest() {
        return MessageChannels.direct().getObject();
    }

    @Bean
    IntegrationFlow inboundAmqpYearReportIntegrationFlow(
            ConnectionFactory connectionFactory,
            @Qualifier("slaveInboundYearReportRequest") DirectChannel slaveInboundYearReportRequest
    ) {
        return IntegrationFlow//
                .from(Amqp.inboundAdapter(connectionFactory, Properties.getRabbitmqQueueThree()))//requests
                .channel(slaveInboundYearReportRequest)//
                .get();
    }

    @Bean
    @Qualifier("slaveOutboundYearReportReply")
    DirectChannel slaveOutboundYearReportReply() {
        return MessageChannels.direct().getObject();
    }

    @Bean
    IntegrationFlow outboundAmqpYearReporIntegrationFlow(
            AmqpTemplate template,
            @Qualifier("slaveOutboundYearReportReply")
            DirectChannel slaveOutboundYearReportReply
            ) {
        return IntegrationFlow //
                .from(slaveOutboundYearReportReply)//
                .handle(Amqp.outboundAdapter(template).routingKey(Properties.getRabbitmqQueueFour()))//replies
                .get();
    }

    @Bean
    @Qualifier("slaveInboundGameByYearRequest")
    DirectChannel slaveInboundGameByYearRequest() {
        return MessageChannels.direct().getObject();
    }

    @Bean
    IntegrationFlow inboundAmqpGameByYearIntegrationFlow(
            ConnectionFactory connectionFactory,
            @Qualifier("slaveInboundGameByYearRequest") DirectChannel slaveInboundGameByYearRequest) {
        return IntegrationFlow//
                .from(Amqp.inboundAdapter(connectionFactory, Properties.getRabbitmqQueueFive()))//requests
                .channel(slaveInboundGameByYearRequest)//
                .get();
    }

    @Bean
    @Qualifier("slaveOutboundGameByYearReply")
    DirectChannel slaveOutboundGameByYearReply() {
        return MessageChannels.direct().getObject();
    }

    @Bean
    IntegrationFlow outboundAmqpGameByYearIntegrationFlow(
            AmqpTemplate template,
            @Qualifier("slaveOutboundGameByYearReply") DirectChannel slaveOutboundGameByYearReply
    ) {
        return IntegrationFlow //
                .from(slaveOutboundGameByYearReply)//
                .handle(Amqp.outboundAdapter(template).routingKey(Properties.getRabbitmqQueueSix()))//replies
                .get();
    }




    @Bean
    @Qualifier("slaveInboundEmailRequest")
    DirectChannel slaveInboundEmailRequest() {
        return MessageChannels.direct().getObject();
    }

    @Bean
    IntegrationFlow inboundAmqpEmailIntegrationFlow(
            ConnectionFactory connectionFactory,
            @Qualifier("slaveInboundEmailRequest") DirectChannel slaveInboundEmailRequest
    ) {
        return IntegrationFlow//
                .from(Amqp.inboundAdapter(connectionFactory, Properties.getRabbitmqQueueSeven()))//requests
                .channel(slaveInboundEmailRequest)//
                .get();
    }

    @Bean
    @Qualifier("slaveOutboundEmailReply")
    DirectChannel slaveOutboundEmailReply() {
        return MessageChannels.direct().getObject();
    }

    @Bean
    IntegrationFlow outboundAmqpEmailIntegrationFlow(
            AmqpTemplate template,
            @Qualifier("slaveOutboundEmailReply") DirectChannel slaveOutboundEmailReply
    ) {
        return IntegrationFlow //
                .from(slaveOutboundEmailReply)//
                .handle(Amqp.outboundAdapter(template).routingKey(Properties.getRabbitmqQueueEight()))//replies
                .get();
    }

}
