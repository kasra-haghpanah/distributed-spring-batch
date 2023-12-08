package com.example.batchprocessing.master.configuration.integration;

import com.example.batchprocessing.master.configuration.properties.Properties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.amqp.core.AmqpTemplate;

import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.integration.chunk.ChunkMessageChannelItemWriter;
import org.springframework.batch.integration.chunk.ChunkRequest;
import org.springframework.batch.integration.chunk.ChunkResponse;
import org.springframework.batch.integration.chunk.RemoteChunkHandlerFactoryBean;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.integration.amqp.dsl.Amqp;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.core.MessagingTemplate;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.MessageChannels;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.PollableChannel;
import org.springframework.messaging.support.MessageBuilder;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;

//@DependsOn("properties")
@Configuration
public class IntegrationConfig {

    @Bean
    @Qualifier("requests")// outbound
    MessageChannel requests() {
        return MessageChannels.direct().getObject();
    }

    @Bean
    @Qualifier("replies")// inbound
    PollableChannel replies() {
        return MessageChannels.queue().getObject();
    }

    @Bean
    @Qualifier("remoteChunkMessagingTemplate")
    public MessagingTemplate remoteChunkMessagingTemplate(ConnectionFactory connectionFactory) {
        MessagingTemplate messagingTemplate = new MessagingTemplate();
        messagingTemplate.setDefaultChannel(requests());
        messagingTemplate.setReceiveTimeout(2000);
        return messagingTemplate;
    }

    // todo connect this with rabbitmq or kafka or something real so I can setup a worker node
    @Bean
    IntegrationFlow chunkIntegrationFlow(){
        return IntegrationFlow
                .from(requests())
                .handle(message ->{
                    if(message.getPayload() instanceof ChunkRequest<?> chunkRequest){
                        var chunkResponse = new ChunkResponse(chunkRequest.getSequence(), chunkRequest.getJobId(), chunkRequest.getStepContribution());
                        replies().send(MessageBuilder.withPayload(chunkResponse).build());
                    }
                })
                .get();
    }

    @Bean
    @StepScope
    ChunkMessageChannelItemWriter<String> chunkMessageChannelItemWriter(
            @Qualifier("replies") PollableChannel replies,
            @Qualifier("remoteChunkMessagingTemplate") MessagingTemplate remoteChunkMessagingTemplate
    ) {
        var chunkMessageChannelItemWriter = new ChunkMessageChannelItemWriter<String>();
        chunkMessageChannelItemWriter.setMessagingOperations(remoteChunkMessagingTemplate);
        chunkMessageChannelItemWriter.setReplyChannel(replies);
        return chunkMessageChannelItemWriter;
    }

    @Bean
    RemoteChunkHandlerFactoryBean<String> chunkHandler(
            ChunkMessageChannelItemWriter<String> chunkMessageChannelItemWriter,
            @Qualifier("yearReportStep") TaskletStep yearReportStep
    ) {
        var remoteChunkHandlerFactoryBean = new RemoteChunkHandlerFactoryBean<String>();
        remoteChunkHandlerFactoryBean.setChunkWriter(chunkMessageChannelItemWriter);
        remoteChunkHandlerFactoryBean.setStep(yearReportStep);
        return remoteChunkHandlerFactoryBean;
    }

/*    @Bean
    IntegrationFlow outboundFlow(AmqpTemplate amqpTemplate) {
        return IntegrationFlow.from(requests())
                .handle(Amqp.outboundAdapter(amqpTemplate).routingKey(Properties.getRabbitmqTopicExchangeRoutingKeyOne()))
                .get();
    }

    @Bean
    IntegrationFlow replyFlow(ConnectionFactory connectionFactory) {
        return IntegrationFlow
                .from(Amqp.inboundAdapter(connectionFactory, Properties.getRabbitmqQueueTwo()))
                .channel(replies())
                .get();
    }*/

}
