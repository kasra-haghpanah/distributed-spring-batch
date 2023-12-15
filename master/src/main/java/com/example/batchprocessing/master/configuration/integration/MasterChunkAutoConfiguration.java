package com.example.batchprocessing.master.configuration.integration;

import com.example.batchprocessing.master.configuration.properties.Properties;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.aot.hint.MemberCategory;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.RuntimeHintsRegistrar;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.integration.chunk.ChunkMessageChannelItemWriter;
import org.springframework.batch.integration.chunk.RemoteChunkHandlerFactoryBean;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportRuntimeHints;
import org.springframework.integration.amqp.dsl.Amqp;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.core.MessagingTemplate;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.MessageChannels;
import org.springframework.messaging.MessageChannel;

import java.util.Set;

@Configuration
@ConditionalOnProperty(value = "bootiful.batch.chunk.master", havingValue = "true")
@ImportRuntimeHints(MasterChunkAutoConfiguration.Hints.class)
class MasterChunkAutoConfiguration {

    static class Hints implements RuntimeHintsRegistrar {

        @Override
        public void registerHints(RuntimeHints hints, ClassLoader classLoader) {
            Set.of(ChunkMessageChannelItemWriter.class)
                    .forEach(c -> hints.reflection().registerType(c, MemberCategory.values()));
        }

    }

    /*
     * Configure outbound flow (requests going to workers)
     */

    @Bean
    @Qualifier("masterOutboundChunkChannel")
    DirectChannel masterRequestsMessageChannel() {
        return MessageChannels.direct().getObject();
    }

    @Bean
    IntegrationFlow outboundIntegrationFlow(@Qualifier("masterOutboundChunkChannel") MessageChannel out, AmqpTemplate amqpTemplate) {
        return IntegrationFlow //
                .from(out)//
                .handle(Amqp.outboundAdapter(amqpTemplate).routingKey(Properties.getRabbitmqTopicExchangeRoutingKeyOne()))//requests
                .get();
    }


    /*
     * Configure inbound flow (replies coming from workers)
     */
    @Bean
    @Qualifier("masterInboundChunkChannel")
    QueueChannel masterRepliesMessageChannel() {
        return MessageChannels.queue().getObject();
    }

    @Bean
    IntegrationFlow inboundIntegrationFlow(ConnectionFactory cf, @Qualifier("masterInboundChunkChannel") MessageChannel in) {
        return IntegrationFlow//
                .from(Amqp.inboundAdapter(cf, Properties.getRabbitmqTopicExchangeRoutingKeyTwo()))//replies
                .channel(in)//
                .get();
    }

/*    @Bean
    @ConditionalOnMissingBean
    @Qualifier("masterChunkMessagingTemplate")
    MessagingTemplate masterChunkMessagingTemplate(@Qualifier("masterOutboundChunkChannel") DirectChannel masterRequestsMessageChannel) {
        var template = new MessagingTemplate();
        template.setDefaultChannel(masterRequestsMessageChannel);
        template.setReceiveTimeout(2000);
        return template;
    }*/

/*    @Bean
    @ConditionalOnMissingBean
    RemoteChunkHandlerFactoryBean<Object> masterChunkHandler(
            ChunkMessageChannelItemWriter<Object> chunkMessageChannelItemWriterProxy,
            //@Qualifier("yearReportStep") TaskletStep step
            @Qualifier("managerStep") TaskletStep managerStep
    ) {
        var remoteChunkHandlerFactoryBean = new RemoteChunkHandlerFactoryBean<>();
        remoteChunkHandlerFactoryBean.setChunkWriter(chunkMessageChannelItemWriterProxy);
        remoteChunkHandlerFactoryBean.setStep(managerStep);
        return remoteChunkHandlerFactoryBean;
    }*/

/*    @Bean
    @Qualifier("masterChunkItemWriter")
        // @StepScope
    ChunkMessageChannelItemWriter<?> masterChunkMessageChannelItemWriter(
            @Qualifier("masterChunkMessagingTemplate") MessagingTemplate template,
            @Qualifier("masterInboundChunkChannel") QueueChannel masterRepliesMessageChannel
    ) {

        ChunkMessageChannelItemWriter<Integer> chunkMessageChannelItemWriter = new ChunkMessageChannelItemWriter<>();
        chunkMessageChannelItemWriter.setMessagingOperations(template);
        chunkMessageChannelItemWriter.setReplyChannel(masterRepliesMessageChannel);
        return chunkMessageChannelItemWriter;
    }*/




    // todo connect this with rabbitmq or kafka or something real so I can setup a worker node
    //@Bean
/*    IntegrationFlow chunkIntegrationFlow(){
        return IntegrationFlow
                .from(requests())
                .handle(message ->{
                    if(message.getPayload() instanceof ChunkRequest<?> chunkRequest){
                        var chunkResponse = new ChunkResponse(chunkRequest.getSequence(), chunkRequest.getJobId(), chunkRequest.getStepContribution());
                        replies().send(MessageBuilder.withPayload(chunkResponse).build());
                    }
                })
                .get();
    }*/

}
