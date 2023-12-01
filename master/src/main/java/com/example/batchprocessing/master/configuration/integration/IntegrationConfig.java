package com.example.batchprocessing.master.configuration.integration;

import com.example.batchprocessing.master.configuration.properties.Properties;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.integration.amqp.dsl.Amqp;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.dsl.IntegrationFlow;

//@DependsOn("properties")
//@Configuration
public class IntegrationConfig {

    //@Bean
    IntegrationFlow outboundFlow(
            AmqpTemplate amqpTemplate,
            @Qualifier("requests") DirectChannel requests
    ) {
        return IntegrationFlow.from(requests)
                .handle(Amqp.outboundAdapter(amqpTemplate).routingKey(Properties.getRabbitmqTopicExchangeRoutingKeyOne()))
                .get();
    }

    //@Bean
    IntegrationFlow replyFlow(
            ConnectionFactory connectionFactory,
            @Qualifier("replies") QueueChannel replies
    ) {
        return IntegrationFlow
                .from(Amqp.inboundAdapter(connectionFactory, Properties.getRabbitmqQueueTwo()))
                .channel(replies)
                .get();
    }

}
