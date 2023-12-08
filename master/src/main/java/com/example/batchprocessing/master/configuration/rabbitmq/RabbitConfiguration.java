package com.example.batchprocessing.master.configuration.rabbitmq;

import com.example.batchprocessing.master.configuration.properties.Properties;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.core.MessagingTemplate;
import org.springframework.integration.dsl.MessageChannels;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.PollableChannel;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * installs all the infrastructure for RabbitMQ
 */
// https://spring.io/guides/gs/messaging-rabbitmq/
// https://hevodata.com/learn/spring-message-queue/
// https://medium.com/javarevisited/getting-started-with-rabbitmq-in-spring-boot-6323b9179247
@DependsOn("properties")
@Configuration
public class RabbitConfiguration {

    @Bean
    public ConnectionFactory connectionFactory() {
        CachingConnectionFactory connectionFactory = new CachingConnectionFactory();
        connectionFactory.setAddresses(Properties.getRabbitmqHost());
        connectionFactory.setPort(Properties.getRabbitmqPort());
        connectionFactory.setUsername(Properties.getRabbitmqUsername());
        connectionFactory.setPassword(Properties.getRabbitmqPassword());
        return connectionFactory;
    }

    @Bean
    public RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory) {
        return new RabbitTemplate(connectionFactory);
    }

    @Bean
    @Qualifier("requestQueue")
    Queue requestQueue() {
        return new org.springframework.amqp.core.Queue(Properties.getRabbitmqQueueOne(), false);
    }

    @Bean
    @Qualifier("repliesQueue")
    Queue repliesQueue() {
        return new Queue(Properties.getRabbitmqQueueTwo(), false);
    }

    @Bean
    TopicExchange exchange() {
        return new TopicExchange(Properties.getRabbitmqTopicExchange());
    }

    @Bean
    Binding requestBinding(TopicExchange exchange) {
        return BindingBuilder.bind(requestQueue()).to(exchange).with(Properties.getRabbitmqTopicExchangeRoutingKeyOne());
    }

    @Bean
    Binding repliesBinding(TopicExchange exchange) {
        return BindingBuilder.bind(repliesQueue()).to(exchange).with(Properties.getRabbitmqTopicExchangeRoutingKeyTwo());
    }

    @Bean
    SimpleMessageListenerContainer container(ConnectionFactory connectionFactory, MessageListenerAdapter listenerAdapter) {
        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        container.setQueueNames(Properties.getRabbitmqTopicExchangeRoutingKeyOne(), Properties.getRabbitmqTopicExchangeRoutingKeyTwo());
        container.setMessageListener(listenerAdapter);
        return container;
    }

    @Bean
    MessageListenerAdapter listenerAdapter(Receiver receiver) {
        return new MessageListenerAdapter(receiver, "receiveMessage");
    }


}
