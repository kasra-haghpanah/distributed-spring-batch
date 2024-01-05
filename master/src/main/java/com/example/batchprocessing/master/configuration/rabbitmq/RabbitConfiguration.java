package com.example.batchprocessing.master.configuration.rabbitmq;

import com.example.batchprocessing.master.configuration.properties.Properties;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.annotation.Primary;

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
    @Primary
    public AmqpTemplate amqpTemplate(ConnectionFactory connectionFactory) {
        return new RabbitTemplate(connectionFactory);
    }
    @Bean
    TopicExchange exchange() {
        return new TopicExchange(Properties.getRabbitmqTopicExchange());
    }
    @Bean
    @Qualifier("customerRequestQueue")
    Queue customerRequestQueue() {
        return new Queue(Properties.getRabbitmqQueueOne(), false);
    }
    @Bean
    @Qualifier("customerReplieQueue")
    Queue customerReplieQueue() {
        return new Queue(Properties.getRabbitmqQueueTwo(), false);
    }

    @Bean
    @Qualifier("yearReportRequestQueue")
    Queue yearReportRequestQueue() {
        return new Queue(Properties.getRabbitmqQueueThree(), false);
    }
    @Bean
    @Qualifier("yearReportReplieQueue")
    Queue yearReportReplieQueue() {
        return new Queue(Properties.getRabbitmqQueueFour(), false);
    }

    @Bean
    @Qualifier("gameByYearRequestQueue")
    Queue gameByYearRequestQueue() {
        return new Queue(Properties.getRabbitmqQueueFive(), false);
    }
    @Bean
    @Qualifier("gameByYearReplieQueue")
    Queue gameByYearReplieQueue() {
        return new Queue(Properties.getRabbitmqQueueSix(), false);
    }

    @Bean
    @Qualifier("emailRequestQueue")
    Queue emailRequestQueue() {
        return new Queue(Properties.getRabbitmqQueueSeven(), false);
    }
    @Bean
    @Qualifier("emailReplieQueue")
    Queue emailReplieQueue() {
        return new Queue(Properties.getRabbitmqQueueEight(), false);
    }


    @Bean
    Binding customerRequestBinding(TopicExchange exchange) {
        return BindingBuilder.bind(customerRequestQueue()).to(exchange).with(Properties.getRabbitmqQueueOne());
    }
    @Bean
    Binding customerReplieBinding(TopicExchange exchange) {
        return BindingBuilder.bind(customerReplieQueue()).to(exchange).with(Properties.getRabbitmqQueueTwo());
    }

    @Bean
    Binding yearReportRequestBinding(TopicExchange exchange) {
        return BindingBuilder.bind(yearReportRequestQueue()).to(exchange).with(Properties.getRabbitmqQueueThree());
    }
    @Bean
    Binding yearReportReplieBinding(TopicExchange exchange) {
        return BindingBuilder.bind(yearReportReplieQueue()).to(exchange).with(Properties.getRabbitmqQueueFour());
    }

    @Bean
    Binding gameByYearRequestBinding(TopicExchange exchange) {
        return BindingBuilder.bind(yearReportRequestQueue()).to(exchange).with(Properties.getRabbitmqQueueFive());
    }
    @Bean
    Binding gameByYearReplieBinding(TopicExchange exchange) {
        return BindingBuilder.bind(yearReportReplieQueue()).to(exchange).with(Properties.getRabbitmqQueueSix());
    }

    @Bean
    Binding emailRequestBinding(TopicExchange exchange) {
        return BindingBuilder.bind(yearReportRequestQueue()).to(exchange).with(Properties.getRabbitmqQueueSeven());
    }
    @Bean
    Binding emailReplieBinding(TopicExchange exchange) {
        return BindingBuilder.bind(yearReportReplieQueue()).to(exchange).with(Properties.getRabbitmqQueueEight());
    }

/*    @Bean
    SimpleMessageListenerContainer container(ConnectionFactory connectionFactory, MessageListenerAdapter listenerAdapter) {
        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        container.setQueueNames(Properties.getRabbitmqQueueOne(), Properties.getRabbitmqQueueTwo());
        container.setMessageListener(listenerAdapter);
        return container;
    }

    @Bean
    MessageListenerAdapter listenerAdapter(Receiver receiver) {
        return new MessageListenerAdapter(receiver, "receiveMessage");
    }*/


}
