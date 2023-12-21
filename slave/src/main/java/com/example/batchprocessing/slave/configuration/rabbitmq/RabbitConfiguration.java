package com.example.batchprocessing.slave.configuration.rabbitmq;

import com.example.batchprocessing.slave.configuration.properties.Properties;
import org.springframework.amqp.core.AmqpTemplate;
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
    @Primary
    @Qualifier("rabbitMQConnectionFactory")
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
        RabbitTemplate  amqpTemplate = new RabbitTemplate(connectionFactory);
        //amqpTemplate.setMessageConverter(jsonToMapMessageConverter());
        return amqpTemplate;
    }

/*    @Bean
    @Primary
    public MessageConverter jsonToMapMessageConverter() {
        DefaultClassMapper defaultClassMapper = new DefaultClassMapper();
        defaultClassMapper.setTrustedPackages("org.springframework.batch.integration.chunk.*"); // trusted packages
        defaultClassMapper.setDefaultMapClass(ChunkRequest.class);
        Jackson2JsonMessageConverter jackson2JsonMessageConverter = new Jackson2JsonMessageConverter();
        //jackson2JsonMessageConverter.setClassMapper(new ImporterClassMapper(FileUploadMessage.class));
        jackson2JsonMessageConverter.setClassMapper(defaultClassMapper);
        return jackson2JsonMessageConverter;
    }*/


}
