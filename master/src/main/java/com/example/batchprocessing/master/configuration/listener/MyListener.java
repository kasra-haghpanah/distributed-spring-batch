package com.example.batchprocessing.master.configuration.listener;

import com.example.batchprocessing.master.configuration.properties.Properties;
import com.example.batchprocessing.master.configuration.rabbitmq.Receiver;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.DependsOn;

import java.nio.charset.StandardCharsets;

@DependsOn("properties")
@CoffeeSoftwareComponent
public class MyListener implements ApplicationListener<ApplicationReadyEvent> {

    final RabbitTemplate rabbitTemplate;
    final Receiver receiver;

    public MyListener(RabbitTemplate rabbitTemplate, Receiver receiver) {
        this.rabbitTemplate = rabbitTemplate;
        this.receiver = receiver;
    }

    @Override
    public void onApplicationEvent(ApplicationReadyEvent event) {
        System.out.println("Hello, from a custom component!");
        //rabbitTemplate.convertAndSend(Properties.getRabbitmqTopicExchange(), Properties.getRabbitmqTopicExchangeRoutingKeyOne(), "Hello from RabbitMQ by requests!".getBytes(StandardCharsets.UTF_8));
        //rabbitTemplate.convertAndSend(Properties.getRabbitmqTopicExchange(), Properties.getRabbitmqTopicExchangeRoutingKeyTwo(), "Hello from RabbitMQ by replies!".getBytes(StandardCharsets.UTF_8));
    }
}
