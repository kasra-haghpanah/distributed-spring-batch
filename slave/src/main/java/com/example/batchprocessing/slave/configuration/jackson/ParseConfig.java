package com.example.batchprocessing.slave.configuration.jackson;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.amqp.support.converter.DefaultClassMapper;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class ParseConfig {

    @Bean
    @Primary
    @Qualifier("jsonMapper")
    public ObjectMapper getObjectMapper() {
        return new ObjectMapper();
    }

    @Bean
    public Jackson2JsonMessageConverter getConverter(
            @Qualifier("jsonMapper")  ObjectMapper objectMapper) {
        Jackson2JsonMessageConverter messageConverter =
                new Jackson2JsonMessageConverter(objectMapper);
        messageConverter.setClassMapper(getClassMapper());
        return messageConverter;
    }

    @Bean
    public DefaultClassMapper getClassMapper() {
        DefaultClassMapper classMapper = new DefaultClassMapper();
        Map<String, Class<?>> map = new HashMap<>();
        map.put(
                "com.example.batchprocessing.slave.configuration.jackson.Customer",
                Customer.class);
        //classMapper.setIdClassMapping(idClassMapping);
        return classMapper;
    }
}

