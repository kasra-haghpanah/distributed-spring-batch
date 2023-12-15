package com.example.batchprocessing.master.configuration.jackson;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
public class ParseConfig {

    @Bean
    @Primary
    @Qualifier("jsonMapper")
    public ObjectMapper getObjectMapper() {
        return new ObjectMapper();
    }

    @Bean
    public Jackson2JsonMessageConverter getConverter(@Qualifier("jsonMapper") ObjectMapper objectMapper) {
        return new Jackson2JsonMessageConverter(objectMapper);
    }

}
