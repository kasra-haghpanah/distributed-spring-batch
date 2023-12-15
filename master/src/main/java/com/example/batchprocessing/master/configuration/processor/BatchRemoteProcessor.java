package com.example.batchprocessing.master.configuration.processor;

import com.example.batchprocessing.master.configuration.structure.Customer;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Arrays;

@Configuration
public class BatchRemoteProcessor {

    @Bean
    @Qualifier("masterItemReader")
    public ListItemReader<Customer> itemReader() {
        return new ListItemReader<Customer>(Arrays.asList(new Customer("Dave"), new Customer("Michael"), new Customer("Mahmoud")));
    }

    @Bean
    @Qualifier("masterItemProcessor")
    ItemProcessor<Object, Object> masterItemProcessor(ObjectMapper objectMapper) {
        return (customers) -> {
            return objectMapper.writeValueAsString(customers);
        };
    }

    @Bean
    @Qualifier("masterItemWriter")
    ItemWriter<Object> masterItemWriter() {
        return (item) -> {
            System.out.println(item);
        };
    }

}
