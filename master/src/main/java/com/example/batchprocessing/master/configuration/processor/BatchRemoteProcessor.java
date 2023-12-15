package com.example.batchprocessing.master.configuration.processor;

import com.example.batchprocessing.master.configuration.structure.Customer;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.batch.integration.async.AsyncItemProcessor;
import org.springframework.batch.integration.async.AsyncItemWriter;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;

import java.util.Arrays;
import java.util.List;

@Configuration
public class BatchRemoteProcessor {

    @Bean
    @Qualifier("masterItemReader")
    public ItemReader<Customer> itemReader() {
        return new ListItemReader<Customer>(Arrays.asList(new Customer("Dave"), new Customer("Michael"), new Customer("Mahmoud")));
    }

    @Bean
    @Qualifier("masterItemProcessor")
    ItemProcessor<Object, Object> masterItemProcessor(ObjectMapper objectMapper) {
        return (customers) -> {
            return objectMapper.writeValueAsString(customers);
        };
    }

/*    @Bean
    @Qualifier("masterAsyncItemProcessor")
    public AsyncItemProcessor itemProcessor(
            @Qualifier("masterItemProcessor") ItemProcessor<List<Customer>, String> itemProcessor,
            TaskExecutor taskExecutor
    ) {
        AsyncItemProcessor asyncItemProcessor = new AsyncItemProcessor();
        asyncItemProcessor.setTaskExecutor(taskExecutor);
        asyncItemProcessor.setDelegate(itemProcessor);
        return asyncItemProcessor;
    }*/

    @Bean
    @Qualifier("masterItemWriter")
    ItemWriter<Object> masterItemWriter() {
        return (item) -> {
            System.out.println(item);
        };
    }

/*    @Bean
    @Qualifier("masterAsyncItemWriter")
    public AsyncItemWriter writer(@Qualifier("masterItemWriter") ItemWriter<String> masterItemWriter) {
        AsyncItemWriter asyncItemWriter = new AsyncItemWriter();
        asyncItemWriter.setDelegate(masterItemWriter);
        return asyncItemWriter;
    }*/

}
