package com.example.batchprocessing.slave.integration;

import com.example.batchprocessing.slave.structure.Customer;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.batch.integration.chunk.RemoteChunkingWorkerBuilder;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.IntegrationFlow;

import java.util.List;

@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(value = "bootiful.batch.chunk.slave", havingValue = "true")
class SlaveCustomerRemoteChunk {

    ItemProcessor<String, Customer> itemProcessor(ObjectMapper objectMapper) {
        return item -> {
            System.out.println(item);
            Customer customer = objectMapper.readValue(item, Customer.class);
            return customer;
        };
    }

    public void itemWriter(Chunk<Customer> chunk) {
        System.out.println("doing the long-running writing thing");
        List<Customer> items = chunk.getItems();
        for (Customer customer : items) {
            System.out.println("itemWriter => " + customer);
        }
    }

    @Bean
    public IntegrationFlow customerWorkerFlow(
            RemoteChunkingWorkerBuilder workerBuilder,
            @Qualifier("slaveInboundCustomerRequest") DirectChannel inbound,
            @Qualifier("slaveOutboundCustomerReply") DirectChannel outbound,
            ObjectMapper objectMapper
    ) {
        return workerBuilder
                .itemProcessor(itemProcessor(objectMapper))
                .itemWriter(this::itemWriter)
                .inputChannel(inbound) // requests received from the manager
                .outputChannel(outbound) // replies sent to the manager
                .build();
    }

}
