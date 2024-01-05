package com.example.batchprocessing.slave.integration;

import com.example.batchprocessing.slave.structure.Email;
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

@Configuration
@ConditionalOnProperty(value = "bootiful.batch.chunk.slave", havingValue = "true")
class SlaveEmailRemoteChunk {

    ItemProcessor<String, Email> itemProcessor(ObjectMapper objectMapper) {
        return item -> {
            System.out.println(item);
            Email email = objectMapper.readValue(item, Email.class);
            return email;
        };
    }

    public void itemWriter(Chunk<Email> chunk) {
        System.out.println("doing the long-running writing thing");
        List<Email> items = chunk.getItems();
        for (Email email : items) {
            System.out.println("itemWriter => " + email);
        }
    }

    @Bean
    public IntegrationFlow emailWorkerFlow(
            RemoteChunkingWorkerBuilder workerBuilder,
            @Qualifier("slaveInboundEmailRequest") DirectChannel inbound,
            @Qualifier("slaveOutboundEmailReply") DirectChannel outbound,
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
