package com.example.batchprocessing.slave.integration;

import com.example.batchprocessing.slave.structure.YearReport;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.batch.integration.chunk.RemoteChunkingWorkerBuilder;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.IntegrationFlow;

import java.util.List;

@Configuration
public class SlaveYearReportRemoteChunk {

    ItemProcessor<String, YearReport> itemProcessor(ObjectMapper objectMapper) {
        return item -> {
            System.out.println(item);
            YearReport yearReport = objectMapper.readValue(item, YearReport.class);
            return yearReport;
        };
    }

    public void itemWriter(Chunk<YearReport> chunk) {
        System.out.println("doing the long-running writing thing");
        List<YearReport> yearReports = chunk.getItems();
        for (YearReport yearReport : yearReports) {
            System.out.println("itemWriter => " + yearReport);
        }
    }

    @Bean
    public IntegrationFlow yearReportWorkerFlow(
            RemoteChunkingWorkerBuilder workerBuilder,
            @Qualifier("slaveInboundYearReportRequest") DirectChannel inbound,
            @Qualifier("slaveOutboundYearReportReply") DirectChannel outbound,
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
