package com.example.batchprocessing.slave.integration;

import com.example.batchprocessing.slave.structure.YearPlatformSales;
import com.example.batchprocessing.slave.structure.YearReport;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.batch.integration.chunk.RemoteChunkingWorkerBuilder;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.retry.annotation.Retryable;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    @Qualifier("yearReportItemWriter")
    @Retryable
    JdbcBatchItemWriter<YearReport> yearReportItemWriter(@Qualifier("dataSourceTwo") DataSource dataSource) {
        // for upsert
        String sql = """
                REPLACE INTO `remote_year_platform_report` (`year`, `platform`, `sales`)
                VALUES (:year, :platform, :sales)
                """;
        return new JdbcBatchItemWriterBuilder<YearReport>()
                .sql(sql)
                .dataSource(dataSource)
                .itemSqlParameterSourceProvider((yearReport) -> {
                    Map<String, Object> map = new HashMap<String, Object>();
                    map.putAll(Map.of("year", yearReport.year()));

                    if (yearReport.breakout().iterator().hasNext()) {
                        YearPlatformSales yearPlatformSales = yearReport.breakout().iterator().next();
                        map.putAll(Map.of(
                                "platform", yearPlatformSales.platform().trim(),
                                "sales", yearPlatformSales.sales()
                        ));
                    }

                    return new MapSqlParameterSource(map);
                })
                .build();
    }

    @Bean
    public IntegrationFlow yearReportWorkerFlow(
            RemoteChunkingWorkerBuilder workerBuilder,
            @Qualifier("slaveInboundYearReportRequest") DirectChannel inbound,
            @Qualifier("slaveOutboundYearReportReply") DirectChannel outbound,
            @Qualifier("yearReportItemWriter") JdbcBatchItemWriter<YearReport> yearReportItemWriter,
            ObjectMapper objectMapper
    ) {
        return workerBuilder
                .itemProcessor(itemProcessor(objectMapper))
                .itemWriter(yearReportItemWriter)
                .inputChannel(inbound) // requests received from the manager
                .outputChannel(outbound) // replies sent to the manager
                .build();
    }

}
