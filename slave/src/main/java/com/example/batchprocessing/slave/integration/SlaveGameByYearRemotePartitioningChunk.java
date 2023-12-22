package com.example.batchprocessing.slave.integration;

import com.example.batchprocessing.slave.structure.GameByYear;
import com.example.batchprocessing.slave.structure.YearPlatformSales;
import com.example.batchprocessing.slave.structure.YearReport;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.integration.partition.RemotePartitioningWorkerStepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class SlaveGameByYearRemotePartitioningChunk {


    public GameByYear rowMapper(ResultSet rs, int rowNum) throws SQLException {

        int rank = rs.getInt("rank");
        String name = rs.getString("name");
        String platform = rs.getString("platform");
        Integer year = rs.getInt("year");
        String genre = rs.getString("genre");
        String publisher = rs.getString("publisher");
        float na = rs.getFloat("na_sales");
        float eu = rs.getFloat("eu_sales");
        float jp = rs.getFloat("jp_sales");
        float other = rs.getFloat("other_sales");
        float global = rs.getFloat("global_sales");
        return new GameByYear(rank, name, platform, year, genre, publisher, na, eu, jp, other, global);
    }

    @Bean
    @StepScope
    @Qualifier("gameByYearItemReader")
    public ItemReader<GameByYear> gameByYearItemReader(@Qualifier("dataSourceTwo") DataSource dataSource) {

        Map<String, Object> parameterValues = new HashMap<>();
        parameterValues.put("statusCode", 10);

        return new JdbcPagingItemReaderBuilder<GameByYear>().name("customerReader")
                .dataSource(dataSource)
                .selectClause("SELECT rank, name, platform, year, genre, publisher, na_sales, eu_sales, jp_sales, other_sales, global_sales")
                .fromClause("FROM video_game_sales")
                .whereClause("WHERE year %10 != :statusCode")
                .sortKeys(Map.of("year", Order.ASCENDING))
                .rowMapper(this::rowMapper)
                .pageSize(100)
                .parameterValues(parameterValues)
                .build();
    }

    @Bean
    @Qualifier("gameByYearItemProcessor")
    public ItemProcessor<GameByYear, GameByYear> gameByYearItemProcessor() {
        return (gameByYear) -> {
            return gameByYear;
        };
    }

    @Bean
    @Qualifier("gameByYearItemWriter")
    JdbcBatchItemWriter<GameByYear> gameByYearItemWriter(@Qualifier("dataSourceTwo") DataSource dataSource) {
        // for upsert
        String sql = """
                REPLACE INTO `remote_video_game_sales` (`rank`, `name`, `platform`, `year`, `genre`, `publisher`, `na_sales`, `eu_sales`, `jp_sales`, `other_sales`, `global_sales`)
                VALUES (:rank, :name, :platform, :year, :genre, :publisher, :na, :eu, :jp, :other, :global)
                """;
        return new JdbcBatchItemWriterBuilder<GameByYear>()
                .sql(sql)
                .dataSource(dataSource)
                .itemSqlParameterSourceProvider((gameByYear) -> {
                    Map<String, Object> map = new HashMap<String, Object>();
                    map.putAll(Map.of(
                            "rank", gameByYear.rank(),
                            "name", gameByYear.name(),
                            "platform", gameByYear.platform(),
                            "year", gameByYear.year(),
                            "genre", gameByYear.genre(),
                            "publisher", gameByYear.publisher()
                    ));

                    map.putAll(Map.of(
                            "na", gameByYear.na(),
                            "eu", gameByYear.eu(),
                            "jp", gameByYear.jp(),
                            "other", gameByYear.other(),
                            "global", gameByYear.global()
                    ));

                    return new MapSqlParameterSource(map);
                })
                .build();
    }

    @Bean
    @Qualifier("workerRemotePartitioningStep")
    public Step workerRemotePartitioningStep(
            RemotePartitioningWorkerStepBuilderFactory workerStepBuilderFactory,
            @Qualifier("slaveInboundGameByYearRequest") DirectChannel inbound,
            @Qualifier("slaveOutboundGameByYearReply") DirectChannel outbound,
            @Qualifier("gameByYearItemReader") ItemReader itemReader,
            @Qualifier("gameByYearItemProcessor") ItemProcessor itemProcessor,
            @Qualifier("gameByYearItemWriter") ItemWriter itemWriter,
            PlatformTransactionManager transactionManager

    ) {
        return workerStepBuilderFactory
                .get("workerRemotePartitioningStep")
                .inputChannel(inbound)
                .outputChannel(outbound)
                .<GameByYear, GameByYear>chunk(100, transactionManager)
                .reader(itemReader)
                .processor(itemProcessor)
                .writer(itemWriter)
                .faultTolerant()
                .retry(Exception.class)
                .retryLimit(3)
                .build();
    }

}
