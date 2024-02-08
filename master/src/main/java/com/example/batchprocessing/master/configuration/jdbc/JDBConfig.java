package com.example.batchprocessing.master.configuration.jdbc;

import com.example.batchprocessing.master.configuration.properties.Properties;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.batch.BatchDataSource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.retry.annotation.Retryable;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.net.ConnectException;

@DependsOn({"properties"})
@Configuration(proxyBeanMethods = false)
public class JDBConfig {


    @Primary
    @Bean
    @Qualifier("dataSourceOne")
    @BatchDataSource
    @Retryable({ConnectException.class})
    DataSource dataSourceOne() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(Properties.getDatasourceOneUrl());
        config.setDriverClassName(Properties.getDatasourceOneDriverClassName());
        config.setUsername(Properties.getDatasourceOneUsername());
        config.setPassword(Properties.getDatasourceOnePassword());
        config.setMaximumPoolSize(Properties.getDatasourceOneMaximumPoolSize());
        config.setMaxLifetime(Properties.getDatasourceOneMaximumLifeTime());
        return new HikariDataSource(config);
    }

    @Primary
    @Bean
    @Qualifier("batchJdbcTemplate")
    JdbcTemplate batchJdbcTemplate(@Qualifier("dataSourceOne") DataSource dataSourceOne) {
        return new JdbcTemplate(dataSourceOne);
    }

    @Primary
    @Bean
    @Qualifier("batchTM")
    PlatformTransactionManager batchTM(@Qualifier("dataSourceOne") DataSource dataSourceOne) {
        return new JdbcTransactionManager(dataSourceOne);
    }

    @Primary
    @Bean
    @Qualifier("batchTT")
    TransactionTemplate batchTT(@Qualifier("batchTM") PlatformTransactionManager batchTM) {
        return new TransactionTemplate(batchTM);
    }

    @Bean
    @Qualifier("dataSourceTwo")
    @Retryable({ConnectException.class})
    DataSource dataSourceTwo() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(Properties.getDatasourceTwoUrl());
        config.setDriverClassName(Properties.getDatasourceTwoDriverClassName());
        config.setUsername(Properties.getDatasourceTwoUsername());
        config.setPassword(Properties.getDatasourceTwoPassword());
        config.setMaximumPoolSize(Properties.getDatasourceTwoMaximumPoolSize());
        config.setMaxLifetime(Properties.getDatasourceTwoMaximumLifeTime());
        return new HikariDataSource(config);
    }

    @Bean
    @Qualifier("batchDestinationJdbcTemplate")
    JdbcTemplate batchDestinationJdbcTemplate(@Qualifier("dataSourceTwo") DataSource dataSourceTwo) {
        return new JdbcTemplate(dataSourceTwo);
    }

    @Bean
    @Qualifier("batchDestinationTM")
    PlatformTransactionManager batchDestinationTM(@Qualifier("dataSourceTwo") DataSource dataSourceTwo) {
        return new JdbcTransactionManager(dataSourceTwo);
    }

    @Bean
    @Qualifier("batchDestinationTT")
    TransactionTemplate batchDestinationTT(@Qualifier("batchDestinationTM") PlatformTransactionManager batchDestinationTM) {
        return new TransactionTemplate(batchDestinationTM);
    }


}
