package com.example.batchprocessing.master.configuration.batch;

import jakarta.annotation.PostConstruct;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.TaskExecutorJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.annotation.Primary;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.Date;
import java.util.UUID;

@Configuration
public class JobLauncherApp {

    private final JdbcTemplate jdbcTemplate;
    private final JobRepository jobRepository;
    ///private final JobLauncher jobLauncher;

    public static final String videoGameSalesTableScript = """
            CREATE TABLE IF NOT EXISTS video_game_sales
            (
                `rank`         int,
                `name`         text NOT NULL,
                `platform`     text NOT NULL,
                `year`         int,
                `genre`        text NOT NULL,
                `publisher`    text,
                `na_sales`     numeric(4,2),
                `eu_sales`     numeric(4,2),
                `jp_sales`     numeric(4,2),
                `other_sales`  numeric(4,2),
                `global_sales` numeric(4,2),
                PRIMARY KEY (`name`(100), `platform`(100), `year`, `genre`(100))
            )
            """;
    public static final String yearPlatformReportTableScript = """
            CREATE TABLE IF NOT EXISTS year_platform_report
            (
                year     int,
                platform text,
                sales    numeric(8, 2),
                unique (year, platform)
            );
            """;


    public JobLauncherApp(
            @Qualifier("batchDestinationJdbcTemplate") JdbcTemplate jdbcTemplate,
            JobRepository jobRepository
    ) {
        this.jdbcTemplate = jdbcTemplate;
        this.jobRepository = jobRepository;
    }

    // for running multiple jobs as async
    @Bean
    @Qualifier("jobLauncherTwo")
    public JobLauncher jobLauncherTwo() throws Exception {
        TaskExecutorJobLauncher jobLauncher = new TaskExecutorJobLauncher();
        jobLauncher.setJobRepository(jobRepository);
        jobLauncher.setTaskExecutor(new SimpleAsyncTaskExecutor());
        jobLauncher.afterPropertiesSet();
        return jobLauncher;
    }


    @Bean
    @Qualifier("runner")
    ApplicationRunner runner(
            @Qualifier("jobSampleOne") Job jobSampleOne,
            @Qualifier("jobLauncherTwo") JobLauncher jobLauncherTwo
    ) {
        return (applicationArguments) -> {

            JobExecution run = jobLauncherTwo.run(jobSampleOne, new JobParametersBuilder().addString("uuid", UUID.randomUUID().toString()).addDate("date", new Date()).toJobParameters());
            JobInstance jobInstance = run.getJobInstance();
            System.out.println("instanceId: " + jobInstance.getInstanceId());
        };

    }

}
