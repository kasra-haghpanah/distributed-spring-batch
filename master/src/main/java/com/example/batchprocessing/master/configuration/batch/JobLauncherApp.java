package com.example.batchprocessing.master.configuration.batch;

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
import org.springframework.context.annotation.Primary;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.Date;
import java.util.UUID;

@Configuration
public class JobLauncherApp {

    private final JdbcTemplate jdbcTemplate;
    private final JobRepository jobRepository;
    ///private final JobLauncher jobLauncher;

    public JobLauncherApp(
            @Qualifier("batchDestinationJdbcTemplate") JdbcTemplate jdbcTemplate,
            JobRepository jobRepository
    ) {
        this.jdbcTemplate = jdbcTemplate;
        this.jobRepository = jobRepository;
    }


    @Bean
    @Primary
    @Qualifier("taskExecutor")
    TaskExecutor taskExecutor() {
        return new SimpleAsyncTaskExecutor();
    }

    // for running multiple jobs as async
    @Bean
    @Qualifier("jobLauncherTwo")
    public JobLauncher jobLauncherTwo(@Qualifier("taskExecutor") TaskExecutor taskExecutor) throws Exception {
        TaskExecutorJobLauncher jobLauncher = new TaskExecutorJobLauncher();
        jobLauncher.setJobRepository(jobRepository);
        jobLauncher.setTaskExecutor(taskExecutor);
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
