package com.example.batchprocessing.master.configuration.schedule;

import com.example.batchprocessing.master.configuration.properties.Properties;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import java.text.MessageFormat;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static com.example.batchprocessing.master.configuration.batch.JobLauncherConfig.runJobLauncher;

@Configuration
@DependsOn("properties")
@ConditionalOnProperty(value = "enable.schedule", havingValue = "true")
public class SchedulerConfig {

    static final AtomicInteger atomicInteger = new AtomicInteger(0);

    @Bean
    @Qualifier("taskScheduler")
    public TaskScheduler taskScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(10);
        scheduler.setThreadNamePrefix("MyTaskScheduler-");
        scheduler.setWaitForTasksToCompleteOnShutdown(true);
        return scheduler;
    }

    //@Bean
    public CommandLineRunner scheduleTask(@Qualifier("taskScheduler") TaskScheduler taskScheduler) {

        Runnable task = () -> {
            int counter = atomicInteger.incrementAndGet();
            LocalDateTime localDateTime = LocalDateTime.now(ZoneId.of("Asia/Tehran"));
            String date = localDateTime.format(DateTimeFormatter.ofPattern("yyyy:MM:dd hh:mm:ss"));
            String text = MessageFormat.format("{0}- Programmatically scheduled task performed at {1}", counter, date);
            System.out.println(text);
        };
        taskScheduler.scheduleWithFixedDelay(task, Duration.ofSeconds(5));
        return null;
    }

    @Bean
    public CommandLineRunner scheduleJobOne(
            @Qualifier("taskScheduler") TaskScheduler taskScheduler,
            @Qualifier("taskExecutorJobLauncher") JobLauncher taskExecutorJobLauncher,
            @Qualifier("jobSampleOne") Job jobSampleOne,
            JobOperator jobOperator,
            JobExplorer jobExplorer,
            JobRepository jobRepository
    ) {

        Runnable task = () -> {
            JobParameters jobParameters = new JobParametersBuilder()
                    .addString("uuid", UUID.randomUUID().toString())
                    .addDate("date", new Date())
                    .toJobParameters();
            runJobLauncher(taskExecutorJobLauncher, jobSampleOne, jobOperator, jobParameters, jobExplorer, jobRepository);
        };
        taskScheduler.scheduleWithFixedDelay(task, Duration.ofSeconds(Properties.getBatchJobLauncherScheduleWithFixedDelay()));
        return null;
    }


    @Bean
    public CommandLineRunner scheduleJobTwo(
            @Qualifier("taskScheduler") TaskScheduler taskScheduler,
            @Qualifier("taskExecutorJobLauncher") JobLauncher taskExecutorJobLauncher,
            @Qualifier("jobSampleTwo") Job jobSampleTwo,
            JobOperator jobOperator,
            JobExplorer jobExplorer,
            JobRepository jobRepository

    ) {


        Runnable task = () -> {
            JobParameters jobParameters = new JobParametersBuilder()
                    .addString("uuid", UUID.randomUUID().toString())
                    .addDate("date", new Date())
                    .addString("inputFile", "classpath:data/email.json")
                    .addString("outputFile", "classpath:data/email-out.json")
                    .toJobParameters();

            runJobLauncher(taskExecutorJobLauncher, jobSampleTwo, jobOperator, jobParameters, jobExplorer, jobRepository);
        };
        taskScheduler.scheduleWithFixedDelay(task, Duration.ofSeconds(Properties.getBatchJobLauncherScheduleWithFixedDelay()));
        return null;
    }

}
