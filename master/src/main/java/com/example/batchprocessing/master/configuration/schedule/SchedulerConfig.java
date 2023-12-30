package com.example.batchprocessing.master.configuration.schedule;

import com.example.batchprocessing.master.configuration.properties.Properties;
import org.springframework.batch.core.*;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.launch.NoSuchJobException;
import org.springframework.batch.core.launch.NoSuchJobExecutionException;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Qualifier;
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
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

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
    public String scheduleTask(@Qualifier("taskScheduler") TaskScheduler taskScheduler) {

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
    public String scheduleJobOne(
            @Qualifier("taskScheduler") TaskScheduler taskScheduler,
            @Qualifier("jobSampleOne") Job jobSampleOne,
            JobLauncher jobLauncher,
            JobOperator jobOperator
    ) {


        Runnable task = () -> {
            Set<Long> executions = null;
            try {
/*
                registry.destroySingleton("dataSourceOne");
                registry.registerSingleton("dataSourceOne", createDataSource());

                Set<Long> executions = jobOperator.getRunningExecutions("jobSampleOne");
                jobOperator.startNextInstance("jobSampleOne");
                List<Long> executions = jobOperator.getJobInstances("jobSampleOne", 0, 1);
                jobOperator.restart(executions.get(0));
                jobOperator.stop(executions.iterator().next());
                jobOperator.abandon(executions.iterator().next());
                */
                executions = jobOperator.getRunningExecutions(jobSampleOne.getName());
            } catch (NoSuchJobException e) {
                throw new RuntimeException(e);
            }
            if (executions.size() == 0) {
                JobExecution run1 = null;
                try {
                    run1 = jobLauncher.run(jobSampleOne, new JobParametersBuilder().addString("uuid", UUID.randomUUID().toString()).addDate("date", new Date()).toJobParameters());
                    JobInstance jobInstance1 = run1.getJobInstance();
                    System.out.println("instanceId1: " + jobInstance1.getInstanceId());
                } catch (JobExecutionAlreadyRunningException | JobRestartException |
                         JobInstanceAlreadyCompleteException | JobParametersInvalidException e) {
                    throw new RuntimeException(e);
                }

            }
        };
        taskScheduler.scheduleWithFixedDelay(task, Duration.ofSeconds(Properties.getBatchJobLauncherScheduleWithFixedDelay()));
        return null;
    }


}
