package com.example.batchprocessing.master.configuration.schedule;

import com.example.batchprocessing.master.configuration.properties.Properties;
import org.springframework.batch.core.*;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.launch.NoSuchJobException;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.util.CollectionUtils;

import java.text.MessageFormat;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.List;
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
            @Qualifier("taskExecutorJobLauncher") JobLauncher taskExecutorJobLauncher,
            JobOperator jobOperator,
            @Qualifier("jobSampleOne") Job jobSampleOne,
            JobExplorer jobExplorer,
            JobRepository jobRepository
    ) {

        Runnable task = () -> {
            JobParameters jobParameters = new JobParametersBuilder()
                    .addString("uuid", UUID.randomUUID().toString())
                    .addDate("date", new Date())
                    .toJobParameters();
            //runJobLauncher(taskExecutorJobLauncher, jobOperator, jobSampleOne, jobParameters);
            restart(taskExecutorJobLauncher, jobOperator, jobSampleOne, jobParameters, jobExplorer, jobRepository);
        };
        taskScheduler.scheduleWithFixedDelay(task, Duration.ofSeconds(Properties.getBatchJobLauncherScheduleWithFixedDelay()));
        return null;
    }


    @Bean
    public String scheduleJobTwo(
            @Qualifier("taskScheduler") TaskScheduler taskScheduler,
            @Qualifier("taskExecutorJobLauncher") JobLauncher taskExecutorJobLauncher,
            JobOperator jobOperator,
            @Qualifier("jobSampleTwo") Job jobSampleTwo,
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
            //runJobLauncher(taskExecutorJobLauncher, jobOperator, jobSampleTwo, jobParameters);
            restart(taskExecutorJobLauncher, jobOperator, jobSampleTwo, jobParameters, jobExplorer, jobRepository);
        };
        taskScheduler.scheduleWithFixedDelay(task, Duration.ofSeconds(Properties.getBatchJobLauncherScheduleWithFixedDelay()));
        return null;
    }

    public void restart(
            JobLauncher jobLauncher,
            JobOperator jobOperator,
            Job job,
            JobParameters jobParameters,
            JobExplorer jobExplorer,
            JobRepository jobRepository
    ) {

        boolean isLaunch = runJobLauncher(jobLauncher, jobOperator, job, jobParameters);
        if (isLaunch) {
            return;
        }
        try {
            List<JobInstance> jobInstances = jobExplorer.getJobInstances(job.getName(), 0, 1);// this will get one latest job from the database
            if (!CollectionUtils.isEmpty(jobInstances)) {
                JobInstance jobInstance = jobInstances.get(0);
                List<JobExecution> jobExecutions = jobExplorer.getJobExecutions(jobInstance);
                if (!CollectionUtils.isEmpty(jobExecutions)) {
                    for (JobExecution execution : jobExecutions) {
                        // If the job status is STARTED then update the status to FAILED and restart the job using JobOperator.java
                        if (execution.getStatus().equals(BatchStatus.STARTED)) {
                            execution.setEndTime(LocalDateTime.now());
                            execution.setStatus(BatchStatus.FAILED);
                            execution.setExitStatus(ExitStatus.FAILED);
                            jobRepository.update(execution);
                            jobOperator.restart(execution.getId());
                        }
                    }
                }
            }
        } catch (Exception e1) {
            e1.printStackTrace();
        }
    }


    public static boolean runJobLauncher(
            JobLauncher jobLauncher,
            JobOperator jobOperator,
            Job job,
            JobParameters jobParameters
    ) {
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
            executions = jobOperator.getRunningExecutions(job.getName());

        } catch (NoSuchJobException e) {
            throw new RuntimeException(e);
        }
        if (executions.size() == 0) {
            JobExecution run = null;
            try {
                run = jobLauncher.run(job, jobParameters);
                JobInstance jobInstance = run.getJobInstance();
                System.out.println(MessageFormat.format("jobName: {0} , instanceId: {1}", jobInstance.getJobName(), jobInstance.getInstanceId()));
            } catch (JobExecutionAlreadyRunningException | JobRestartException |
                     JobInstanceAlreadyCompleteException | JobParametersInvalidException e) {
                System.out.println(e.getMessage());
            }
            return true;
        }
        return false;
    }


}
