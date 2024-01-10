package com.example.batchprocessing.master.configuration.batch;

import org.springframework.batch.core.*;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.launch.NoSuchJobException;
import org.springframework.batch.core.launch.support.TaskExecutorJobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.CollectionUtils;

import java.text.MessageFormat;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;

@Configuration
public class JobLauncherConfig {

    @Bean
    @Qualifier("taskExecutorJobLauncher")
    public JobLauncher taskExecutorJobLauncher(JobRepository jobRepository) throws Exception {
        TaskExecutorJobLauncher jobLauncher = new TaskExecutorJobLauncher();
        jobLauncher.setJobRepository(jobRepository);
        jobLauncher.afterPropertiesSet();
        return jobLauncher;
    }

    public static void restart(
            JobLauncher jobLauncher,
            Job job,
            JobOperator jobOperator,
            JobParameters jobParameters,
            JobExplorer jobExplorer,
            JobRepository jobRepository
    ) {

        boolean isLaunch = runJobLauncher(jobLauncher, job, jobOperator, jobParameters);
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
            Job job,
            JobOperator jobOperator,
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
