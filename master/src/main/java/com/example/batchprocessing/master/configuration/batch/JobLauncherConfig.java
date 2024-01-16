package com.example.batchprocessing.master.configuration.batch;

import org.springframework.batch.core.*;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.launch.NoSuchJobException;
import org.springframework.batch.core.launch.NoSuchJobExecutionException;
import org.springframework.batch.core.launch.support.TaskExecutorJobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

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

    public static boolean runJobLauncher(
            JobLauncher jobLauncher,
            Job job,
            JobOperator jobOperator,
            JobParameters jobParameters,
            JobExplorer jobExplorer,
            JobRepository jobRepository
    ) {
        Set<Long> executions = null;
        try {
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
        } else {
            Long executionId = executions.iterator().next();
            JobInstance jobInstance = jobExplorer.getJobInstance(executionId);
            List<JobExecution> jobExecutions = jobExplorer.getJobExecutions(jobInstance);

            for (JobExecution jobExecution : jobExecutions) {

                if (
                        (!jobExecution.isRunning() && jobExecution.getStatus().equals(BatchStatus.STARTED))
                                ||
                                jobExecution.getStatus().equals(BatchStatus.UNKNOWN)
                                ||
                                jobExecution.getExitStatus().equals(ExitStatus.UNKNOWN)
                ) {
                    JobParameters parameters = jobExecution.getJobParameters();

                    jobExecution.setEndTime(LocalDateTime.now());
                    jobExecution.setStatus(BatchStatus.FAILED);
                    jobExecution.setExitStatus(ExitStatus.FAILED);
                    jobRepository.update(jobExecution);

                    try {
                        jobOperator.restart(jobExecution.getId());
                    } catch (JobInstanceAlreadyCompleteException |
                             NoSuchJobExecutionException |
                             NoSuchJobException |
                             JobRestartException |
                             JobParametersInvalidException e) {
                        throw new RuntimeException(e);
                    }
                }

            }

        }
        return false;
    }


}
