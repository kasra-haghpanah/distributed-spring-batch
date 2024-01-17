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

    public static void runJobLauncher(
            JobLauncher jobLauncher,
            Job job,
            JobOperator jobOperator,
            JobParameters jobParameters,
            JobExplorer jobExplorer,
            JobRepository jobRepository
    ) {
        //Set<Long> longSet = jobOperator.getRunningExecutions(job.getName());
        Set<JobExecution> jobExecutionSet = jobExplorer.findRunningJobExecutions(job.getName());

        if (jobExecutionSet.size() == 0) {
            JobExecution jobExecution = run(jobLauncher, job, jobParameters);
            System.out.println(MessageFormat.format("jobName: {0} , instanceId: {1} , executionId: {2}", jobExecution.getJobInstance().getJobName(), jobExecution.getJobInstance().getInstanceId(), jobExecution.getJobId()));
            return;
        }

        for (JobExecution jobExecution : jobExecutionSet) {
            ExitStatus exitStatus = jobExecution.getExitStatus();
            BatchStatus batchStatus = jobExecution.getStatus();
            LocalDateTime endtime = jobExecution.getEndTime();
            if (!batchStatus.equals(BatchStatus.COMPLETED)) {
                if (
                        (!jobExecution.isRunning() && (batchStatus.equals(BatchStatus.FAILED) || batchStatus.equals(BatchStatus.UNKNOWN)))
                                ||
                                (jobExecution.isRunning() && jobExecution.getStartTime().equals(jobExecution.getLastUpdated()) && batchStatus.equals(BatchStatus.STARTED) && exitStatus.equals(ExitStatus.UNKNOWN))// when the application is stopped
                ) {
                    jobExecution.setLastUpdated(LocalDateTime.now());
                    jobExecution.setStatus(BatchStatus.FAILED);
                    jobExecution.setExitStatus(ExitStatus.FAILED);
                    jobExecution.upgradeStatus(BatchStatus.FAILED);
                    jobExecution.setEndTime(LocalDateTime.now());
                    jobRepository.update(jobExecution);
                    restart(jobOperator, jobExecution);
                }
            }
        }

    }

    public static Long restart(JobOperator jobOperator, JobExecution jobExecution) {
        try {
            return jobOperator.restart(jobExecution.getId());
        } catch (JobInstanceAlreadyCompleteException e) {
            throw new RuntimeException(e);
        } catch (NoSuchJobExecutionException e) {
            throw new RuntimeException(e);
        } catch (NoSuchJobException e) {
            throw new RuntimeException(e);
        } catch (JobRestartException e) {
            throw new RuntimeException(e);
        } catch (JobParametersInvalidException e) {
            throw new RuntimeException(e);
        }
    }

    public static JobExecution run(JobLauncher jobLauncher, Job job, JobParameters jobParameters) {
        try {
            return jobLauncher.run(job, jobParameters);
        } catch (JobExecutionAlreadyRunningException e) {
            throw new RuntimeException(e);
        } catch (JobRestartException e) {
            throw new RuntimeException(e);
        } catch (JobInstanceAlreadyCompleteException e) {
            throw new RuntimeException(e);
        } catch (JobParametersInvalidException e) {
            throw new RuntimeException(e);
        }
    }


}
