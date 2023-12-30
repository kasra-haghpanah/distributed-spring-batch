package com.example.batchprocessing.master.configuration.batch;

import org.springframework.batch.core.*;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.core.partition.support.SimplePartitioner;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.integration.partition.RemotePartitioningManagerStepBuilderFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.jdbc.core.JdbcTemplate;

import java.text.MessageFormat;
import java.util.*;

@Configuration
public class JobTwo {

    public class BasicPartitioner extends SimplePartitioner {

        private static final String PARTITION_KEY = "partition";

        @Override
        public Map<String, ExecutionContext> partition(int gridSize) {
            Map<String, ExecutionContext> partitions = super.partition(gridSize);
            int i = 0;
            for (ExecutionContext context : partitions.values()) {
                context.put(PARTITION_KEY, PARTITION_KEY + (i++));
            }
            return partitions;
        }

    }

    @Bean
    @Qualifier("jobSampleTwo")
    public Job jobSampleTwo(
            JobRepository jobRepository,
            @Qualifier("managerRemotePartitionerStep") Step step
    ) {
        return new JobBuilder("jobSampleTwo", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(step)
                .build();
    }


    @Bean
    @Qualifier("managerRemotePartitionerStep")
    public Step managerRemotePartitionerStep(
            RemotePartitioningManagerStepBuilderFactory stepBuilderFactory,
            @Qualifier("masterInboundGameByYear") QueueChannel inbound,
            @Qualifier("masterOutboundGameByYear") DirectChannel outbound,
            @Qualifier("batchDestinationJdbcTemplate") JdbcTemplate jdbcTemplate

    ) {
        return stepBuilderFactory
                .get("managerRemotePartitionerStep")
                .partitioner("workerRemotePartitioningStep", new BasicPartitioner())
                .gridSize(10)
                .outputChannel(outbound)
                .inputChannel(inbound)
                .listener(new StepExecutionListener() {
                    @Override
                    public void beforeStep(StepExecution stepExecution) {
                        System.out.println(MessageFormat.format("managerRemotePartitionerStep is {0}.", stepExecution.getStepName()));
                    }

                    @Override
                    public ExitStatus afterStep(StepExecution stepExecution) {
                        Integer count = Objects.requireNonNull(jdbcTemplate.queryForObject("SELECT coalesce(count(*) ,0) FROM remote_video_game_sales", Integer.class));
                        ExitStatus status = count == 0 ? new ExitStatus(JobOne.EMPTY_CSV_STATUS) : ExitStatus.COMPLETED;
                        System.out.println("the status is " + status);
                        return status;
                    }
                })
                .build();
    }


}
