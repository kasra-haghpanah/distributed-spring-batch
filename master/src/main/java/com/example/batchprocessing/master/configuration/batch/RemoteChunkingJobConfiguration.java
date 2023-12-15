package com.example.batchprocessing.master.configuration.batch;

import com.example.batchprocessing.master.configuration.structure.Customer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.integration.chunk.RemoteChunkingManagerStepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.transaction.PlatformTransactionManager;

//@EnableBatchIntegration
//@EnableBatchProcessing
@Configuration
public class RemoteChunkingJobConfiguration {

    @Bean
    @Qualifier("chunkingManagerStepBuilderFactory")
    RemoteChunkingManagerStepBuilderFactory chunkingMasterStepBuilderFactory(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new RemoteChunkingManagerStepBuilderFactory(jobRepository, transactionManager);
    }

    @Bean
    @Qualifier("managerStep")
    public TaskletStep managerStep(
            @Qualifier("masterInboundChunkChannel") QueueChannel inbound,
            @Qualifier("masterOutboundChunkChannel") DirectChannel outbound,
            @Qualifier("chunkingManagerStepBuilderFactory") RemoteChunkingManagerStepBuilderFactory managerStepBuilderFactory,
            @Qualifier("masterItemReader") ListItemReader<Customer> itemReader,
            @Qualifier("masterItemProcessor") ItemProcessor<Object, Object> itemProcessor
    ) {
        return managerStepBuilderFactory.get("managerStep")
                .chunk(3)
                .reader(itemReader)
                .processor(itemProcessor)
                .outputChannel(outbound) // requests sent to workers
                .inputChannel(inbound)   // replies received from workers
                .build();
    }

    // Middleware beans setup omitted


}
