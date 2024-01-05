package com.example.batchprocessing.master.configuration.batch;

import com.example.batchprocessing.master.configuration.properties.Properties;
import com.example.batchprocessing.master.configuration.structure.Email;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.partition.support.SimplePartitioner;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.integration.chunk.RemoteChunkingManagerStepBuilderFactory;
import org.springframework.batch.integration.partition.RemotePartitioningManagerStepBuilderFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.json.JacksonJsonObjectReader;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.batch.item.json.builder.JsonItemReaderBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import java.text.MessageFormat;
import java.util.Map;
import java.util.Objects;

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
            @Qualifier("remoteEmailStep") Step remoteEmailStep,
            @Qualifier("managerRemotePartitionerStep") Step managerRemotePartitionerStep
    ) {
        return new JobBuilder("jobSampleTwo", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(remoteEmailStep)// remoteChunk
                .next(managerRemotePartitionerStep)// remotePartitioningChunk
                .build();
    }


    @Bean
    @Qualifier("jsonItemReader")
    @StepScope
    public JsonItemReader<Email> jsonItemReader(
            @Value("#{jobParameters[inputFile]}") Resource resource,
            ObjectMapper objectMapper            //@Qualifier("emailJsonObjectReader") JacksonJsonObjectReader<Email> jacksonJsonObjectReader
            //@Value("classpath:data/email.json") Resource resource
    ) {
        JacksonJsonObjectReader<Email> reader = new JacksonJsonObjectReader<>(Email.class);
        reader.setMapper(objectMapper);

        return new JsonItemReaderBuilder<Email>()
                .name("emailsJsonItemReader")
                .resource(resource)
                .saveState(true)
                //.jsonObjectReader(new GsonJsonObjectReader<>(Trade.class))
                .jsonObjectReader(reader)
                .build();
    }

    public Email itemProcessor(Email email) {
        return email;
    }

/*    @Bean
    @Qualifier("jsonItemWriter")
    @StepScope
    public JsonFileItemWriter<Email> jsonItemWriter(
            @Value("#{jobParameters[outputFile]}") FileSystemResource resource,
            ObjectMapper objectMapper
    ) {

        JacksonJsonObjectMarshaller<Email> marshaller = new JacksonJsonObjectMarshaller<Email>();
        marshaller.setObjectMapper(objectMapper);

        return new JsonFileItemWriterBuilder<Email>().resource(resource)
                .lineSeparator("\n")
                .jsonObjectMarshaller(marshaller)
                .name("emailsJsonFileItemWriter")
                .shouldDeleteIfExists(true)
                .build();
    }*/

    @Bean
    public ItemWriter<Email> itemWriter() {
        return items -> {
            for (Email item : items) {
                System.out.println("item = " + item);
            }
        };
    }

    @Bean // remoteChunk
    @Qualifier("remoteEmailStep")
    public TaskletStep remoteEmailStep(
            RemoteChunkingManagerStepBuilderFactory remoteChunkingManagerStepBuilderFactory,
            @Qualifier("jsonItemReader") ItemReader<Email> itemReader,
            @Qualifier("masterOutboundEmail") DirectChannel outbound,
            @Qualifier("masterInboundEmail") QueueChannel inbound,
            PlatformTransactionManager platformTransactionManager,
            ObjectMapper objectMapper
    ) {

        return remoteChunkingManagerStepBuilderFactory.get("remoteEmailStep")
                .<Email, String>chunk(2)
                .reader(itemReader)
                .processor((email) -> {
                    return objectMapper.writeValueAsString(email);
                })
                .outputChannel(outbound) // requests sent to workers
                .inputChannel(inbound)   // replies received from workers
                .transactionManager(platformTransactionManager)
                .listener(new StepExecutionListener() {
                    @Override
                    public void beforeStep(StepExecution stepExecution) {
                        System.out.println(MessageFormat.format("emailStep is {0}.", stepExecution.getStepName()));
                    }

                    @Override
                    public ExitStatus afterStep(StepExecution stepExecution) {
                        ExitStatus status = ExitStatus.COMPLETED;
                        System.out.println("the status is " + status);
                        return status;
                    }
                })
                .build();
    }

/*    @Bean
    @Qualifier("jsonStep")
    public Step jsonStep(
            @Qualifier("jsonItemReader") ItemReader<Email> itemReader,
            @Qualifier("jsonItemWriter") ItemWriter<Email> itemWriter,
            JobRepository repository,
            PlatformTransactionManager transactionManager
    ) {
        return new StepBuilder("jsonStep", repository)
                .<Email, Email>chunk(2, transactionManager)
                .reader(itemReader)
                .processor(this::itemProcessor)
                //.writer(itemWriter)
                .writer(itemWriter)
                .faultTolerant()
                .skip(IllegalArgumentException.class)
                .skipLimit(3)
                .listener(new StepExecutionListener() {
                    @Override
                    public void beforeStep(StepExecution stepExecution) {
                        System.out.println(MessageFormat.format("jsonStep is {0}.", stepExecution.getStepName()));
                    }

                    @Override
                    public ExitStatus afterStep(StepExecution stepExecution) {
                        //Integer count = Objects.requireNonNull(jdbcTemplate.queryForObject("SELECT coalesce(count(*) ,0) FROM video_game_sales", Integer.class));
                        //ExitStatus status = count == 0 ? new ExitStatus(EMPTY_CSV_STATUS) : ExitStatus.COMPLETED;
                        ExitStatus status = ExitStatus.COMPLETED;
                        System.out.println("the status is " + status);
                        return status;
                    }
                })
                .build();

    }*/


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
                        ExitStatus status = count == 0 ? new ExitStatus(Properties.EMPTY_CSV_STATUS) : ExitStatus.COMPLETED;
                        System.out.println("the status is " + status);
                        return status;
                    }
                })
                .build();
    }


}
