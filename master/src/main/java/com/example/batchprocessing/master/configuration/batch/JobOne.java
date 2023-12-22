package com.example.batchprocessing.master.configuration.batch;

import com.example.batchprocessing.master.configuration.listener.JobCompletedListener;
import com.example.batchprocessing.master.configuration.structure.CsvRow;
import com.example.batchprocessing.master.configuration.structure.Customer;
import com.example.batchprocessing.master.configuration.structure.YearPlatformSales;
import com.example.batchprocessing.master.configuration.structure.YearReport;
import com.example.batchprocessing.master.configuration.util.JavaUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.integration.chunk.RemoteChunkingManagerStepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.*;

// https://github.com/spring-projects/spring-batch/issues/4294
// https://github.com/coffee-software-show/lets-code-spring-batch
// SELECT * FROM batch_job_instance;
// SELECT * FROM batch_job_execution_params;
@Configuration
public class JobOne {

    public static final String EMPTY_CSV_STATUS = "EMPTY";

    @Bean
    @Qualifier("jobSampleOne")
    public Job jobSampleOne(
            JobRepository jobRepository,
            @Qualifier("stepOne") Step stepOne,
            @Qualifier("gameByYearStep") Step gameByYearStep,
            @Qualifier("errorStep") Step errorStep,
            @Qualifier("yearPlatformReportStep") Step yearPlatformReportStep,
            @Qualifier("yearReportStep") TaskletStep yearReportStep,
            @Qualifier("managerStep") TaskletStep managerStep,
            @Qualifier("managerRemotePartitionerStep") Step step,
            @Qualifier("endStep") Step endStep
    ) {
        return new JobBuilder("jobSampleOne", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(stepOne)
                .next(gameByYearStep).on(EMPTY_CSV_STATUS).to(errorStep)
                .from(gameByYearStep).on("*").to(yearPlatformReportStep)
                .next(managerStep)// remoteChunk
                .next(yearReportStep)// remoteChunk
                .next(step)// remotePartitioningChunk
                .next(endStep)
                .build()
                .build();
    }


    @Bean
    @StepScope
    @Qualifier("taskletOne")
    public Tasklet taskletOne(@Value("#{jobParameters['uuid']}") String uuid, @Value("#{jobParameters['date']}") String date) {
        return (contribution, context) -> {
            System.out.println(MessageFormat.format("taskletOne: Hello word! the UUID is {0} - date: {1}", uuid, date));
            return RepeatStatus.FINISHED;
        };
    }

    @Bean
    @Qualifier("stepOne")
    public Step stepOne(
            JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            @Qualifier("taskletOne") Tasklet taskletOne) {
        return new StepBuilder("stepOne", jobRepository)
                .tasklet(taskletOne, transactionManager)
                .build();
    }

    @Bean // JsonItemReader
    @Qualifier("gameByYearReader")
    public FlatFileItemReader<CsvRow> gameByYearReader(
            // @Value("file://C:/PC/javaProject/SpringBoot/batch-processing/src/main/resources/data/vgsales.csv")
            @Value("classpath:data/vgsales.csv") Resource resource) {

        return new FlatFileItemReaderBuilder<CsvRow>()
                .resource(resource)
                .name("gameByYearReader")
                .delimited().delimiter(",")//
                .names("rank,name,platform,year,genre,publisher,na,eu,jp,other,global".split(","))
                .linesToSkip(1)
                .fieldSetMapper(fieldSet -> { // FieldSet<CsvRow>
                    return new CsvRow(fieldSet.readInt("rank"), fieldSet.readString("name"), fieldSet.readString("platform"), JavaUtil.getInteger(fieldSet.readString("year"), 0), fieldSet.readString("genre"), fieldSet.readString("publisher"), fieldSet.readFloat("na"), fieldSet.readFloat("eu"), fieldSet.readFloat("jp"), fieldSet.readFloat("other"), fieldSet.readFloat("global"));
                }).build();
    }

    @Bean
    @Qualifier("gameByYearWriter")
    JdbcBatchItemWriter<CsvRow> gameByYearWriter(@Qualifier("dataSourceTwo") DataSource dataSource) {
        // for upsert
        String sql = """
                REPLACE INTO `video_game_sales` (`rank`, `name`, `platform`, `year`, `genre`, `publisher`, `na_sales`, `eu_sales`, `jp_sales`, `other_sales`, `global_sales`)
                VALUES (:rank, :name, :platform, :year, :genre, :publisher, :na_sales, :eu_sales, :jp_sales, :other_sales, :global_sales)
                """;
        return new JdbcBatchItemWriterBuilder<CsvRow>()
                .sql(sql)
                .dataSource(dataSource)
                .itemSqlParameterSourceProvider((csvRow) -> {
                    Map<String, Object> map = new HashMap<String, Object>();
                    map.putAll(Map.of(
                            "rank", csvRow.rank(),
                            "name", csvRow.name(),
                            "platform", csvRow.platform().trim(),
                            "year", csvRow.year(),
                            "genre", csvRow.genre().trim(),
                            "publisher", csvRow.publisher().trim()
                    ));
                    map.putAll(Map.of(
                            "na_sales", csvRow.na(),
                            "eu_sales", csvRow.eu(),
                            "jp_sales", csvRow.jp(),
                            "other_sales", csvRow.other(),
                            "global_sales", csvRow.global()
                    ));

                    return new MapSqlParameterSource(map);
                })
/*                .itemPreparedStatementSetter((csvRow, preparedStatement) -> {

                    int i = 0;
                    preparedStatement.setInt(i++, csvRow.rank());
                    preparedStatement.setString(i++, csvRow.name());
                    preparedStatement.setString(i++, csvRow.platform());
                    preparedStatement.setInt(i++, csvRow.year());
                    preparedStatement.setString(i++, csvRow.genre());
                    preparedStatement.setString(i++, csvRow.publisher());
                    preparedStatement.setFloat(i++, csvRow.na());
                    preparedStatement.setFloat(i++, csvRow.eu());
                    preparedStatement.setFloat(i++, csvRow.jp());
                    preparedStatement.setFloat(i++, csvRow.other());
                    preparedStatement.setFloat(i++, csvRow.global());
                    preparedStatement.execute();
                })*/
                .build();

    }

    @Bean
    @Qualifier("gameByYearStep")
    public Step gameByYearStep(
            JobRepository repository,
            PlatformTransactionManager transactionManager,
            @Qualifier("gameByYearReader") FlatFileItemReader<CsvRow> gameByYearReader,
            @Qualifier("gameByYearWriter") JdbcBatchItemWriter<CsvRow> gameByYearWriter,
            @Qualifier("batchDestinationJdbcTemplate") JdbcTemplate jdbcTemplate
    ) {
        return new StepBuilder("csvToDB", repository)
                .<CsvRow, CsvRow>chunk(100, transactionManager)
                .reader(gameByYearReader)
                .writer(gameByYearWriter)
                .faultTolerant()
                .skip(IllegalArgumentException.class)
                .skipLimit(3)
                .listener(new StepExecutionListener() {
                    @Override
                    public void beforeStep(StepExecution stepExecution) {
                        System.out.println(MessageFormat.format("gameByYearStep is {0}.", stepExecution.getStepName()));
                    }

                    @Override
                    public ExitStatus afterStep(StepExecution stepExecution) {
                        Integer count = Objects.requireNonNull(jdbcTemplate.queryForObject("SELECT coalesce(count(*) ,0) FROM video_game_sales", Integer.class));
                        ExitStatus status = count == 0 ? new ExitStatus(EMPTY_CSV_STATUS) : ExitStatus.COMPLETED;
                        System.out.println("the status is " + status);
                        return status;
                    }
                })
                .build();
    }


    @Bean
    @Qualifier("endStep")
    public Step endStep(
            JobRepository jobRepository,
            PlatformTransactionManager transactionManager
    ) {
        return new StepBuilder("end", jobRepository)
                .tasklet((contribution, chunkContext) -> {
                    System.out.println("the job is finished");
                    return RepeatStatus.FINISHED;
                }, transactionManager)
                .build();
    }

    @Bean
    @Qualifier("errorStep")
    public Step errorStep(
            JobRepository repository,
            PlatformTransactionManager transactionManager
    ) {
        return new StepBuilder("errorStep", repository)
                .tasklet((contribution, chunkContext) -> {
                    System.out.println("oops!");
                    return RepeatStatus.FINISHED;
                }, transactionManager)
                .build();
    }


    @Bean
    @Qualifier("yearPlatformReportStep")
    public Step yearPlatformReportStep(
            JobRepository repository,
            /*@Qualifier("batchDestinationTM")*/ PlatformTransactionManager transactionManager,
            @Qualifier("batchDestinationJdbcTemplate") JdbcTemplate jdbcTemplate,
            @Qualifier("batchDestinationTT") TransactionTemplate transactionTemplate

    ) {
        return new StepBuilder("yearPlatformReportStep", repository)
                .tasklet((contribution, chunkContext) ->
                        transactionTemplate.execute(status -> {
                            jdbcTemplate.execute(
                                    """
                                                REPLACE INTO year_platform_report (year, platform)
                                                SELECT year, platform from video_game_sales;
                                            """);
                            jdbcTemplate.execute("""
                                    REPLACE INTO year_platform_report
                                    SELECT yp1.year,
                                           yp1.platform, (
                                               SELECT SUM(vgs.global_sales) FROM video_game_sales vgs
                                               WHERE vgs.platform = yp1.platform AND vgs.year = yp1.year
                                           ) sales
                                    FROM year_platform_report AS yp1
                                    """);
                            return RepeatStatus.FINISHED;
                        }), transactionManager)//
                .build();
    }

    @Bean
    @Qualifier("managerItemReader")
    public ListItemReader<Customer> managerItemReader() {
        return new ListItemReader<Customer>(Arrays.asList(new Customer("Dave"), new Customer("Michael"), new Customer("Mahmoud")));
    }

    @Bean
    @Qualifier("managerItemProcessor")
    public ItemProcessor<Object, Object> managerItemProcessor(ObjectMapper objectMapper) {
        return (customer) -> {
            return objectMapper.writeValueAsString(customer);
        };
    }

    @Bean // remoteChunk
    @Qualifier("managerStep")
    public TaskletStep managerStep(
            RemoteChunkingManagerStepBuilderFactory remoteChunkingManagerStepBuilderFactory,
            @Qualifier("managerItemReader") ListItemReader<Customer> managerItemReader,
            @Qualifier("managerItemProcessor") ItemProcessor<Object, Object> managerItemProcessor,
            @Qualifier("masterOutboundCustomerRequest") DirectChannel outbound,
            @Qualifier("masterInboundCustomerReply") QueueChannel inbound
    ) {
        return remoteChunkingManagerStepBuilderFactory.get("managerStep")
                .<Customer, String>chunk(2)
                .reader(managerItemReader)
                .processor(managerItemProcessor)
                .outputChannel(outbound) // requests sent to workers
                .inputChannel(inbound)   // replies received from workers
                .listener(new StepExecutionListener() {
                    @Override
                    public void beforeStep(StepExecution stepExecution) {
                        System.out.println(MessageFormat.format("yearReportStep is {0}.", stepExecution.getStepName()));
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


    public static YearReport rowMapper(ResultSet rs, int rowNum) throws SQLException {
        Integer year = rs.getInt("year");
        if (!JobCompletedListener.reportMap.containsKey(year)) {
            JobCompletedListener.reportMap.put(year, new YearReport(year, new ArrayList<YearPlatformSales>()));
        }
        YearReport yr = JobCompletedListener.reportMap.get(year);
        yr.breakout().add(new YearPlatformSales(rs.getInt("year"), rs.getString("platform"), rs.getFloat("sales")));
        return yr;
    }

    @Bean
    @StepScope
    @Qualifier("yearPlatformSalesItemReader")
    public ItemReader<YearReport> yearPlatformSalesItemReader(@Qualifier("dataSourceTwo") DataSource dataSource) {

        Map<String, Object> parameterValues = new HashMap<>();
        parameterValues.put("statusCode", 0);

        return new JdbcPagingItemReaderBuilder<YearReport>().name("customerReader")
                .dataSource(dataSource)
                .selectClause("""
                        SELECT year,
                        ypr.platform,
                        ypr.sales,
                        (SELECT COUNT(yps.year) FROM year_platform_report yps WHERE yps.year = ypr.year ) 
                         """)
                .fromClause("FROM year_platform_report ypr")
                .whereClause("WHERE ypr.year != :statusCode")
                .sortKeys(Map.of("year", Order.ASCENDING))
                .rowMapper(JobOne::rowMapper)
                .pageSize(100)
                .parameterValues(parameterValues)
                .build();

        /*return new JdbcCursorItemReaderBuilder<YearReport>()
                .sql(sql)
                .name("yearPlatformSalesItemReader")
                .dataSource(dataSource)
                .rowMapper(rowMapper)
                .build();*/

    }


    @Bean // remoteChunk
    @Qualifier("yearReportStep")
    public TaskletStep yearReportStep(
            RemoteChunkingManagerStepBuilderFactory remoteChunkingManagerStepBuilderFactory,
            @Qualifier("yearPlatformSalesItemReader") ItemReader<YearReport> itemReader,
            @Qualifier("masterOutboundYearReport") DirectChannel outbound,
            @Qualifier("masterInboundYearReport") QueueChannel inbound,
            PlatformTransactionManager platformTransactionManager,
            @Qualifier("batchDestinationJdbcTemplate") JdbcTemplate jdbcTemplate,
            ObjectMapper objectMapper
    ) {

        return remoteChunkingManagerStepBuilderFactory.get("yearReportStep")
                .<YearReport, String>chunk(100)
                .reader(itemReader)
                .processor((yearReport) -> {
                    return objectMapper.writeValueAsString(yearReport);
                })
                .outputChannel(outbound) // requests sent to workers
                .inputChannel(inbound)   // replies received from workers
                .transactionManager(platformTransactionManager)
                .listener(new StepExecutionListener() {
                    @Override
                    public void beforeStep(StepExecution stepExecution) {
                        System.out.println(MessageFormat.format("yearReportStep is {0}.", stepExecution.getStepName()));
                    }

                    @Override
                    public ExitStatus afterStep(StepExecution stepExecution) {
                        Integer count = Objects.requireNonNull(jdbcTemplate.queryForObject("SELECT coalesce(count(*) ,0) FROM remote_year_platform_report", Integer.class));
                        ExitStatus status = count == 0 ? new ExitStatus(EMPTY_CSV_STATUS) : ExitStatus.COMPLETED;
                        System.out.println("the status is " + status);
                        return status;
                    }
                })
                .build();
    }


}
