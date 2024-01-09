package com.example.batchprocessing.slave.integration;

import com.example.batchprocessing.slave.configuration.properties.Properties;
import com.example.batchprocessing.slave.structure.Email;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.batch.integration.chunk.RemoteChunkingWorkerBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;

import java.time.ZoneId;
import java.util.Date;
import java.util.List;

@Configuration
@ConditionalOnProperty(value = "bootiful.batch.chunk.slave", havingValue = "true")
class SlaveEmailRemoteChunk {


    ItemProcessor<String, SimpleMailMessage> itemProcessor(ObjectMapper objectMapper) {
        return item -> {//MailMessage
            System.out.println(item);
            Email email = objectMapper.readValue(item, Email.class);
            Date date = Date.from(email.date().atStartOfDay(ZoneId.of(Properties.getTimeZone())).toInstant());

            SimpleMailMessage message = new SimpleMailMessage();
            message.setTo(email.receiver());
            message.setFrom("kasrakhpk1985@gmail.com");
            message.setSubject(email.receiver());
            message.setSentDate(date);
            message.setText(email.textMessage());

            return message;
        };
    }

    @Bean
    @Qualifier("emailItemWriter")
    public ItemWriter<SimpleMailMessage> emailItemWriter(JavaMailSender javaMailSender) {

        return (chunk) -> {

            System.out.println("doing the long-running writing thing");
            List<SimpleMailMessage> items = (List<SimpleMailMessage>) chunk.getItems();
            for (SimpleMailMessage email : items) {
                //javaMailSender.send(email);
                System.out.println("itemWriter => " + email);
            }

        };

    }

    @Bean
    public IntegrationFlow emailWorkerFlow(
            RemoteChunkingWorkerBuilder workerBuilder,
            @Qualifier("emailItemWriter") ItemWriter<SimpleMailMessage> itemWriter,
            @Qualifier("slaveInboundEmailRequest") DirectChannel inbound,
            @Qualifier("slaveOutboundEmailReply") DirectChannel outbound,
            ObjectMapper objectMapper
    ) {
        return workerBuilder
                .itemProcessor(itemProcessor(objectMapper))
                .itemWriter(itemWriter)
                .inputChannel(inbound) // requests received from the manager
                .outputChannel(outbound) // replies sent to the manager
                .build();
    }

}
