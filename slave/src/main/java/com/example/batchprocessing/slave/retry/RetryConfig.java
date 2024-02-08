package com.example.batchprocessing.slave.retry;


import com.example.batchprocessing.slave.configuration.properties.Properties;
import org.springframework.beans.factory.support.DefaultSingletonBeanRegistry;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.annotation.Recover;
import org.springframework.retry.listener.MethodInvocationRetryListenerSupport;
import org.springframework.retry.support.RetryTemplate;

import java.io.IOException;
import java.net.SocketException;
import java.text.MessageFormat;
import java.time.Duration;
import java.util.Arrays;

@Configuration(proxyBeanMethods = false)
@DependsOn({"properties"})
public class RetryConfig {

    public class CustomRetryListener extends MethodInvocationRetryListenerSupport {

        @Override
        public <T, E extends Throwable> void onError(RetryContext context, RetryCallback<T, E> callback, Throwable throwable) {
            System.out.println("Retrying...");
            super.onError(context, callback, throwable);
        }
    }

    @Bean
    RetryTemplate retryTemplate() {
        return RetryTemplate.builder()
                .maxAttempts(Properties.getRetryMaxAttempts())
                .fixedBackoff(Duration.ofSeconds(Properties.getRetryFixedBackoff()))
                .retryOn(Arrays.asList(SocketException.class, IOException.class))
                .withListener(new CustomRetryListener())
                .build();
    }

    @Bean
    public CommandLineRunner commandLineRunner(
            RetryTemplate retryTemplate,
            ApplicationContext applicationContext
    ) {

        DefaultSingletonBeanRegistry registry = (DefaultSingletonBeanRegistry) applicationContext.getAutowireCapableBeanFactory();

        return args -> {

            // With annotations
            System.out.println("retry!!!!!!!!!");
            // RetryTemplate
            retryTemplate.execute(context -> {

                return null;
            }, context -> {
                System.out.println(MessageFormat.format("Recovering from {0}", context.getLastThrowable().getMessage()));

                return null;
            });

        };
    }

    @Recover
    public void recover(Exception e) {
        System.out.println(MessageFormat.format("Recovering from {0}", e.getMessage()));
    }

}
