package com.example.batchprocessing.master;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class MasterApplication {

    public static void main(String[] args) {
        System.setProperty("spring.amqp.deserialization.trust.all","true");
        SpringApplication.run(MasterApplication.class, args);
    }

}
