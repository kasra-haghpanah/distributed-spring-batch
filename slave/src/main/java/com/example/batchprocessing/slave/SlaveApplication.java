package com.example.batchprocessing.slave;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SlaveApplication {

    public static void main(String[] args) {
        System.setProperty("spring.amqp.deserialization.trust.all","true");
        SpringApplication.run(SlaveApplication.class, args);
    }

}
