package com.example.batchprocessing.master;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class MasterApplication {
    // java -DspringAot=true -jar master.jar
    public static void main(String[] args) {
        System.setProperty("spring.amqp.deserialization.trust.all","true");
        SpringApplication.run(MasterApplication.class, args);
    }

}
