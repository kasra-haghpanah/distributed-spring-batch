package com.example.batchprocessing.slave;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;

@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class })
public class SlaveApplication {

    public static void main(String[] args) {
        SpringApplication.run(SlaveApplication.class, args);
    }

}
