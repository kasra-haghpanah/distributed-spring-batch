package com.example.batchprocessing.master.configuration.rabbitmq;

import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

@Component
public class Receiver {

    public void receiveMessage(byte[] bytes) {
        String message = new String(bytes, StandardCharsets.UTF_8);
        System.out.println("{\"received\": \"" + message + "\"}");
    }


}
