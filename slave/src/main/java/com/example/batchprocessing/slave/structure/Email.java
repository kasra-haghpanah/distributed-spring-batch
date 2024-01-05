package com.example.batchprocessing.slave.structure;

import java.time.LocalDate;

public record Email(String sender, String receiver, String textMessage, LocalDate date) {
}
