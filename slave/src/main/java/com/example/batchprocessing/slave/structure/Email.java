package com.example.batchprocessing.slave.structure;

import java.time.LocalDate;

public record Email(String receiver, String subject, String textMessage, LocalDate date) {
}
