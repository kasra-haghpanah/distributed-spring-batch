package com.example.batchprocessing.master.configuration.structure;

import java.time.LocalDate;

public record Email(String receiver, String subject, String textMessage, LocalDate date) {
}
