package com.example.batchprocessing.master.configuration.structure;

import java.time.LocalDate;

public record Email(String sender, String receiver, String textMessage, LocalDate date) {
}
