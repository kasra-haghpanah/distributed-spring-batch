package com.example.batchprocessing.master.configuration.structure;

public record GameByYear(int rank, String name, String platform, int year, String genre, String publisher, float na,
                         float eu, float jp, float other, float global) {
}
