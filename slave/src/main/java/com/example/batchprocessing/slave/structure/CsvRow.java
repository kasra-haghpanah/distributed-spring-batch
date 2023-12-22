package com.example.batchprocessing.slave.structure;

public record CsvRow(int rank, String name, String platform, int year, String genre, String publisher, float na, float eu,
                     float jp, float other, float global) {
}
