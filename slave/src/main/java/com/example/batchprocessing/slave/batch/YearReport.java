package com.example.batchprocessing.slave.batch;

import java.util.Collection;

public record YearReport(int year, Collection<YearPlatformSales> breakout) {
}
