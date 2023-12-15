package com.example.batchprocessing.slave.structure;

import java.util.Collection;

public record YearReport(int year, Collection<YearPlatformSales> breakout) {
}
