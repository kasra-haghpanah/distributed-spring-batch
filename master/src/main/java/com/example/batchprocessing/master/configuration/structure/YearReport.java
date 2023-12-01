package com.example.batchprocessing.master.configuration.structure;

import java.util.Collection;

public record YearReport(int year, Collection<YearPlatformSales> breakout) {
}
