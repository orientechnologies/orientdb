package com.orientechnologies.common.profiler;

public record OProfilerEntrySnapshot(
    long entries,
    long last,
    long min,
    long max,
    long total,
    long firstExecution,
    long lastExecution,
    long lastResetEntries,
    long lastReset) {}
