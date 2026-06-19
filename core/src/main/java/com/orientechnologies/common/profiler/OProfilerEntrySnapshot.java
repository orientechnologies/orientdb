package com.orientechnologies.common.profiler;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public record OProfilerEntrySnapshot(
    long entries,
    long last,
    long min,
    long max,
    long total,
    long firstExecution,
    long lastExecution,
    long lastResetEntries,
    long lastReset) {

  public void writeNetwork(DataOutput out) throws IOException {
    out.writeLong(entries);
    out.writeLong(last);
    out.writeLong(min);
    out.writeLong(max);
    out.writeLong(total);
    out.writeLong(firstExecution);
    out.writeLong(lastExecution);
    out.writeLong(lastResetEntries);
    out.writeLong(lastReset);
  }

  public static OProfilerEntrySnapshot readNetwork(DataInput input) throws IOException {
    var entries = input.readLong();
    var last = input.readLong();
    var min = input.readLong();
    var max = input.readLong();
    var total = input.readLong();
    var firstExecution = input.readLong();
    var lastExecution = input.readLong();
    var lastResetEntries = input.readLong();
    var lastReset = input.readLong();

    return new OProfilerEntrySnapshot(
        entries, last, min, max, total, firstExecution, lastExecution, lastResetEntries, lastReset);
  }
}
