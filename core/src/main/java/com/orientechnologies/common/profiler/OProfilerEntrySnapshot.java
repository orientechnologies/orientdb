package com.orientechnologies.common.profiler;

import com.orientechnologies.orient.core.record.impl.ODocument;
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

  public OProfilerEntrySnapshot(ODocument doc) {
    this(
        doc.getProperty("entries"),
        doc.getProperty("last"),
        doc.getProperty("min"),
        doc.getProperty("max"),
        doc.getProperty("total"),
        doc.getProperty("firstExecution"),
        doc.getProperty("lastExecution"),
        doc.getProperty("lastReset"),
        doc.getProperty("lastResetEntries"));
  }

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

  public ODocument toDocument() {
    final ODocument doc = new ODocument();
    doc.setProperty("entries", entries());
    doc.setProperty("last", last());
    doc.setProperty("min", min());
    doc.setProperty("max", max());
    doc.setProperty("average", average());
    doc.setProperty("total", total());
    doc.setProperty("firstExecution", firstExecution());
    doc.setProperty("lastExecution", lastExecution());
    doc.setProperty("lastReset", lastReset());
    doc.setProperty("lastResetEntries", lastResetEntries());
    return doc;
  }

  public float average() {
    if (entries > 0) {
      return (float) total / (float) entries;
    } else {
      return 0;
    }
  }
}
