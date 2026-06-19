/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.common.profiler;

import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

/**
 * Contains the profiling data abount timing.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OProfilerEntry {
  private String name = null;
  private long entries = 0;
  private long last = 0;
  private long min = 999999999;
  private long max = 0;
  private long total = 0;
  private final long firstExecution;
  private long lastExecution;

  private String payLoad;

  private long lastResetEntries = 0;
  private long lastReset;

  private Set<String> users;

  public OProfilerEntry(String name) {
    this.name = name;
    this.firstExecution = System.currentTimeMillis();
    this.lastExecution = this.firstExecution;
  }

  public ODocument toDocument() {
    final ODocument doc = new ODocument();
    doc.field("entries", getEntries());
    doc.field("last", getLast());
    doc.field("min", getMin());
    doc.field("max", getMax());
    doc.field("average", getAverage());
    doc.field("total", getTotal());
    doc.field("firstExecution", getFirstExecution());
    doc.field("lastExecution", getLastExecution());
    doc.field("lastReset", getLastReset());
    doc.field("lastResetEntries", getLastResetEntries());
    if (getPayLoad() != null) doc.field("payload", getPayLoad());
    return doc;
  }

  public OProfilerEntrySnapshot toSnapshot() {
    return new OProfilerEntrySnapshot(
        entries, last, min, max, total, firstExecution, lastExecution, lastResetEntries, lastReset);
  }

  public String toJSON() {
    final StringBuilder buffer = new StringBuilder(1024);
    toJSON(buffer);
    return buffer.toString();
  }

  public void toJSON(final StringBuilder buffer) {
    buffer.append('{');
    buffer.append(String.format(Locale.ENGLISH, "\"%s\":%d,", "entries", getEntries()));
    buffer.append(String.format(Locale.ENGLISH, "\"%s\":%d,", "last", getLast()));
    buffer.append(String.format(Locale.ENGLISH, "\"%s\":%d,", "min", getMin()));
    buffer.append(String.format(Locale.ENGLISH, "\"%s\":%d,", "max", getMax()));
    buffer.append(String.format(Locale.ENGLISH, "\"%s\":%.2f,", "average", getAverage()));
    buffer.append(String.format(Locale.ENGLISH, "\"%s\":%d,", "total", getTotal()));
    buffer.append(
        String.format(Locale.ENGLISH, "\"%s\":%d,", "firstExecution", getFirstExecution()));
    buffer.append(String.format(Locale.ENGLISH, "\"%s\":%d,", "lastExecution", getLastExecution()));
    buffer.append(String.format(Locale.ENGLISH, "\"%s\":%d,", "lastReset", getLastReset()));
    buffer.append(
        String.format(Locale.ENGLISH, "\"%s\":%d,", "lastResetEntries,", getLastResetEntries()));
    if (getPayLoad() != null)
      buffer.append(String.format(Locale.ENGLISH, "\"%s\":\"%s\"", "payload,", getPayLoad()));
    buffer.append(String.format(Locale.ENGLISH, "\"%s\": [", "users"));

    if (getUsers() != null) {
      String usersList = "";
      int i = 0;
      for (String user : getUsers()) {
        buffer.append(String.format(Locale.ENGLISH, "%s\"%s\"", (i > 0) ? "," : "", user));
        i++;
      }
      buffer.append(String.format(Locale.ENGLISH, "%s", usersList));
    }

    buffer.append(String.format(Locale.ENGLISH, "]"));
    buffer.append('}');
  }

  @Override
  public String toString() {
    return String.format(
        "Profiler entry [%s]: total=%d, average=%.2f, items=%d, last=%d, max=%d, min=%d",
        getName(), getTotal(), getAverage(), getEntries(), getLast(), getMax(), getMin());
  }

  public Set<String> getUsers() {
    return users;
  }

  public void addUser(String user) {
    if (users == null) {
      users = new HashSet<>();
    }
    users.add(user);
  }

  public long getLastReset() {
    return lastReset;
  }

  public long getLastResetEntries() {
    return lastResetEntries;
  }

  public String getPayLoad() {
    return payLoad;
  }

  public long getLastExecution() {
    return lastExecution;
  }

  public long getFirstExecution() {
    return firstExecution;
  }

  public long getTotal() {
    return total;
  }

  public float getAverage() {
    if (entries > 0) {
      return (float) total / (float) entries;
    } else {
      return 0;
    }
  }

  public long getMax() {
    return max;
  }

  public long getMin() {
    return min;
  }

  public long getLast() {
    return last;
  }

  public long getEntries() {
    return entries;
  }

  public String getName() {
    return name;
  }

  public void update(long value, String payload, String user) {
    this.payLoad = payload;
    if (user != null) addUser(user);
    this.lastExecution = System.currentTimeMillis();
    internalUpdate(value);
  }

  public void internalUpdate(long value) {
    this.entries++;
    this.lastResetEntries++;
    this.last = value;
    this.total += value;
    if (value < min) min = value;
    if (value > max) max = value;
  }

  public void resettableUpdate(long value, int resetTime) {
    this.lastExecution = System.currentTimeMillis();
    if (lastExecution - lastReset > resetTime) {
      reset();
    }
    internalUpdate(value);
  }

  protected void reset() {
    last = 0;
    total = 0;
    min = 0;
    max = 0;
    lastResetEntries = 0;
    lastReset = lastExecution;
  }
}
