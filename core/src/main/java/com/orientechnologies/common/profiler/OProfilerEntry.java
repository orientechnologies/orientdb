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
  public String name = null;
  public long entries = 0;
  public long last = 0;
  public long min = 999999999;
  public long max = 0;
  public float average = 0;
  public long total = 0;
  public final long firstExecution;
  public long lastExecution;

  public String payLoad;
  public String description;

  public long lastResetEntries = 0;
  public long lastReset;

  public Set<String> users = new HashSet<String>();

  public OProfilerEntry() {
    firstExecution = System.currentTimeMillis();
    lastExecution = firstExecution;
  }

  public void updateLastExecution() {
    lastExecution = System.currentTimeMillis();
  }

  public ODocument toDocument() {
    final ODocument doc = new ODocument();
    doc.field("entries", entries);
    doc.field("last", last);
    doc.field("min", min);
    doc.field("max", max);
    doc.field("average", average);
    doc.field("total", total);
    doc.field("firstExecution", firstExecution);
    doc.field("lastExecution", lastExecution);
    doc.field("lastReset", lastReset);
    doc.field("lastResetEntries", lastResetEntries);
    if (payLoad != null) doc.field("payload", payLoad);
    return doc;
  }

  public String toJSON() {
    final StringBuilder buffer = new StringBuilder(1024);
    toJSON(buffer);
    return buffer.toString();
  }

  public void toJSON(final StringBuilder buffer) {
    buffer.append('{');
    buffer.append(String.format(Locale.ENGLISH, "\"%s\":%d,", "entries", entries));
    buffer.append(String.format(Locale.ENGLISH, "\"%s\":%d,", "last", last));
    buffer.append(String.format(Locale.ENGLISH, "\"%s\":%d,", "min", min));
    buffer.append(String.format(Locale.ENGLISH, "\"%s\":%d,", "max", max));
    buffer.append(String.format(Locale.ENGLISH, "\"%s\":%.2f,", "average", average));
    buffer.append(String.format(Locale.ENGLISH, "\"%s\":%d,", "total", total));
    buffer.append(String.format(Locale.ENGLISH, "\"%s\":%d,", "firstExecution", firstExecution));
    buffer.append(String.format(Locale.ENGLISH, "\"%s\":%d,", "lastExecution", lastExecution));
    buffer.append(String.format(Locale.ENGLISH, "\"%s\":%d,", "lastReset", lastReset));
    buffer.append(
        String.format(Locale.ENGLISH, "\"%s\":%d,", "lastResetEntries,", lastResetEntries));
    if (payLoad != null)
      buffer.append(String.format(Locale.ENGLISH, "\"%s\":\"%s\"", "payload,", payLoad));
    buffer.append(String.format(Locale.ENGLISH, "\"%s\": [", "users"));

    String usersList = "";
    int i = 0;
    for (String user : users) {
      buffer.append(String.format(Locale.ENGLISH, "%s\"%s\"", (i > 0) ? "," : "", user));
      i++;
    }
    buffer.append(String.format(Locale.ENGLISH, "%s", usersList));

    buffer.append(String.format(Locale.ENGLISH, "]"));
    buffer.append('}');
  }

  @Override
  public String toString() {
    return String.format(
        "Profiler entry [%s]: total=%d, average=%.2f, items=%d, last=%d, max=%d, min=%d",
        name, total, average, entries, last, max, min);
  }
}
