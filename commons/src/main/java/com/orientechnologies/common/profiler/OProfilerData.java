/*
 * Copyright 1999-2005 Luca Garulli (l.garulli--at-orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.orientechnologies.common.profiler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.WeakHashMap;

import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.log.OLogManager;

/**
 * Profiling utility class. Handles chronos (times), statistics and counters. By default it's used as Singleton but you can create
 * any instances you want for separate profiling contexts.
 * 
 * To start the recording use call startRecording(). By default record is turned off to avoid a run-time execution cost.
 * 
 * @author Luca Garulli
 * @copyrights Orient Technologies.com
 */
public class OProfilerData {
  private long                        recordingFrom = 0;
  private long                        recordingTo   = Long.MAX_VALUE;
  private Map<String, Long>           counters;
  private Map<String, OProfilerEntry> chronos;
  private Map<String, OProfilerEntry> stats;
  private Map<String, Object>         hooks;

  public class OProfilerEntry {
    public String name    = null;
    public long   entries = 0;
    public long   last    = 0;
    public long   min     = 999999999;
    public long   max     = 0;
    public long   average = 0;
    public long   total   = 0;
    public String payLoad;
    public String description;

    public void toJSON(final StringBuilder buffer) {
      buffer.append(String.format("\"%s\":{", name.replaceAll("\"","\\\\\"")));
      buffer.append(String.format("\"%s\":{", OIOUtils.encode(name)));
      buffer.append(String.format("\"%s\":%d,", "entries", entries));
      buffer.append(String.format("\"%s\":%d,", "last", last));
      buffer.append(String.format("\"%s\":%d,", "min", min));
      buffer.append(String.format("\"%s\":%d,", "max", max));
      buffer.append(String.format("\"%s\":%d,", "average", average));
      buffer.append(String.format("\"%s\":%d", "total", total));
      if (payLoad != null)
        buffer.append(String.format("\"%s\":%d", "payload", payLoad));
      buffer.append("}");
    }

    @Override
    public String toString() {
      return String.format("Profiler entry [%s]: total=%d, average=%d, items=%d, last=%d, max=%d, min=%d", total, name, average,
          entries, last, max, min);
    }
  }

  public OProfilerData() {
    counters = new HashMap<String, Long>();
    chronos = new HashMap<String, OProfilerEntry>();
    stats = new HashMap<String, OProfilerEntry>();
    hooks = new WeakHashMap<String, Object>();
    recordingFrom = System.currentTimeMillis();
  }

  public void clear() {
    counters.clear();
    chronos.clear();
    stats.clear();
    hooks.clear();
  }

  public long endRecording() {
    recordingTo = System.currentTimeMillis();
    return recordingTo;
  }

  public void mergeWith(final OProfilerData iToMerge) {
    if (iToMerge.recordingFrom < recordingFrom)
      recordingFrom = iToMerge.recordingFrom;
    if (iToMerge.recordingTo > recordingTo)
      recordingTo = iToMerge.recordingTo;

    // COUNTERS
    for (Entry<String, Long> entry : iToMerge.counters.entrySet()) {
      Long currentValue = counters.get(entry.getKey());
      if (currentValue == null)
        currentValue = 0l;
      counters.put(entry.getKey(), currentValue + entry.getValue());
    }

    // HOOKS
    for (Entry<String, Object> entry : iToMerge.hooks.entrySet()) {
      Object currentValue = hooks.get(entry.getKey());
      if (currentValue == null)
        currentValue = entry.getValue();
      else {
        // MERGE IT
        final Object otherValue = entry.getValue();
        if (currentValue instanceof Long)
          currentValue = ((Long) currentValue).longValue() + ((Long) otherValue).longValue();
        else if (currentValue instanceof Integer)
          currentValue = ((Integer) currentValue).intValue() + ((Integer) otherValue).intValue();
        else if (currentValue instanceof Short)
          currentValue = ((Short) currentValue).shortValue() + ((Short) otherValue).shortValue();
        else if (currentValue instanceof Float)
          currentValue = ((Float) currentValue).floatValue() + ((Float) otherValue).floatValue();
        else if (currentValue instanceof Double)
          currentValue = ((Double) currentValue).doubleValue() + ((Double) otherValue).doubleValue();
        else if (currentValue instanceof Boolean)
          currentValue = otherValue;
        else if (currentValue instanceof String)
          currentValue = otherValue;
        else
          OLogManager.instance().warn(this, "Type of value '%s' not support on profiler hook '%s' to merge with value: %s",
              currentValue, entry.getKey(), entry.getValue());
      }

      hooks.put(entry.getKey(), currentValue);
    }

    // CHRONOS
    mergeEntries(chronos, iToMerge.chronos);

    // STATS
    mergeEntries(stats, iToMerge.stats);
  }

  public void toJSON(final StringBuilder buffer, final String iFilter) {
    buffer.append("{");
    buffer.append(String.format("\"from\": %d,", recordingFrom));
    buffer.append(String.format("\"to\": %d,", recordingTo));

    // HOOKS
    buffer.append("\"hookValues\":{ ");

    List<String> names = new ArrayList<String>(hooks.keySet());
    Collections.sort(names);
    boolean firstItem = true;
    for (String k : names) {
      if (iFilter != null && !k.startsWith(iFilter))
        // APPLIED FILTER: DOESN'T MATCH
        continue;

      final Object value = hooks.get(k);
      if (firstItem)
        firstItem = false;
      else
        buffer.append(',');

      if (value == null)
        buffer.append(String.format("\"%s\":null", OIOUtils.encode(k)));
      else if (value instanceof Number)
        buffer.append(String.format("\"%s\":%d", OIOUtils.encode(k), value));
      else if (value instanceof Boolean)
        buffer.append(String.format("\"%s\":%s", OIOUtils.encode(k), value));
      else {
        buffer.append(String.format("\"%s\":\"%s\"", OIOUtils.encode(k), OIOUtils.encode(value)));
      }
    }
    buffer.append("}");

    // CHRONOS
    buffer.append(",\"chronos\":{");
    names = new ArrayList<String>(chronos.keySet());
    Collections.sort(names);
    firstItem = true;
    for (String k : names) {
      if (iFilter != null && !k.startsWith(iFilter))
        // APPLIED FILTER: DOESN'T MATCH
        continue;

      if (firstItem)
        firstItem = false;
      else
        buffer.append(',');
      chronos.get(k).toJSON(buffer);
    }
    buffer.append("}");

    // STATISTICS
    buffer.append(",\"statistics\":{");
    names = new ArrayList<String>(stats.keySet());
    Collections.sort(names);
    firstItem = true;
    for (String k : names) {
      if (iFilter != null && !k.startsWith(iFilter))
        // APPLIED FILTER: DOESN'T MATCH
        continue;

      if (firstItem)
        firstItem = false;
      else
        buffer.append(',');
      stats.get(k).toJSON(buffer);
    }
    buffer.append("}");

    // COUNTERS
    buffer.append(",\"counters\":{");
    names = new ArrayList<String>(counters.keySet());
    Collections.sort(names);
    firstItem = true;
    for (String k : names) {
      if (iFilter != null && !k.startsWith(iFilter))
        // APPLIED FILTER: DOESN'T MATCH
        continue;

      if (firstItem)
        firstItem = false;
      else
        buffer.append(',');
      buffer.append(String.format("\"%s\":%d", OIOUtils.encode(k), counters.get(k)));
    }
    buffer.append("}");

    buffer.append("}");
  }

  public String dump() {
    final StringBuilder buffer = new StringBuilder();
    buffer.append("Dump of profiler data from " + new Date(recordingFrom) + " to " + new Date(recordingFrom) + "\n");

    buffer.append(dumpHookValues());
    buffer.append("\n");
    buffer.append(dumpCounters());
    buffer.append("\n\n");
    buffer.append(dumpStats());
    buffer.append("\n\n");
    buffer.append(dumpChronos());
    return buffer.toString();
  }

  public void updateCounter(final String iStatName, final long iPlus) {
    if (iStatName == null)
      return;

    synchronized (counters) {
      final Long stat = counters.get(iStatName);
      final long oldValue = stat == null ? 0 : stat.longValue();
      counters.put(iStatName, new Long(oldValue + iPlus));
    }
  }

  public long getCounter(final String iStatName) {
    if (iStatName == null)
      return -1;

    synchronized (counters) {
      final Long stat = counters.get(iStatName);
      if (stat == null)
        return -1;

      return stat.longValue();
    }
  }

  public String dumpCounters() {
    synchronized (counters) {
      final StringBuilder buffer = new StringBuilder();
      buffer.append("Dumping COUNTERS:");

      buffer.append(String.format("\n%50s +-------------------------------------------------------------------+", ""));
      buffer.append(String.format("\n%50s | Value                                                             |", "Name"));
      buffer.append(String.format("\n%50s +-------------------------------------------------------------------+", ""));

      final List<String> keys = new ArrayList<String>(counters.keySet());
      Collections.sort(keys);

      for (String k : keys) {
        final Long stat = counters.get(k);
        buffer.append(String.format("\n%-50s | %-65d |", k, stat));
      }
      buffer.append(String.format("\n%50s +-------------------------------------------------------------------+", ""));
      return buffer.toString();
    }
  }

  public long stopChrono(final String iName, final long iStartTime, final String iPayload) {
    return updateEntry(chronos, iName, System.currentTimeMillis() - iStartTime, iPayload);
  }

  public String dumpChronos() {
    return dumpEntries(chronos, new StringBuilder("Dumping CHRONOS. Times in ms:"));
  }

  public long updateStat(final String iName, final long iValue) {
    return updateEntry(stats, iName, iValue, null);
  }

  public String dumpStats() {
    return dumpEntries(stats, new StringBuilder("Dumping STATISTICS. Times in ms:"));
  }

  public String dumpHookValues() {
    final StringBuilder buffer = new StringBuilder();

    synchronized (hooks) {
      if (hooks.size() == 0)
        return "";

      buffer.append("Dumping HOOK VALUES:");

      buffer.append(String.format("\n%50s +-------------------------------------------------------------------+", ""));
      buffer.append(String.format("\n%50s | Value                                                             |", "Name"));
      buffer.append(String.format("\n%50s +-------------------------------------------------------------------+", ""));

      final List<String> names = new ArrayList<String>(hooks.keySet());
      Collections.sort(names);

      for (String k : names) {
        final Object hookValue = hooks.get(k);
        buffer.append(String.format("\n%-50s | %-65s |", k, hookValue != null ? hookValue.toString() : "null"));
      }
    }

    buffer.append(String.format("\n%50s +-------------------------------------------------------------------+", ""));
    return buffer.toString();
  }

  public Object getHookValue(final String iName) {
    if (iName == null)
      return null;

    synchronized (hooks) {
      return hooks.get(iName);
    }
  }

  public void setHookValues(final Map<String, Object> iHooks) {
    synchronized (hooks) {
      hooks.clear();
      if (iHooks != null)
        hooks.putAll(iHooks);
    }
  }

  public String[] getCountersAsString() {
    synchronized (counters) {
      final String[] output = new String[counters.size()];
      int i = 0;
      for (Entry<String, Long> entry : counters.entrySet()) {
        output[i++] = entry.getKey() + ": " + entry.getValue().toString();
      }
      return output;
    }
  }

  public String[] getChronosAsString() {
    synchronized (chronos) {
      final String[] output = new String[chronos.size()];
      int i = 0;
      for (Entry<String, OProfilerEntry> entry : chronos.entrySet()) {
        output[i++] = entry.getKey() + ": " + entry.getValue().toString();
      }
      return output;
    }
  }

  public String[] getStatsAsString() {
    synchronized (stats) {
      final String[] output = new String[stats.size()];
      int i = 0;
      for (Entry<String, OProfilerEntry> entry : stats.entrySet()) {
        output[i++] = entry.getKey() + ": " + entry.getValue().toString();
      }
      return output;
    }
  }

  public List<String> getCounters() {
    synchronized (counters) {
      final List<String> list = new ArrayList<String>(counters.keySet());
      Collections.sort(list);
      return list;
    }
  }

  public List<String> getHooks() {
    synchronized (hooks) {
      final List<String> list = new ArrayList<String>(hooks.keySet());
      Collections.sort(list);
      return list;
    }
  }

  public List<String> getChronos() {
    synchronized (chronos) {
      final List<String> list = new ArrayList<String>(chronos.keySet());
      Collections.sort(list);
      return list;
    }
  }

  public List<String> getStats() {
    synchronized (stats) {
      final List<String> list = new ArrayList<String>(stats.keySet());
      Collections.sort(list);
      return list;
    }
  }

  public OProfilerEntry getStat(final String iStatName) {
    if (iStatName == null)
      return null;

    synchronized (stats) {
      return stats.get(iStatName);
    }
  }

  public OProfilerEntry getChrono(final String iChronoName) {
    if (iChronoName == null)
      return null;

    synchronized (chronos) {
      return chronos.get(iChronoName);
    }
  }

  protected synchronized long updateEntry(final Map<String, OProfilerEntry> iValues, final String iName, final long iValue,
      final String iPayload) {
    synchronized (iValues) {
      OProfilerEntry c = iValues.get(iName);

      if (c == null) {
        // CREATE NEW CHRONO
        c = new OProfilerEntry();
        iValues.put(iName, c);
      }

      c.name = iName;
      c.payLoad = iPayload;
      c.entries++;
      c.last = iValue;
      c.total += c.last;
      c.average = c.total / c.entries;

      if (c.last < c.min)
        c.min = c.last;

      if (c.last > c.max)
        c.max = c.last;

      return c.last;
    }
  }

  protected synchronized String dumpEntries(final Map<String, OProfilerEntry> iValues, final StringBuilder iBuffer) {
    // CHECK IF CHRONOS ARE ACTIVED
    synchronized (iValues) {
      if (iValues.size() == 0)
        return "";

      OProfilerEntry c;

      iBuffer.append(String.format("\n%50s +-------------------------------------------------------------------+", ""));
      iBuffer.append(String.format("\n%50s | %10s %10s %10s %10s %10s %10s |", "Name", "last", "total", "min", "max", "average",
          "items"));
      iBuffer.append(String.format("\n%50s +-------------------------------------------------------------------+", ""));

      final List<String> keys = new ArrayList<String>(iValues.keySet());
      Collections.sort(keys);

      for (String k : keys) {
        c = iValues.get(k);
        iBuffer.append(String.format("\n%-50s | %10d %10d %10d %10d %10d %10d |", k, c.last, c.total, c.min, c.max, c.average,
            c.entries));
      }
      iBuffer.append(String.format("\n%50s +-------------------------------------------------------------------+", ""));
      return iBuffer.toString();
    }
  }

  protected void mergeEntries(final Map<String, OProfilerEntry> iMyEntries, final Map<String, OProfilerEntry> iOthersEntries) {
    for (Entry<String, OProfilerEntry> entry : iOthersEntries.entrySet()) {
      OProfilerEntry currentValue = iMyEntries.get(entry.getKey());
      if (currentValue == null) {
        currentValue = entry.getValue();
        iMyEntries.put(entry.getKey(), currentValue);
      } else {
        // MERGE IT
        currentValue.entries += entry.getValue().entries;
        currentValue.last = entry.getValue().last;
        currentValue.min = Math.min(currentValue.min, entry.getValue().min);
        currentValue.max = Math.max(currentValue.max, entry.getValue().max);
        currentValue.average = (currentValue.total + entry.getValue().total) / currentValue.entries;
        currentValue.total += entry.getValue().total;
      }
    }
  }

  public boolean isInRange(final long from, final long to) {
    return recordingFrom >= from && recordingTo <= to;
  }
}
