/*
 * Copyright 2010-2013 Orient Technologies LTD (info--at--orientechnologies.com)
 * All Rights Reserved. Commercial License.
 * 
 * NOTICE:  All information contained herein is, and remains the property of
 * Orient Technologies LTD and its suppliers, if any.  The intellectual and
 * technical concepts contained herein are proprietary to
 * Orient Technologies LTD and its suppliers and may be covered by United
 * Kingdom and Foreign Patents, patents in process, and are protected by trade
 * secret or copyright law.
 * 
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from Orient Technologies LTD.
 * 
 * For more information: http://www.orientechnologies.com
 */

package com.orientechnologies.agent.profiler;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.profiler.OProfilerEntry;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

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
  private final ConcurrentLinkedHashMap<String, Long>           counters      = new ConcurrentLinkedHashMap.Builder()
                                                                                  .maximumWeightedCapacity(
                                                                                      OGlobalConfiguration.PROFILER_MAXVALUES
                                                                                          .getValueAsInteger()).build();
  private final ConcurrentLinkedHashMap<String, OProfilerEntry> chronos       = new ConcurrentLinkedHashMap.Builder()
                                                                                  .maximumWeightedCapacity(
                                                                                      OGlobalConfiguration.PROFILER_MAXVALUES
                                                                                          .getValueAsInteger()).build();
  private final ConcurrentLinkedHashMap<String, OProfilerEntry> stats         = new ConcurrentLinkedHashMap.Builder()
                                                                                  .maximumWeightedCapacity(
                                                                                      OGlobalConfiguration.PROFILER_MAXVALUES
                                                                                          .getValueAsInteger()).build();
  private final ConcurrentLinkedHashMap<String, AtomicInteger>  tips          = new ConcurrentLinkedHashMap.Builder()
                                                                                  .maximumWeightedCapacity(
                                                                                      OGlobalConfiguration.PROFILER_MAXVALUES
                                                                                          .getValueAsInteger()).build();
  private final ConcurrentLinkedHashMap<String, Long>           tipsTimestamp = new ConcurrentLinkedHashMap.Builder()
                                                                                  .maximumWeightedCapacity(
                                                                                      OGlobalConfiguration.PROFILER_MAXVALUES
                                                                                          .getValueAsInteger()).build();
  private final Map<String, Object>                             hooks         = new WeakHashMap<String, Object>();
  private long                                                  recordingFrom = 0;
  private long                                                  recordingTo   = Long.MAX_VALUE;

  public OProfilerData() {
    recordingFrom = System.currentTimeMillis();
  }

  public void clear() {
    counters.clear();
    chronos.clear();
    stats.clear();
    hooks.clear();
    tips.clear();
    tipsTimestamp.clear();
  }

  public void clear(final String iFilter) {
    List<String> names = new ArrayList<String>(hooks.keySet());
    Collections.sort(names);
    for (String k : names) {
      if (iFilter != null && !k.startsWith(iFilter))
        // APPLIED FILTER: DOESN'T MATCH
        continue;

      hooks.remove(k);
    }

    // CHRONOS
    names = new ArrayList<String>(chronos.keySet());
    Collections.sort(names);
    for (String k : names) {
      if (iFilter != null && !k.startsWith(iFilter))
        // APPLIED FILTER: DOESN'T MATCH
        continue;

      chronos.remove(k);
    }

    // STATISTICS
    names = new ArrayList<String>(stats.keySet());
    Collections.sort(names);
    for (String k : names) {
      if (iFilter != null && !k.startsWith(iFilter))
        // APPLIED FILTER: DOESN'T MATCH
        continue;
      stats.remove(k);
    }

    // COUNTERS
    names = new ArrayList<String>(counters.keySet());
    Collections.sort(names);
    for (String k : names) {
      if (iFilter != null && !k.startsWith(iFilter))
        // APPLIED FILTER: DOESN'T MATCH
        continue;

      counters.remove(k);
    }
  }

  public long endRecording() {
    recordingTo = System.currentTimeMillis();
    return recordingTo;
  }

  public void toJSON(final StringBuilder buffer, final String iFilter) {
    buffer.append("{");
    buffer.append(String.format(Locale.ENGLISH, "\"from\": %d,", recordingFrom));
    buffer.append(String.format(Locale.ENGLISH, "\"to\": %d,", recordingTo));

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
        buffer.append(String.format(Locale.ENGLISH, "\"%s\":null", OIOUtils.encode(k)));
      else if (value instanceof Number)
        buffer.append(String.format(Locale.ENGLISH, "\"%s\":%d", OIOUtils.encode(k), value));
      else if (value instanceof Boolean)
        buffer.append(String.format(Locale.ENGLISH, "\"%s\":%s", OIOUtils.encode(k), value));
      else {
        buffer.append(String.format(Locale.ENGLISH, "\"%s\":\"%s\"", OIOUtils.encode(k), OIOUtils.encode(value)));
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

      buffer.append(String.format(Locale.ENGLISH, "\"%s\":", OIOUtils.encode(k)));
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
      buffer.append(String.format(Locale.ENGLISH, "\"%s\":", OIOUtils.encode(k)));
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
      buffer.append(String.format(Locale.ENGLISH, "\"%s\":%d", OIOUtils.encode(k), counters.get(k)));
    }
    buffer.append("}");

    buffer.append(",\"tips\":{");

    firstItem = true;
    for (String s : tips.keySet()) {
      if (firstItem)
        firstItem = false;
      else
        buffer.append(',');
      buffer.append(String.format(Locale.ENGLISH, "\"%s\": { \"time\" : %d , \"count\" : %d } ", OIOUtils.encode(s),
          tipsTimestamp.get(s), tips.get(s).get()));
    }

    buffer.append("}");

    buffer.append("}");
  }

  public String dump() {
    final StringBuilder buffer = new StringBuilder(OEnterpriseProfiler.BUFFER_SIZE);
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

    Long oldValue;
    Long newValue;
    do {
      oldValue = counters.get(iStatName);

      if (oldValue == null) {
        counters.putIfAbsent(iStatName, 0L);
        oldValue = counters.get(iStatName);
      }

      newValue = oldValue + iPlus;
    } while (!counters.replace(iStatName, oldValue, newValue));
  }

  public long getCounter(final String iStatName) {
    if (iStatName == null)
      return -1;

    final Long stat = counters.get(iStatName);
    if (stat == null)
      return -1;

    return stat.longValue();
  }

  public String dumpCounters() {
    final StringBuilder buffer = new StringBuilder(OEnterpriseProfiler.BUFFER_SIZE);
    buffer.append("Dumping COUNTERS:");

    buffer
        .append(String.format(Locale.ENGLISH, "\n%50s +-------------------------------------------------------------------+", ""));
    buffer.append(String.format(Locale.ENGLISH, "\n%50s | Value                                                             |",
        "Name"));
    buffer
        .append(String.format(Locale.ENGLISH, "\n%50s +-------------------------------------------------------------------+", ""));

    final List<String> keys = new ArrayList<String>(counters.keySet());
    Collections.sort(keys);

    for (String k : keys) {
      final Long stat = counters.get(k);
      buffer.append(String.format(Locale.ENGLISH, "\n%-50s | %-65d |", k, stat));
    }
    buffer
        .append(String.format(Locale.ENGLISH, "\n%50s +-------------------------------------------------------------------+", ""));
    return buffer.toString();
  }

  public long stopChrono(final String iName, final long iStartTime, final String iPayload, String user) {
    return updateEntry(chronos, iName, System.currentTimeMillis() - iStartTime, iPayload, user);
  }

  public String dumpChronos() {
    return dumpEntries(chronos, new StringBuilder("Dumping CHRONOS. Times in ms:"));
  }

  public long updateStat(final String iName, final long iValue) {
    return updateEntry(stats, iName, iValue, null, null);
  }

  public String dumpStats() {
    return dumpEntries(stats, new StringBuilder("Dumping STATISTICS. Times in ms:"));
  }

  public String dumpHookValues() {
    final StringBuilder buffer = new StringBuilder(OEnterpriseProfiler.BUFFER_SIZE);

    synchronized (hooks) {
      if (hooks.size() == 0)
        return "";

      buffer.append("Dumping HOOK VALUES:");

      buffer.append(String.format(Locale.ENGLISH, "\n%50s +-------------------------------------------------------------------+",
          ""));
      buffer.append(String.format(Locale.ENGLISH, "\n%50s | Value                                                             |",
          "Name"));
      buffer.append(String.format(Locale.ENGLISH, "\n%50s +-------------------------------------------------------------------+",
          ""));

      final List<String> names = new ArrayList<String>(hooks.keySet());
      Collections.sort(names);

      for (String k : names) {
        final Object hookValue = hooks.get(k);
        buffer.append(String.format(Locale.ENGLISH, "\n%-50s | %-65s |", k, hookValue != null ? hookValue.toString() : "null"));
      }
    }

    buffer
        .append(String.format(Locale.ENGLISH, "\n%50s +-------------------------------------------------------------------+", ""));
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
    return getMetricAsString(counters);
  }

  public String[] getChronosAsString() {
    return getMetricAsString(chronos);
  }

  public String[] getStatsAsString() {
    return getMetricAsString(stats);
  }

  public List<String> getCounters() {
    final List<String> list = new ArrayList<String>(counters.keySet());
    Collections.sort(list);
    return list;
  }

  public List<String> getHooks() {
    synchronized (hooks) {
      final List<String> list = new ArrayList<String>(hooks.keySet());
      Collections.sort(list);
      return list;
    }
  }

  public List<String> getChronos() {
    final List<String> list = new ArrayList<String>(chronos.keySet());
    Collections.sort(list);
    return list;
  }

  public List<String> getStats() {
    final List<String> list = new ArrayList<String>(stats.keySet());
    Collections.sort(list);
    return list;
  }

  public OProfilerEntry getStat(final String iStatName) {
    if (iStatName == null)
      return null;

    return stats.get(iStatName);
  }

  public OProfilerEntry getChrono(final String iChronoName) {
    if (iChronoName == null)
      return null;

    return chronos.get(iChronoName);
  }

  public long getRecordingFrom() {
    return recordingFrom;
  }

  public void setTip(String iMessage, AtomicInteger counter) {
    tips.put(iMessage, counter);
    tipsTimestamp.put(iMessage, System.currentTimeMillis());
  }

  public AtomicInteger getTip(final String iMessage) {
    return tips.get(iMessage);
  }

  protected synchronized long updateEntry(final ConcurrentMap<String, OProfilerEntry> iValues, final String iName,
      final long iValue, final String iPayload, String user) {

    OProfilerEntry c = iValues.get(iName);
    if (c == null) {
      // CREATE NEW CHRONO
      c = new OProfilerEntry();
      final OProfilerEntry oldValue = iValues.putIfAbsent(iName, c);
      if (oldValue != null)
        c = oldValue;
    }

    c.name = iName;
    c.payLoad = iPayload;
    c.entries++;
    c.last = iValue;
    c.total += c.last;
    c.average = c.total / c.entries;
    if (user != null)
      c.users.add(user);
    if (c.last < c.min)
      c.min = c.last;

    if (c.last > c.max)
      c.max = c.last;

    c.updateLastExecution();
    return c.last;
  }

  protected synchronized String dumpEntries(final ConcurrentMap<String, OProfilerEntry> iValues, final StringBuilder iBuffer) {
    // CHECK IF CHRONOS ARE ACTIVED
    if (iValues.size() == 0)
      return "";

    OProfilerEntry c;

    iBuffer.append(String
        .format(Locale.ENGLISH, "\n%50s +-------------------------------------------------------------------+", ""));
    iBuffer.append(String.format(Locale.ENGLISH, "\n%50s | %10s %10s %10s %10s %10s %10s |", "Name", "last", "total", "min", "max",
        "average", "items"));
    iBuffer.append(String
        .format(Locale.ENGLISH, "\n%50s +-------------------------------------------------------------------+", ""));

    final List<String> keys = new ArrayList<String>(iValues.keySet());
    Collections.sort(keys);

    for (String k : keys) {
      c = iValues.get(k);
      if (c != null)
        iBuffer.append(String.format(Locale.ENGLISH, "\n%-50s | %10d %10d %10d %10d %7.2f %10d |", k, c.last, c.total, c.min,
            c.max, c.average, c.entries));
    }
    iBuffer.append(String
        .format(Locale.ENGLISH, "\n%50s +-------------------------------------------------------------------+", ""));
    return iBuffer.toString();
  }

  protected String[] getMetricAsString(final ConcurrentMap<String, ?> iMetrics) {
    final List<String> output = new ArrayList<String>(iMetrics.size());
    for (Entry<String, ?> entry : iMetrics.entrySet()) {
      output.add(entry.getKey() + ": " + entry.getValue().toString());
    }
    return output.toArray(new String[output.size()]);
  }
}
