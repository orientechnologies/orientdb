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
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import com.orientechnologies.common.concur.resource.OSharedResourceAbstract;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.profiler.OProfilerData.OProfilerEntry;
import com.orientechnologies.common.util.OPair;

/**
 * Profiling utility class. Handles chronos (times), statistics and counters. By default it's used as Singleton but you can create
 * any instances you want for separate profiling contexts.
 * 
 * To start the recording use call startRecording(). By default record is turned off to avoid a run-time execution cost.
 * 
 * @author Luca Garulli
 * @copyrights Orient Technologies.com
 */
public class OProfiler extends OSharedResourceAbstract implements OProfilerMBean {
  public enum METRIC_TYPE {
    CHRONO, COUNTER, STAT, SIZE, ENABLED, TEXT
  }

  protected long                                   recordingFrom           = -1;
  protected Map<String, OProfilerHookValue>        hooks                   = new ConcurrentHashMap<String, OProfilerHookValue>();
  protected Date                                   lastReset               = new Date();

  protected ConcurrentHashMap<String, String>      dictionary              = new ConcurrentHashMap<String, String>();
  protected ConcurrentHashMap<String, METRIC_TYPE> types                   = new ConcurrentHashMap<String, METRIC_TYPE>();
  protected OProfilerData                          realTime                = new OProfilerData();
  protected OProfilerData                          lastSnapshot;
  protected List<OProfilerData>                    snapshots               = new ArrayList<OProfilerData>();
  protected List<OProfilerData>                    summaries               = new ArrayList<OProfilerData>();

  protected int                                    elapsedToCreateSnapshot = 0;
  protected int                                    maxSnapshots            = 0;
  protected int                                    maxSummaries            = 0;
  protected final static Timer                     timer                   = new Timer(true);
  protected TimerTask                              archiverTask;

  public interface OProfilerHookValue {
    public Object getValue();
  }

  public OProfiler() {
  }

  public OProfiler(final int iElapsedToCreateSnapshot, final int iMaxSnapshot, final int iMaxSumaries) {
    elapsedToCreateSnapshot = iElapsedToCreateSnapshot;
    maxSnapshots = iMaxSnapshot;
    maxSummaries = iMaxSumaries;
  }

  public void configure(final String iConfiguration) {
    if (iConfiguration == null || iConfiguration.length() == 0)
      return;

    final String[] parts = iConfiguration.split(",");
    elapsedToCreateSnapshot = Integer.parseInt(parts[0].trim());
    maxSnapshots = Integer.parseInt(parts[1].trim());
    maxSummaries = Integer.parseInt(parts[2].trim());

    if (isRecording())
      stopRecording();

    startRecording();
  }

  /**
   * Frees the memory removing profiling information
   */
  public void memoryUsageLow(final long iFreeMemory, final long iFreeMemoryPercentage) {
    synchronized (snapshots) {
      snapshots.clear();
    }
    synchronized (summaries) {
      summaries.clear();
    }
  }

  public void shutdown() {
    stopRecording();
    hooks.clear();

    synchronized (snapshots) {
      snapshots.clear();
    }
    synchronized (summaries) {
      summaries.clear();
    }
  }

  public void startRecording() {
    if (recordingFrom > -1)
      return;

    acquireExclusiveLock();
    try {
      OLogManager.instance().info(this, "Profiler is recording metrics with configuration: %d,%d,%d", elapsedToCreateSnapshot,
          maxSnapshots, maxSummaries);

      if (elapsedToCreateSnapshot > 0) {
        lastSnapshot = new OProfilerData();

        if (archiverTask != null)
          archiverTask.cancel();

        archiverTask = new TimerTask() {
          @Override
          public void run() {
            createSnapshot();
          }
        };
        timer.schedule(archiverTask, elapsedToCreateSnapshot * 1000, elapsedToCreateSnapshot * 1000);
      }

      recordingFrom = System.currentTimeMillis();

    } finally {
      releaseExclusiveLock();
    }
  }

  public void stopRecording() {
    if (recordingFrom == -1)
      return;

    acquireExclusiveLock();
    try {
      OLogManager.instance().config(this, "Profiler has stopped recording metrics");

      lastSnapshot = null;
      realTime.clear();
      dictionary.clear();
      types.clear();

      if (archiverTask != null)
        archiverTask.cancel();

      recordingFrom = -1;

    } finally {
      releaseExclusiveLock();
    }
  }

  public boolean isRecording() {
    return recordingFrom > -1;
  }

  public void createSnapshot() {
    if (lastSnapshot == null)
      return;

    final Map<String, Object> hookValuesSnapshots = archiveHooks();

    acquireExclusiveLock();
    try {

      synchronized (snapshots) {
        // ARCHIVE IT
        lastSnapshot.setHookValues(hookValuesSnapshots);
        lastSnapshot.endRecording();
        snapshots.add(lastSnapshot);

        lastSnapshot = new OProfilerData();

        if (snapshots.size() >= maxSnapshots && maxSnapshots > 0) {
          // COPY ALL THE ARCHIVE AND RESET IT

          // MERGE RESULTS IN A SUMMARY AND COLLECT IT
          synchronized (summaries) {
            final OProfilerData summary = new OProfilerData();
            for (OProfilerData a : snapshots) {
              summary.mergeWith(a);
            }
            summaries.add(summary);

            if (summaries.size() > maxSummaries)
              // REMOVE THE FIRST OLDEST
              summaries.remove(0);

          }
          snapshots.clear();
        }
      }

    } finally {
      releaseExclusiveLock();
    }
  }

  public void updateCounter(final String iName, final String iDescription, final long iPlus) {
    updateCounter(iName, iDescription, iPlus, iName);
  }

  public void updateCounter(final String iName, final String iDescription, final long iPlus, final String iMetadata) {
    if (iName == null || recordingFrom < 0)
      return;

    updateMetadata(iMetadata, iDescription, METRIC_TYPE.COUNTER);

    acquireSharedLock();
    try {
      if (lastSnapshot != null)
        lastSnapshot.updateCounter(iName, iPlus);
      realTime.updateCounter(iName, iPlus);
    } finally {
      releaseSharedLock();
    }
  }

  public long getCounter(final String iStatName) {
    if (iStatName == null || recordingFrom < 0)
      return -1;

    acquireSharedLock();
    try {
      return realTime.getCounter(iStatName);
    } finally {
      releaseSharedLock();
    }
  }

  public String toJSON(final String iQuery, final String iPar1, final String iPar2) {
    final StringBuilder buffer = new StringBuilder();

    Map<String, Object> hookValuesSnapshots = null;
    if (iQuery.equals("realtime"))
      // GET LATETS HOOK VALUES
      hookValuesSnapshots = archiveHooks();

    buffer.append("{ \"" + iQuery + "\":");

    acquireSharedLock();
    try {
      if (iQuery.equals("realtime")) {
        realTime.setHookValues(hookValuesSnapshots);
        realTime.toJSON(buffer, iPar1);

      } else if (iQuery.equals("last")) {
        if (lastSnapshot != null)
          lastSnapshot.toJSON(buffer, iPar1);

      } else {
        // GET THE RANGES
        if (iPar1 == null || iPar2 == null)
          throw new IllegalArgumentException("Invalid range format. Use: <from> <to>, where * means any");

        final long from = iPar1.equals("*") ? 0 : Long.parseLong(iPar1);
        final long to = iPar2.equals("*") ? Long.MAX_VALUE : Long.parseLong(iPar2);

        boolean firstItem = true;
        buffer.append("[");
        if (iQuery.equals("archive")) {
          // ARCHIVE
          for (int i = 0; i < snapshots.size(); ++i) {
            final OProfilerData a = snapshots.get(i);
            if (a.isInRange(from, to)) {
              if (firstItem)
                firstItem = false;
              else
                buffer.append(',');

              a.toJSON(buffer, null);
            }
          }
        } else if (iQuery.equals("summary")) {
          // SUMMARY
          for (int i = 0; i < summaries.size(); ++i) {
            final OProfilerData a = summaries.get(i);
            if (a.isInRange(from, to)) {
              if (firstItem)
                firstItem = false;
              else
                buffer.append(',');

              a.toJSON(buffer, iPar1);
            }
          }
        } else
          throw new IllegalArgumentException("Invalid archive query: use realtime|last|archive|summary");

        buffer.append("]");
      }

      buffer.append("}");

    } finally {
      releaseSharedLock();
    }

    return buffer.toString();
  }

  public Map<String, OPair<String, METRIC_TYPE>> getMetadata() {
    final Map<String, OPair<String, METRIC_TYPE>> metadata = new HashMap<String, OPair<String, METRIC_TYPE>>();
    for (Entry<String, String> entry : dictionary.entrySet())
      metadata.put(entry.getKey(), new OPair<String, METRIC_TYPE>(entry.getValue(), types.get(entry.getKey())));
    return metadata;
  }

  public String metadataToJSON() {
    final StringBuilder buffer = new StringBuilder();

    buffer.append("{ \"metadata\": {\n  ");
    boolean first = true;
    for (Entry<String, String> entry : dictionary.entrySet()) {
      final String key = entry.getKey();

      if (first)
        first = false;
      else
        buffer.append(",\n  ");
      buffer.append('"');
      buffer.append(key);
      buffer.append("\":{\"description\":\"");
      buffer.append(entry.getValue());
      buffer.append("\",\"type\":\"");
      buffer.append(types.get(key));
      buffer.append("\"}");
    }
    buffer.append("} }");

    return buffer.toString();
  }

  public String dump() {
    final float maxMem = Runtime.getRuntime().maxMemory() / 1000000f;
    final float totMem = Runtime.getRuntime().totalMemory() / 1000000f;
    final float freeMem = maxMem - totMem;

    final long now = System.currentTimeMillis();

    acquireSharedLock();
    try {

      final StringBuilder buffer = new StringBuilder();
      buffer.append("\nOrientDB profiler dump of ");
      buffer.append(new Date(now));
      buffer.append(" after ");
      buffer.append((now - recordingFrom) / 1000);
      buffer.append(" secs of profiling");
      buffer.append(String.format("\nFree memory: %2.2fMb (%2.2f%%) - Total memory: %2.2fMb - Max memory: %2.2fMb - CPUs: %d",
          freeMem, (freeMem * 100 / (float) maxMem), totMem, maxMem, Runtime.getRuntime().availableProcessors()));
      buffer.append("\n");
      buffer.append(dumpHookValues());
      buffer.append("\n");
      buffer.append(dumpCounters());
      buffer.append("\n\n");
      buffer.append(dumpStats());
      buffer.append("\n\n");
      buffer.append(dumpChronos());
      return buffer.toString();

    } finally {
      releaseSharedLock();
    }
  }

  public long startChrono() {
    // CHECK IF CHRONOS ARE ACTIVED
    if (recordingFrom < 0)
      return -1;

    return System.currentTimeMillis();
  }

  public long stopChrono(final String iName, final String iDescription, final long iStartTime) {
    return stopChrono(iName, iDescription, iStartTime, iName, null);
  }

  public long stopChrono(final String iName, final String iDescription, final long iStartTime, final String iDictionaryName) {
    return stopChrono(iName, iDescription, iStartTime, iDictionaryName, null);
  }

  public long stopChrono(final String iName, final String iDescription, final long iStartTime, final String iDictionaryName,
      final String iPayload) {
    // CHECK IF CHRONOS ARE ACTIVED
    if (recordingFrom < 0)
      return -1;

    updateMetadata(iDictionaryName, iDescription, METRIC_TYPE.CHRONO);

    acquireSharedLock();
    try {

      if (lastSnapshot != null)
        lastSnapshot.stopChrono(iName, iStartTime, iPayload);
      return realTime.stopChrono(iName, iStartTime, iPayload);

    } finally {
      releaseSharedLock();
    }
  }

  public long updateStat(final String iName, final String iDescription, final long iValue) {
    // CHECK IF CHRONOS ARE ACTIVED
    if (recordingFrom < 0)
      return -1;

    updateMetadata(iName, iDescription, METRIC_TYPE.STAT);

    acquireSharedLock();
    try {

      if (lastSnapshot != null)
        lastSnapshot.updateStat(iName, iValue);
      return realTime.updateStat(iName, iValue);

    } finally {
      releaseSharedLock();
    }
  }

  public String dumpCounters() {
    // CHECK IF STATISTICS ARE ACTIVED
    if (recordingFrom < 0)
      return "Counters: <no recording>";

    acquireSharedLock();
    try {
      return realTime.dumpCounters();
    } finally {
      releaseSharedLock();
    }
  }

  public String dumpChronos() {
    acquireSharedLock();
    try {
      return realTime.dumpChronos();
    } finally {
      releaseSharedLock();
    }
  }

  public String dumpStats() {
    acquireSharedLock();
    try {
      return realTime.dumpStats();
    } finally {
      releaseSharedLock();
    }
  }

  public String dumpHookValues() {
    if (recordingFrom < 0)
      return "HookValues: <no recording>";

    final StringBuilder buffer = new StringBuilder();

    acquireSharedLock();
    try {

      if (hooks.size() == 0)
        return "";

      buffer.append("HOOK VALUES:");

      buffer.append(String.format("\n%50s +-------------------------------------------------------------------+", ""));
      buffer.append(String.format("\n%50s | Value                                                             |", "Name"));
      buffer.append(String.format("\n%50s +-------------------------------------------------------------------+", ""));

      final List<String> names = new ArrayList<String>(hooks.keySet());
      Collections.sort(names);

      for (String k : names) {
        final OProfilerHookValue v = hooks.get(k);
        if (v != null) {
          final Object hookValue = v.getValue();
          buffer.append(String.format("\n%-50s | %-65s |", k, hookValue != null ? hookValue.toString() : "null"));
        }
      }

    } finally {
      releaseSharedLock();
    }

    buffer.append(String.format("\n%50s +-------------------------------------------------------------------+", ""));
    return buffer.toString();
  }

  public Object getHookValue(final String iName) {
    final OProfilerHookValue v = hooks.get(iName);
    return v != null ? v.getValue() : null;
  }

  public String[] getCountersAsString() {
    acquireSharedLock();
    try {
      return realTime.getCountersAsString();
    } finally {
      releaseSharedLock();
    }
  }

  public String[] getChronosAsString() {
    acquireSharedLock();
    try {
      return realTime.getChronosAsString();
    } finally {
      releaseSharedLock();
    }
  }

  public String[] getStatsAsString() {
    acquireSharedLock();
    try {
      return realTime.getStatsAsString();
    } finally {
      releaseSharedLock();
    }
  }

  public Date getLastReset() {
    return lastReset;
  }

  public List<String> getCounters() {
    acquireSharedLock();
    try {
      return realTime.getCounters();
    } finally {
      releaseSharedLock();
    }
  }

  public OProfilerEntry getStat(final String iStatName) {
    acquireSharedLock();
    try {
      return realTime.getStat(iStatName);
    } finally {
      releaseSharedLock();
    }
  }

  public OProfilerEntry getChrono(final String iChronoName) {
    acquireSharedLock();
    try {
      return realTime.getChrono(iChronoName);
    } finally {
      releaseSharedLock();
    }
  }

  public void registerHookValue(final String iName, final String iDescription, final METRIC_TYPE iType,
      final OProfilerHookValue iHookValue) {
    registerHookValue(iName, iDescription, iType, iHookValue, iName);
  }

  public void registerHookValue(final String iName, final String iDescription, final METRIC_TYPE iType,
      final OProfilerHookValue iHookValue, final String iMetadataName) {
    unregisterHookValue(iName);
    updateMetadata(iMetadataName, iDescription, iType);
    hooks.put(iName, iHookValue);
  }

  public void unregisterHookValue(final String iName) {
    if (recordingFrom < 0)
      return;

    hooks.remove(iName);
  }

  public void setAutoDump(final int iSeconds) {
    if (iSeconds > 0) {
      final int ms = iSeconds * 1000;

      timer.schedule(new TimerTask() {

        @Override
        public void run() {
          System.out.println(dump());
        }
      }, ms, ms);
    }
  }

  /**
   * Must be not called inside a lock.
   */
  protected Map<String, Object> archiveHooks() {
    if (!isRecording())
      return null;

    final Map<String, Object> result = new HashMap<String, Object>();

    for (Map.Entry<String, OProfilerHookValue> v : hooks.entrySet())
      result.put(v.getKey(), v.getValue().getValue());

    return result;
  }

  /**
   * Updates the metric metadata.
   */
  protected void updateMetadata(final String iName, final String iDescription, final METRIC_TYPE iType) {
    if (iDescription != null && dictionary.putIfAbsent(iName, iDescription) == null)
      types.put(iName, iType);
  }
}
