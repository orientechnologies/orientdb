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
import java.util.Timer;
import java.util.TimerTask;
import java.util.WeakHashMap;

import com.orientechnologies.common.concur.resource.OSharedResourceAbstract;
import com.orientechnologies.common.profiler.OProfilerData.OProfilerEntry;

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
  protected long                            recordingFrom = -1;
  protected Map<OProfilerHookValue, String> hooks         = new WeakHashMap<OProfiler.OProfilerHookValue, String>();
  protected Date                            lastReset     = new Date();

  protected OProfilerData                   realTime      = new OProfilerData();
  protected OProfilerData                   lastSnapshot  = new OProfilerData();
  protected List<OProfilerData>             snapshots     = new ArrayList<OProfilerData>();
  protected List<OProfilerData>             summaries     = new ArrayList<OProfilerData>();

  protected int                             elapsedToCreateSnapshot;
  protected int                             maxSnapshots;
  protected int                             maxSummaries;
  protected final static Timer              timer         = new Timer(true);

  protected static final OProfiler          instance      = new OProfiler();

  public interface OProfilerHookValue {
    public Object getValue();
  }

  public OProfiler() {
    // SNAPSHOT EVERY MINUTE, A SUMMARY EVERY HOUR UP TO 48 HOURS
    this(60000, 60, 48);
  }

  public OProfiler(final String iRecording) {
    if (iRecording.equalsIgnoreCase("true"))
      startRecording();
  }

  public OProfiler(final int iElapsedToCreateSnapshot, final int iMaxSnapshot, final int iMaxSumaries) {
    elapsedToCreateSnapshot = iElapsedToCreateSnapshot;
    maxSnapshots = iMaxSnapshot;
    maxSummaries = iMaxSumaries;
  }

  public void startRecording() {
    if (recordingFrom > -1)
      return;

    acquireExclusiveLock();
    try {

      timer.schedule(new TimerTask() {
        @Override
        public void run() {
          createSnapshot();
        }
      }, elapsedToCreateSnapshot, elapsedToCreateSnapshot);

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

      timer.cancel();
      recordingFrom = -1;

    } finally {
      releaseExclusiveLock();
    }
  }

  public boolean isRecording() {
    return recordingFrom > -1;
  }

  public void createSnapshot() {
    final Map<String, Object> hookValuesSnapshots = archiveHooks();

    acquireExclusiveLock();
    try {

      synchronized (snapshots) {
        // ARCHIVE IT
        lastSnapshot.setHookValues(hookValuesSnapshots);
        lastSnapshot.endRecording();
        snapshots.add(lastSnapshot);

        lastSnapshot = new OProfilerData();

        if (snapshots.size() >= maxSnapshots) {
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

  public void updateCounter(final String iStatName, final long iPlus) {
    if (iStatName == null || recordingFrom < 0)
      return;

    acquireSharedLock();
    try {
      lastSnapshot.updateCounter(iStatName, iPlus);
      realTime.updateCounter(iStatName, iPlus);
    } finally {
      releaseSharedLock();
    }
  }

  public long getCounter(final String iStatName) {
    if (iStatName == null || recordingFrom < 0)
      return -1;

    acquireSharedLock();
    try {
      return lastSnapshot.getCounter(iStatName);
    } finally {
      releaseSharedLock();
    }
  }

  public String toJSON(final String iQuery, final String iFrom, final String iTo) {
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
        realTime.toJSON(buffer);

      } else if (iQuery.equals("last")) {
        lastSnapshot.toJSON(buffer);

      } else {
        // GET THE RANGES
        if (iFrom == null || iTo == null)
          throw new IllegalArgumentException("Invalid range format. Use: <from> <to>, where * means any");

        final long from = iFrom.equals("*") ? 0 : Long.parseLong(iFrom);
        final long to = iTo.equals("*") ? Long.MAX_VALUE : Long.parseLong(iTo);

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

              a.toJSON(buffer);
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

              a.toJSON(buffer);
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

  public long stopChrono(final String iName, final long iStartTime) {
    acquireSharedLock();
    try {

      realTime.stopChrono(iName, iStartTime);
      return lastSnapshot.stopChrono(iName, iStartTime);

    } finally {
      releaseSharedLock();
    }
  }

  public long updateStat(final String iName, final long iValue) {
    acquireSharedLock();
    try {

      realTime.updateStat(iName, iValue);
      return lastSnapshot.updateStat(iName, iValue);

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
      return lastSnapshot.dumpCounters();
    } finally {
      releaseSharedLock();
    }
  }

  public String dumpChronos() {
    acquireSharedLock();
    try {
      return lastSnapshot.dumpChronos();
    } finally {
      releaseSharedLock();
    }
  }

  public String dumpStats() {
    acquireSharedLock();
    try {
      return lastSnapshot.dumpStats();
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

      synchronized (hooks) {
        if (hooks.size() == 0)
          return "";

        buffer.append("HOOK VALUES:");

        buffer.append(String.format("\n%50s +-------------------------------------------------------------------+", ""));
        buffer.append(String.format("\n%50s | Value                                                             |", "Name"));
        buffer.append(String.format("\n%50s +-------------------------------------------------------------------+", ""));

        final List<String> names = new ArrayList<String>(hooks.values());
        Collections.sort(names);

        for (String k : names) {
          for (Map.Entry<OProfilerHookValue, String> v : hooks.entrySet()) {
            if (v.getValue().equals(k)) {
              final OProfilerHookValue hook = v.getKey();
              if (hook != null) {
                final Object hookValue = hook.getValue();
                buffer.append(String.format("\n%-50s | %-65s |", k, hookValue != null ? hookValue.toString() : "null"));
              }
              break;
            }
          }
        }
      }

    } finally {
      releaseSharedLock();
    }

    buffer.append(String.format("\n%50s +-------------------------------------------------------------------+", ""));
    return buffer.toString();
  }

  public Object getHookValue(final String iName) {
    synchronized (hooks) {
      for (Map.Entry<OProfilerHookValue, String> v : hooks.entrySet()) {
        if (v.getValue().equals(iName)) {
          final OProfilerHookValue h = v.getKey();
          if (h != null)
            return h.getValue();
        }
      }
    }
    return null;
  }

  public String[] getCountersAsString() {
    acquireSharedLock();
    try {
      return lastSnapshot.getCountersAsString();
    } finally {
      releaseSharedLock();
    }
  }

  public String[] getChronosAsString() {
    acquireSharedLock();
    try {
      return lastSnapshot.getChronosAsString();
    } finally {
      releaseSharedLock();
    }
  }

  public String[] getStatsAsString() {
    acquireSharedLock();
    try {
      return lastSnapshot.getStatsAsString();
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
      return lastSnapshot.getCounters();
    } finally {
      releaseSharedLock();
    }
  }

  public OProfilerEntry getStat(final String iStatName) {
    acquireSharedLock();
    try {
      return lastSnapshot.getStat(iStatName);
    } finally {
      releaseSharedLock();
    }
  }

  public OProfilerEntry getChrono(final String iChronoName) {
    acquireSharedLock();
    try {
      return lastSnapshot.getChrono(iChronoName);
    } finally {
      releaseSharedLock();
    }
  }

  public void registerHookValue(final String iName, final OProfilerHookValue iHookValue) {
    if (recordingFrom < 0)
      return;

    synchronized (hooks) {
      for (Map.Entry<OProfilerHookValue, String> entry : hooks.entrySet()) {
        if (entry.getValue().equals(iName)) {
          hooks.remove(entry.getKey());
          break;
        }
      }

      hooks.put(iHookValue, iName);
    }
  }

  public void unregisterHookValue(final String iName) {
    if (recordingFrom < 0)
      return;

    synchronized (hooks) {
      for (Map.Entry<OProfilerHookValue, String> entry : hooks.entrySet()) {
        if (entry.getValue().equals(iName)) {
          hooks.remove(entry.getKey());
          break;
        }
      }
    }
  }

  public static OProfiler getInstance() {
    return instance;
  }

  public void setAutoDump(final int iSeconds) {
    if (timer != null)
      timer.cancel();

    if (iSeconds > 0) {
      final int ms = iSeconds * 1000;

      timer.scheduleAtFixedRate(new TimerTask() {

        @Override
        public void run() {
          System.out.println(OProfiler.getInstance().dump());
        }
      }, ms, ms);
    }
  }

  /**
   * Must be called inside a lock.
   */
  protected Map<String, Object> archiveHooks() {
    final Map<String, Object> result = new HashMap<String, Object>();

    final Map<OProfilerHookValue, String> copy = new HashMap<OProfiler.OProfilerHookValue, String>();
    synchronized (hooks) {
      copy.putAll(hooks);
    }

    for (Map.Entry<OProfilerHookValue, String> v : copy.entrySet())
      result.put(v.getValue(), v.getKey().getValue());

    return result;
  }
}
