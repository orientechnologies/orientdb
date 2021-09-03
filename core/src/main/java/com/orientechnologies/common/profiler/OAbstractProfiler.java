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

import com.orientechnologies.common.concur.resource.OSharedResourceAbstract;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.OOrientStartupListener;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.cache.OReadCache;
import com.orientechnologies.orient.core.storage.cache.OWriteCache;
import com.orientechnologies.orient.core.storage.disk.OLocalPaginatedStorage;
import java.io.File;
import java.io.PrintStream;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import javax.management.MBeanServer;
import javax.management.ObjectName;

public abstract class OAbstractProfiler extends OSharedResourceAbstract
    implements OProfiler, OOrientStartupListener, OProfilerMXBean {

  protected final Map<String, OProfilerHookRuntime> hooks =
      new ConcurrentHashMap<String, OProfilerHookRuntime>();
  protected final ConcurrentHashMap<String, String> dictionary =
      new ConcurrentHashMap<String, String>();
  protected final ConcurrentHashMap<String, METRIC_TYPE> types =
      new ConcurrentHashMap<String, METRIC_TYPE>();
  protected long recordingFrom = -1;
  protected TimerTask autoDumpTask;
  protected List<OProfilerListener> listeners = new ArrayList<OProfilerListener>();

  private static long statsCreateRecords = 0;
  private static long statsReadRecords = 0;
  private static long statsUpdateRecords = 0;
  private static long statsDeleteRecords = 0;
  private static long statsCommands = 0;
  private static long statsTxCommit = 0;
  private static long statsTxRollback = 0;
  private static long statsLastAutoDump = 0;

  public interface OProfilerHookValue {
    Object getValue();
  }

  public class OProfilerHookRuntime {
    public OProfilerHookValue hook;
    public METRIC_TYPE type;

    public OProfilerHookRuntime(final OProfilerHookValue hook, final METRIC_TYPE type) {
      this.hook = hook;
      this.type = type;
    }
  }

  public class OProfilerHookStatic {
    public Object value;
    public METRIC_TYPE type;

    public OProfilerHookStatic(final Object value, final METRIC_TYPE type) {
      this.value = value;
      this.type = type;
    }
  }

  private static final class MemoryChecker implements Runnable {
    @Override
    public void run() {
      try {
        final long jvmTotMemory = Runtime.getRuntime().totalMemory();
        final long jvmMaxMemory = Runtime.getRuntime().maxMemory();

        for (OStorage s : Orient.instance().getStorages()) {
          if (s instanceof OLocalPaginatedStorage) {
            final OReadCache dk = ((OLocalPaginatedStorage) s).getReadCache();
            final OWriteCache wk = ((OLocalPaginatedStorage) s).getWriteCache();
            if (dk == null || wk == null)
              // NOT YET READY
              continue;

            final long totalDiskCacheUsedMemory =
                (dk.getUsedMemory() + wk.getExclusiveWriteCachePagesSize()) / OFileUtils.MEGABYTE;
            final long maxDiskCacheUsedMemory =
                OGlobalConfiguration.DISK_CACHE_SIZE.getValueAsLong();

            // CHECK IF THERE IS MORE THAN 40% HEAP UNUSED AND DISK-CACHE IS 80% OF THE MAXIMUM SIZE
            if ((jvmTotMemory * 140 / 100) < jvmMaxMemory
                && (totalDiskCacheUsedMemory * 120 / 100) > maxDiskCacheUsedMemory) {

              final long suggestedMaxHeap = jvmTotMemory * 120 / 100;
              final long suggestedDiskCache =
                  OGlobalConfiguration.DISK_CACHE_SIZE.getValueAsLong()
                      + (jvmMaxMemory - suggestedMaxHeap) / OFileUtils.MEGABYTE;

              OLogManager.instance()
                  .info(
                      this,
                      "Database '%s' uses %,dMB/%,dMB of DISKCACHE memory, while Heap is not completely used (usedHeap=%dMB maxHeap=%dMB). To improve performance set maxHeap to %dMB and DISKCACHE to %dMB",
                      s.getName(),
                      totalDiskCacheUsedMemory,
                      maxDiskCacheUsedMemory,
                      jvmTotMemory / OFileUtils.MEGABYTE,
                      jvmMaxMemory / OFileUtils.MEGABYTE,
                      suggestedMaxHeap / OFileUtils.MEGABYTE,
                      suggestedDiskCache);

              OLogManager.instance()
                  .info(
                      this,
                      "-> Open server.sh (or server.bat on Windows) and change the following variables: 1) MAXHEAP=-Xmx%dM 2) MAXDISKCACHE=%d",
                      suggestedMaxHeap / OFileUtils.MEGABYTE,
                      suggestedDiskCache);
            }
          }
        }
      } catch (Exception e) {
        OLogManager.instance().debug(this, "Error on memory checker task", e);
      } catch (Error e) {
        OLogManager.instance().debug(this, "Error on memory checker task", e);
        throw e;
      }
    }
  }

  public OAbstractProfiler() {
    this(true);
  }

  public OAbstractProfiler(boolean registerListener) {
    if (registerListener) {
      Orient.instance().registerWeakOrientStartupListener(this);
    }
  }

  public OAbstractProfiler(final OAbstractProfiler profiler) {
    hooks.putAll(profiler.hooks);
    dictionary.putAll(profiler.dictionary);
    types.putAll(profiler.types);

    Orient.instance().registerWeakOrientStartupListener(this);
  }

  protected abstract void setTip(String iMessage, AtomicInteger counter);

  protected abstract AtomicInteger getTip(String iMessage);

  public static String dumpEnvironment(final String dumpType) {
    final StringBuilder buffer = new StringBuilder();

    final Runtime runtime = Runtime.getRuntime();

    final long freeSpaceInMB = new File(".").getFreeSpace();
    final long totalSpaceInMB = new File(".").getTotalSpace();

    int stgs = 0;
    long diskCacheUsed = 0;
    long diskCacheTotal = 0;
    for (OStorage stg : Orient.instance().getStorages()) {
      if (stg instanceof OLocalPaginatedStorage) {
        diskCacheUsed += ((OLocalPaginatedStorage) stg).getReadCache().getUsedMemory();
        diskCacheTotal += OGlobalConfiguration.DISK_CACHE_SIZE.getValueAsLong() * 1024 * 1024;
        stgs++;
      }
    }
    try {
      MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
      ObjectName osMBeanName =
          ObjectName.getInstance(ManagementFactory.OPERATING_SYSTEM_MXBEAN_NAME);
      if (mbs.isInstanceOf(osMBeanName, "com.sun.management.OperatingSystemMXBean")) {
        final long osTotalMem =
            ((Number) mbs.getAttribute(osMBeanName, "TotalPhysicalMemorySize")).longValue();
        final long osUsedMem =
            osTotalMem
                - ((Number) mbs.getAttribute(osMBeanName, "FreePhysicalMemorySize")).longValue();

        buffer.append(
            String.format(
                "OrientDB Memory profiler: HEAP=%s of %s - DISKCACHE (%s dbs)=%s of %s - OS=%s of %s - FS=%s of %s",
                OFileUtils.getSizeAsString(runtime.totalMemory() - runtime.freeMemory()),
                OFileUtils.getSizeAsString(runtime.maxMemory()),
                stgs,
                OFileUtils.getSizeAsString(diskCacheUsed),
                OFileUtils.getSizeAsString(diskCacheTotal),
                OFileUtils.getSizeAsString(osUsedMem),
                OFileUtils.getSizeAsString(osTotalMem),
                OFileUtils.getSizeAsString(freeSpaceInMB),
                OFileUtils.getSizeAsString(totalSpaceInMB)));
      }

    } catch (Exception e) {
      // JMX NOT AVAILABLE, AVOID OS DATA
      buffer.append(
          String.format(
              "OrientDB Memory profiler: HEAP=%s of %s - DISKCACHE (%s dbs)=%s of %s - FS=%s of %s",
              OFileUtils.getSizeAsString(runtime.totalMemory() - runtime.freeMemory()),
              OFileUtils.getSizeAsString(runtime.maxMemory()),
              stgs,
              OFileUtils.getSizeAsString(diskCacheUsed),
              OFileUtils.getSizeAsString(diskCacheTotal),
              OFileUtils.getSizeAsString(freeSpaceInMB),
              OFileUtils.getSizeAsString(totalSpaceInMB)));
    }

    if ("performance".equalsIgnoreCase(dumpType)) {
      try {
        long lastCreateRecords = 0;
        long lastReadRecords = 0;
        long lastUpdateRecords = 0;
        long lastDeleteRecords = 0;
        long lastCommands = 0;
        long lastTxCommit = 0;
        long lastTxRollback = 0;

        if (statsLastAutoDump > 0) {
          final long msFromLastDump = System.currentTimeMillis() - statsLastAutoDump;

          final String[] hooks = Orient.instance().getProfiler().getHookAsString();
          for (String h : hooks) {
            if (h.startsWith("db.") && h.endsWith("createRecord"))
              lastCreateRecords += (Long) Orient.instance().getProfiler().getHookValue(h);
            else if (h.startsWith("db.") && h.endsWith("readRecord"))
              lastReadRecords += (Long) Orient.instance().getProfiler().getHookValue(h);
            else if (h.startsWith("db.") && h.endsWith("updateRecord"))
              lastUpdateRecords += (Long) Orient.instance().getProfiler().getHookValue(h);
            else if (h.startsWith("db.") && h.endsWith("deleteRecord"))
              lastDeleteRecords += (Long) Orient.instance().getProfiler().getHookValue(h);
            else if (h.startsWith("db.") && h.endsWith("txCommit"))
              lastTxCommit += (Long) Orient.instance().getProfiler().getHookValue(h);
            else if (h.startsWith("db.") && h.endsWith("txRollback"))
              lastTxRollback += (Long) Orient.instance().getProfiler().getHookValue(h);
          }

          final List<String> chronos = Orient.instance().getProfiler().getChronos();
          for (String c : chronos) {
            final OProfilerEntry chrono = Orient.instance().getProfiler().getChrono(c);
            if (chrono != null) {
              if (c.startsWith("db.") && c.contains(".command.")) lastCommands += chrono.entries;
            }
          }

          long lastCreateRecordsSec;
          if (lastCreateRecords == 0) {
            lastCreateRecordsSec = msFromLastDump < 1000 ? 1 : 0;
          } else {
            lastCreateRecordsSec =
                (lastCreateRecords - statsCreateRecords) / (msFromLastDump / 1000);
          }

          long lastReadRecordsSec;
          if (msFromLastDump < 1000) {
            lastReadRecordsSec = lastReadRecords == 0 ? 0 : 1;
          } else if (lastReadRecords == 0) {
            lastReadRecordsSec = 0;
          } else {
            lastReadRecordsSec = (lastReadRecords - statsReadRecords) / (msFromLastDump / 1000);
          }

          long lastUpdateRecordsSec;
          if (lastUpdateRecords == 0 || msFromLastDump < 1000) {
            lastUpdateRecordsSec = 0;
          } else {
            lastUpdateRecordsSec =
                (lastUpdateRecords - statsUpdateRecords) / (msFromLastDump / 1000);
          }

          long lastDeleteRecordsSec;
          if (lastDeleteRecords == 0) {
            lastDeleteRecordsSec = 0;
          } else if (msFromLastDump < 1000) {
            lastDeleteRecordsSec = 1;
          } else {
            lastDeleteRecordsSec =
                (lastDeleteRecords - statsDeleteRecords) / (msFromLastDump / 1000);
          }

          long lastCommandsSec;
          if (lastCommands == 0) {
            lastCommandsSec = 0;
          } else if (msFromLastDump < 1000) {
            lastCommandsSec = 1;
          } else {
            lastCommandsSec = (lastCommands - statsCommands) / (msFromLastDump / 1000);
          }

          long lastTxCommitSec;
          if (lastTxCommit == 0) {
            lastTxCommitSec = 0;
          } else if (msFromLastDump < 1000) {
            lastTxCommitSec = 1;
          } else {
            lastTxCommitSec = (lastTxCommit - statsTxCommit) / (msFromLastDump / 1000);
          }

          long lastTxRollbackSec;
          if (lastTxRollback == 0) {
            lastTxRollbackSec = 0;
          } else if (msFromLastDump < 1000) {
            lastTxRollbackSec = 1;
          } else {
            lastTxRollbackSec = (lastTxRollback - statsTxRollback) / (msFromLastDump / 1000);
          }

          buffer.append(
              String.format(
                  "\nCRUD: C(%d %d/sec) R(%d %d/sec) U(%d %d/sec) D(%d %d/sec) - COMMANDS (%d %d/sec) - TX: COMMIT(%d %d/sec) ROLLBACK(%d %d/sec)",
                  lastCreateRecords,
                  lastCreateRecordsSec,
                  lastReadRecords,
                  lastReadRecordsSec,
                  lastUpdateRecords,
                  lastUpdateRecordsSec,
                  lastDeleteRecords,
                  lastDeleteRecordsSec,
                  lastCommands,
                  lastCommandsSec,
                  lastTxCommit,
                  lastTxCommitSec,
                  lastTxRollback,
                  lastTxRollbackSec));
        }

        statsLastAutoDump = System.currentTimeMillis();
        statsCreateRecords = lastCreateRecords;
        statsReadRecords = lastReadRecords;
        statsUpdateRecords = lastUpdateRecords;
        statsDeleteRecords = lastDeleteRecords;
        statsCommands = lastCommands;
        statsTxCommit = lastTxCommit;
        statsTxRollback = lastTxRollback;

      } catch (Exception e) {
        // IGNORE IT
      }
    }

    return buffer.toString();
  }

  @Override
  public void onStartup() {
    if (OGlobalConfiguration.PROFILER_ENABLED.getValueAsBoolean())
      // ACTIVATE RECORDING OF THE PROFILER
      startRecording();
    installMemoryChecker();
  }

  public void shutdown() {
    stopRecording();
  }

  public int reportTip(final String iMessage) {
    AtomicInteger counter = getTip(iMessage);
    if (counter == null) {
      // DUMP THE MESSAGE ONLY THE FIRST TIME
      OLogManager.instance().info(this, "[TIP] " + iMessage);

      counter = new AtomicInteger(0);
    }

    setTip(iMessage, counter);

    return counter.incrementAndGet();
  }

  public boolean startRecording() {
    if (isRecording()) return false;

    recordingFrom = System.currentTimeMillis();
    return true;
  }

  public boolean stopRecording() {
    if (!isRecording()) return false;

    recordingFrom = -1;
    return true;
  }

  public boolean isRecording() {
    return recordingFrom > -1;
  }

  public void updateCounter(final String iStatName, final String iDescription, final long iPlus) {
    updateCounter(iStatName, iDescription, iPlus, iStatName);
  }

  @Override
  public String getName() {
    return "profiler";
  }

  @Override
  public void startup() {
    startRecording();
  }

  @Override
  public String dump() {
    return dumpEnvironment(OGlobalConfiguration.PROFILER_AUTODUMP_TYPE.getValueAsString());
  }

  @Override
  public void dump(final PrintStream out) {
    out.println(dumpEnvironment(OGlobalConfiguration.PROFILER_AUTODUMP_TYPE.getValueAsString()));
  }

  @Override
  public String dump(final String type) {
    return dumpEnvironment(type);
  }

  @Override
  public String dumpCounters() {
    return null;
  }

  @Override
  public OProfilerEntry getChrono(String string) {
    return null;
  }

  @Override
  public long startChrono() {
    return 0;
  }

  @Override
  public long stopChrono(String iName, String iDescription, long iStartTime) {
    return 0;
  }

  @Override
  public long stopChrono(String iName, String iDescription, long iStartTime, String iDictionary) {
    return 0;
  }

  @Override
  public String dumpChronos() {
    return null;
  }

  @Override
  public String[] getCountersAsString() {
    return null;
  }

  @Override
  public String[] getChronosAsString() {
    return null;
  }

  @Override
  public Date getLastReset() {
    return null;
  }

  @Override
  public void setAutoDump(final int iSeconds) {
    if (autoDumpTask != null) {
      // CANCEL ANY PREVIOUS RUNNING TASK
      autoDumpTask.cancel();
      autoDumpTask = null;
    }

    if (iSeconds > 0) {
      OLogManager.instance()
          .info(this, "Enabled auto dump of profiler every %d second(s)", iSeconds);

      final int ms = iSeconds * 1000;

      autoDumpTask =
          Orient.instance()
              .scheduleTask(
                  () -> {
                    final StringBuilder output = new StringBuilder();

                    final String dumpType =
                        OGlobalConfiguration.PROFILER_AUTODUMP_TYPE.getValueAsString();

                    output.append(
                        "\n*******************************************************************************************************************************************");
                    output.append(
                        "\nPROFILER AUTO DUMP '"
                            + dumpType
                            + "' OUTPUT (to disabled it set 'profiler.autoDump.interval' = 0):\n");
                    output.append(dump(dumpType));
                    output.append(
                        "\n*******************************************************************************************************************************************");

                    OLogManager.instance().info(null, output.toString());
                  },
                  ms,
                  ms);
    } else OLogManager.instance().info(this, "Auto dump of profiler disabled", iSeconds);
  }

  @Override
  public String metadataToJSON() {
    return null;
  }

  @Override
  public Map<String, OPair<String, METRIC_TYPE>> getMetadata() {
    final Map<String, OPair<String, METRIC_TYPE>> metadata =
        new HashMap<String, OPair<String, METRIC_TYPE>>();
    for (Entry<String, String> entry : dictionary.entrySet())
      metadata.put(
          entry.getKey(),
          new OPair<String, METRIC_TYPE>(entry.getValue(), types.get(entry.getKey())));
    return metadata;
  }

  @Override
  public String[] getHookAsString() {
    final List<String> keys = new ArrayList<String>(hooks.keySet());
    final String[] array = new String[keys.size()];
    return keys.toArray(array);
  }

  public void registerHookValue(
      final String iName,
      final String iDescription,
      final METRIC_TYPE iType,
      final OProfilerHookValue iHookValue) {
    registerHookValue(iName, iDescription, iType, iHookValue, iName);
  }

  public void registerHookValue(
      final String iName,
      final String iDescription,
      final METRIC_TYPE iType,
      final OProfilerHookValue iHookValue,
      final String iMetadataName) {
    if (iName != null) {
      unregisterHookValue(iName);
      updateMetadata(iMetadataName, iDescription, iType);
      hooks.put(iName, new OProfilerHookRuntime(iHookValue, iType));
    }
  }

  @Override
  public void unregisterHookValue(final String iName) {
    if (iName != null) hooks.remove(iName);
  }

  @Override
  public String getSystemMetric(final String iMetricName) {
    final StringBuilder buffer = new StringBuilder("system.".length() + iMetricName.length() + 1);
    buffer.append("system.");
    buffer.append(iMetricName);
    return buffer.toString();
  }

  @Override
  public String getProcessMetric(final String iMetricName) {
    final StringBuilder buffer = new StringBuilder("process.".length() + iMetricName.length() + 1);
    buffer.append("process.");
    buffer.append(iMetricName);
    return buffer.toString();
  }

  @Override
  public String getDatabaseMetric(final String iDatabaseName, final String iMetricName) {
    final StringBuilder buffer = new StringBuilder(128);
    buffer.append("db.");
    buffer.append(iDatabaseName != null ? iDatabaseName : "*");
    buffer.append('.');
    buffer.append(iMetricName);
    return buffer.toString();
  }

  @Override
  public String toJSON(String command, final String iPar1) {
    return null;
  }

  protected void installMemoryChecker() {
    final long memoryCheckInterval =
        OGlobalConfiguration.PROFILER_MEMORYCHECK_INTERVAL.getValueAsLong();

    if (memoryCheckInterval > 0)
      Orient.instance().scheduleTask(new MemoryChecker(), memoryCheckInterval, memoryCheckInterval);
  }

  /** Updates the metric metadata. */
  protected void updateMetadata(
      final String iName, final String iDescription, final METRIC_TYPE iType) {
    if (iDescription != null && dictionary.putIfAbsent(iName, iDescription) == null)
      types.put(iName, iType);
  }

  @Override
  public void registerListener(OProfilerListener listener) {
    listeners.add(listener);
  }

  @Override
  public void unregisterListener(OProfilerListener listener) {
    listeners.remove(listener);
  }

  @Override
  public String threadDump() {
    final StringBuilder dump = new StringBuilder();
    dump.append("THREAD DUMP\n");
    final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
    final ThreadInfo[] threadInfos =
        threadMXBean.getThreadInfo(threadMXBean.getAllThreadIds(), 100);
    for (ThreadInfo threadInfo : threadInfos) {
      dump.append('"');
      dump.append(threadInfo.getThreadName());
      dump.append("\" ");
      final Thread.State state = threadInfo.getThreadState();
      dump.append("\n   java.lang.Thread.State: ");
      dump.append(state);
      final StackTraceElement[] stackTraceElements = threadInfo.getStackTrace();
      for (final StackTraceElement stackTraceElement : stackTraceElements) {
        dump.append("\n        at ");
        dump.append(stackTraceElement);
      }
      dump.append("\n\n");
    }
    return dump.toString();
  }

  @Override
  public METRIC_TYPE getType(final String k) {
    return types.get(k);
  }
}
