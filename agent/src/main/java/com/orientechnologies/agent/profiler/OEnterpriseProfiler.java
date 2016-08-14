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

import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.profiler.OAbstractProfiler;
import com.orientechnologies.common.profiler.OProfilerEntry;
import com.orientechnologies.common.profiler.OProfilerListener;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OLocalPaginatedStorage;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedLifecycleListener;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.sun.management.OperatingSystemMXBean;

import java.io.File;
import java.io.PrintStream;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Profiling utility class. Handles chronos (times), statistics and counters. By default it's used as Singleton but you can create
 * any instances you want for separate profiling contexts.
 * <p/>
 * To start the recording use call startRecording(). By default record is turned off to avoid a run-time execution cost.
 *
 * @author Luca Garulli
 * @copyrights Orient Technologies.com
 */
public class OEnterpriseProfiler extends OAbstractProfiler {
  protected final static Timer        timer                   = new Timer(true);
  protected final static int          BUFFER_SIZE             = 2048;
  public static final int             KEEP_ALIVE              = 60 * 1000;
  protected final List<OProfilerData> snapshots               = new ArrayList<OProfilerData>();
  protected final int                 metricProcessors        = Runtime.getRuntime().availableProcessors();
  protected Date                      lastReset               = new Date();
  protected OProfilerData             realTime                = new OProfilerData();
  protected OProfilerData             lastSnapshot;
  protected int                       elapsedToCreateSnapshot = 0;
  protected int                       maxSnapshots            = 0;
  protected TimerTask                 archiverTask;
  protected TimerTask                 autoPause;
  protected TimerTask                 autoPublish;
  protected OServer                   server;
  protected AtomicBoolean             paused                  = new AtomicBoolean(false);
  protected AtomicLong                timestamp               = new AtomicLong(System.currentTimeMillis());

  public OEnterpriseProfiler() {
    init();
  }

  public OEnterpriseProfiler(final int iElapsedToCreateSnapshot, final int iMaxSnapshot, final OAbstractProfiler iParentProfiler,
      OServer server) {
    super(iParentProfiler);
    elapsedToCreateSnapshot = iElapsedToCreateSnapshot;
    maxSnapshots = iMaxSnapshot;
    this.server = server;
    init();
  }

  public void configure(final String iConfiguration) {
    if (iConfiguration == null || iConfiguration.length() == 0)
      return;

    final String[] parts = iConfiguration.split(",");
    elapsedToCreateSnapshot = Integer.parseInt(parts[0].trim());
    maxSnapshots = Integer.parseInt(parts[1].trim());

    if (isRecording())
      stopRecording();

    startRecording();

  }

  public void shutdown() {
    autoPause.cancel();
    autoPublish.cancel();

    super.shutdown();
    hooks.clear();

    synchronized (snapshots) {
      snapshots.clear();
    }
  }

  @Override
  protected void setTip(final String iMessage, final AtomicInteger counter) {
    realTime.setTip(iMessage, counter);
  }

  @Override
  protected AtomicInteger getTip(final String iMessage) {
    return realTime.getTip(iMessage);
  }

  public boolean startRecording() {
    if (!super.startRecording())
      return false;

    acquireExclusiveLock();
    try {
      OLogManager.instance().info(this, "Profiler is recording metrics with configuration: %d,%d", elapsedToCreateSnapshot,
          maxSnapshots);

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

    } finally {
      releaseExclusiveLock();
    }

    return true;
  }

  public boolean stopRecording() {
    if (!super.stopRecording())
      return false;

    acquireExclusiveLock();
    try {
      OLogManager.instance().config(this, "Profiler has stopped recording metrics");

      lastSnapshot = null;
      realTime.clear();
      dictionary.clear();
      types.clear();
      if (archiverTask != null)
        archiverTask.cancel();

    } finally {
      releaseExclusiveLock();
    }

    return true;
  }

  public void createSnapshot() {
    if (lastSnapshot == null)
      return;

    final Map<String, OProfilerHookStatic> hookValuesSnapshots = archiveHooks();

    acquireExclusiveLock();
    try {

      synchronized (snapshots) {
        // ARCHIVE IT
        lastSnapshot.setHookValues(hookValuesSnapshots);
        lastSnapshot.endRecording();
        snapshots.add(lastSnapshot);

        for (OProfilerListener listener : listeners) {
          listener.onSnapshotCreated(lastSnapshot);
        }
        lastSnapshot = new OProfilerData();

        while (snapshots.size() >= maxSnapshots && maxSnapshots > 0) {
          // REMOVE THE OLDEST SNAPSHOT
          snapshots.remove(0);
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

      long counter = realTime.getCounter(iName);
      for (OProfilerListener listener : listeners) {
        listener.onUpdateCounter(iName, counter, realTime.getRecordingFrom(), System.currentTimeMillis());
      }

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

  public void resetRealtime(final String iText) {
    realTime.clear(iText);
  }

  @Override
  public void dump(PrintStream printStream) {

  }

  public String toJSON(final String iQuery, final String iPar1) {
    if (Boolean.TRUE.equals(paused.get())) {
      startRecording();
      paused.set(false);
    }
    final StringBuilder buffer = new StringBuilder(BUFFER_SIZE * 5);
    timestamp.set(System.currentTimeMillis());
    Map<String, OProfilerHookStatic> hookValuesSnapshots = null;

    if (iQuery.equals("realtime") || iQuery.equals("last"))
      // GET LATETS HOOK VALUES
      hookValuesSnapshots = archiveHooks();

    buffer.append("{ \"" + iQuery + "\":");

    acquireSharedLock();
    try {
      if (iQuery.equals("realtime")) {
        realTime.setHookValues(hookValuesSnapshots);
        realTime.toJSON(buffer, iPar1);

      } else if (iQuery.equals("last")) {
        if (lastSnapshot != null) {
          lastSnapshot.setHookValues(hookValuesSnapshots);
          lastSnapshot.toJSON(buffer, iPar1);
        }

      } else {
        // GET THE RANGES
        if (iPar1 == null)
          throw new IllegalArgumentException("Invalid range format. Use: <from>, where * means any");

        final long from = iPar1.equals("*") ? 0 : Long.parseLong(iPar1);

        boolean firstItem = true;
        buffer.append("[");
        if (iQuery.equals("archive")) {
          // ARCHIVE
          for (int i = 0; i < snapshots.size(); ++i) {
            final OProfilerData a = snapshots.get(i);

            if (a.getRecordingFrom() < from) {
              // ALREADY READ, REMOVE IT
              snapshots.remove(i);
              i--;
              continue;
            }

            if (firstItem)
              firstItem = false;
            else
              buffer.append(',');

            a.toJSON(buffer, null);
          }

        } else
          throw new IllegalArgumentException("Invalid archive query: use realtime|last|archive");

        buffer.append("]");
      }

      buffer.append("}");

    } finally {
      releaseSharedLock();
    }

    return buffer.toString();
  }

  public String metadataToJSON() {
    final StringBuilder buffer = new StringBuilder(BUFFER_SIZE);

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

      final StringBuilder buffer = new StringBuilder(BUFFER_SIZE);
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
    return stopChrono(iName, iDescription, iStartTime, iDictionaryName, iPayload, null);
  }

  public long stopChrono(final String iName, final String iDescription, final long iStartTime, final String iDictionaryName,
      final String iPayload, String user) {
    // CHECK IF CHRONOS ARE ACTIVED
    if (recordingFrom < 0)
      return -1;

    updateMetadata(iDictionaryName, iDescription, METRIC_TYPE.CHRONO);

    acquireSharedLock();
    try {

      if (lastSnapshot != null)
        lastSnapshot.stopChrono(iName, iStartTime, iPayload, user);

      long stopChrono = realTime.stopChrono(iName, iStartTime, iPayload, user);
      OProfilerEntry chrono = realTime.getChrono(iName);
      for (OProfilerListener listener : listeners) {
        listener.onUpdateChrono(chrono);
      }
      return stopChrono;

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

    final StringBuilder buffer = new StringBuilder(BUFFER_SIZE);

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
        final OProfilerHookRuntime v = hooks.get(k);
        if (v != null) {
          final Object hookValue = v.hook.getValue();
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
    final OProfilerHookRuntime v = hooks.get(iName);
    return v != null ? v.hook.getValue() : null;
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
  protected Map<String, OProfilerHookStatic> archiveHooks() {
    if (!isRecording())
      return null;

    final Map<String, OProfilerHookStatic> result = new HashMap<String, OProfilerHookStatic>();

    for (Entry<String, OProfilerHookRuntime> entry : hooks.entrySet())
      result.put(entry.getKey(), new OProfilerHookStatic(entry.getValue().hook.getValue(), entry.getValue().type));

    return result;
  }

  protected void init() {
    registerHookValue(getSystemMetric("config.cpus"), "Number of CPUs", METRIC_TYPE.SIZE, new OProfilerHookValue() {
      @Override
      public Object getValue() {
        return metricProcessors;
      }
    });
    registerHookValue(getSystemMetric("config.os.name"), "Operative System name", METRIC_TYPE.TEXT, new OProfilerHookValue() {
      @Override
      public Object getValue() {
        return System.getProperty("os.name");
      }
    });
    registerHookValue(getSystemMetric("config.os.version"), "Operative System version", METRIC_TYPE.TEXT, new OProfilerHookValue() {
      @Override
      public Object getValue() {
        return System.getProperty("os.version");
      }
    });
    registerHookValue(getSystemMetric("config.os.arch"), "Operative System architecture", METRIC_TYPE.TEXT,
        new OProfilerHookValue() {
          @Override
          public Object getValue() {
            return System.getProperty("os.arch");
          }
        });
    registerHookValue(getSystemMetric("config.java.vendor"), "Java vendor", METRIC_TYPE.TEXT, new OProfilerHookValue() {
      @Override
      public Object getValue() {
        return System.getProperty("java.vendor");
      }
    });
    registerHookValue(getSystemMetric("config.java.version"), "Java version", METRIC_TYPE.TEXT, new OProfilerHookValue() {
      @Override
      public Object getValue() {
        return System.getProperty("java.version");
      }
    });
    // registerHookValue(getProcessMetric("runtime.availableMemory"), "Available memory for the process", METRIC_TYPE.SIZE,
    // new OProfilerHookValue() {
    // @Override
    // public Object getValue() {
    // return Runtime.getRuntime().freeMemory();
    // }
    // });
    // registerHookValue(getProcessMetric("runtime.maxMemory"), "Maximum memory usable for the process", METRIC_TYPE.SIZE,
    // new OProfilerHookValue() {
    // @Override
    // public Object getValue() {
    // return Runtime.getRuntime().maxMemory();
    // }
    // });
    // registerHookValue(getProcessMetric("runtime.totalMemory"), "Total memory used by the process", METRIC_TYPE.SIZE,
    // new OProfilerHookValue() {
    // @Override
    // public Object getValue() {
    // return Runtime.getRuntime().totalMemory();
    // }
    // });
    //
    // registerHookValue(getProcessMetric("runtime.cpu"), "Total cpu used by the process", METRIC_TYPE.SIZE, new
    // OProfilerHookValue() {
    // @Override
    // public Object getValue() {
    // return "" + cpuUsage();
    // }
    // });

    final File[] roots = File.listRoots();
    for (final File root : roots) {
      String volumeName = root.getAbsolutePath();
      int pos = volumeName.indexOf(":\\");
      if (pos > -1)
        volumeName = volumeName.substring(0, pos);

      final String metricPrefix = "system.disk." + volumeName;

      registerHookValue(metricPrefix + ".totalSpace", "Total used disk space", METRIC_TYPE.SIZE, new OProfilerHookValue() {
        @Override
        public Object getValue() {
          return root.getTotalSpace();
        }
      });

      registerHookValue(metricPrefix + ".freeSpace", "Total free disk space", METRIC_TYPE.SIZE, new OProfilerHookValue() {
        @Override
        public Object getValue() {
          return root.getFreeSpace();
        }
      });

      registerHookValue(metricPrefix + ".usableSpace", "Total usable disk space", METRIC_TYPE.SIZE, new OProfilerHookValue() {
        @Override
        public Object getValue() {
          return root.getUsableSpace();
        }
      });
    }

    autoPause = new TimerTask() {
      @Override
      public void run() {
        long current = System.currentTimeMillis();

        long old = timestamp.get();

        if (current - old > KEEP_ALIVE) {
          stopRecording();
          paused.set(true);
        }
      }
    };
    timer.schedule(autoPause, KEEP_ALIVE, KEEP_ALIVE);

    if (server.getDistributedManager() != null) {
      final ODistributedServerManager distributedManager = server.getDistributedManager();
      server.getDistributedManager().registerLifecycleListener(new ODistributedLifecycleListener() {
        @Override
        public boolean onNodeJoining(String iNode) {
          return true;
        }

        @Override
        public void onNodeJoined(String iNode) {

        }

        @Override
        public void onNodeLeft(String iNode) {

          synchronized (server) {
            Map<String, Object> configurationMap = distributedManager.getConfigurationMap();
            ODocument doc = (ODocument) configurationMap.get("clusterStats");
            if (doc == null) {
              doc = new ODocument();
              doc.setTrackingChanges(false);
            }
            doc.removeField(iNode);
            ODocumentInternal.clearTrackData(doc);
            configurationMap.put("clusterStats", doc);
          }
        }

        @Override
        public void onDatabaseChangeStatus(String iNode, String iDatabaseName, ODistributedServerManager.DB_STATUS iNewStatus) {
        }
      });
    }
    autoPublish = new TimerTask() {
      @Override
      public void run() {
        try {
          if (!Boolean.TRUE.equals(paused.get())) {

            updateStats();
            synchronized (server) {

              ODistributedServerManager distributedManager = server.getDistributedManager();

              if (distributedManager != null) {
                String localNodeName = distributedManager.getLocalNodeName();
                if (distributedManager != null && distributedManager.isEnabled()) {
                  Map<String, Object> configurationMap = distributedManager.getConfigurationMap();
                  if (configurationMap != null) {
                    ODocument doc = (ODocument) configurationMap.get("clusterStats");

                    if (doc == null) {
                      doc = new ODocument();
                      doc.setTrackingChanges(false);
                    }
                    try {
                      ODocument entries = new ODocument().fromJSON(toJSON("realtime", null));
                      doc.field(localNodeName, entries.toMap());
                      configurationMap.put("clusterStats", doc);
                    } catch (Exception e) {
                      OLogManager.instance().debug(this, "Cannot publish realtime stats for node %s", e, localNodeName);
                    }
                  }
                }
              }
            }
          }
        } catch (HazelcastInstanceNotActiveException e) {
          // IGNORE IT
        }
      }
    };
    timer.schedule(autoPublish, 2000, 2000);
  }

  private void updateStats() {

    double cpuUsage = cpuUsage();
    updateStat(getProcessMetric("runtime.cpu"), "Total cpu used by the process", (long) (cpuUsage * 100));
    updateStat(getProcessMetric("runtime.availableMemory"), "Available memory for the process", Runtime.getRuntime().freeMemory());
    updateStat(getProcessMetric("runtime.maxMemory"), "Maximum memory usable for the process", Runtime.getRuntime().maxMemory());
    updateStat(getProcessMetric("runtime.totalMemory"), "Total memory used by the process", Runtime.getRuntime().totalMemory());

    long diskCacheUsed = 0;
    long diskCacheTotal = 0;
    for (OStorage stg : Orient.instance().getStorages()) {
      if (stg instanceof OLocalPaginatedStorage) {
        diskCacheUsed += ((OLocalPaginatedStorage) stg).getReadCache().getUsedMemory();
        diskCacheTotal += OGlobalConfiguration.DISK_CACHE_SIZE.getValueAsLong() * 1024 * 1024;
        break;
      }
    }
    updateStat(getProcessMetric("runtime.diskCacheTotal"), "Max disk cache for the process", diskCacheTotal);
    updateStat(getProcessMetric("runtime.diskCacheUsed"), "Total disk cache used by the process", diskCacheUsed);
  }

  public double cpuUsage() {
    OperatingSystemMXBean operatingSystemMXBean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();

    try {

      Method cpuMethod = operatingSystemMXBean.getClass().getDeclaredMethod("getProcessCpuLoad");
      cpuMethod.setAccessible(true);
      Double invoke = (Double) cpuMethod.invoke(operatingSystemMXBean);
      return invoke;
    } catch (NoSuchMethodException e) {

    } catch (InvocationTargetException e) {

    } catch (IllegalAccessException e) {

    }
    double cpuUsage;
    RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
    int availableProcessors = operatingSystemMXBean.getAvailableProcessors();
    long prevUpTime = runtimeMXBean.getUptime();
    long prevProcessCpuTime = operatingSystemMXBean.getProcessCpuTime();
    // FALLBACK
    try {
      Thread.sleep(500);
    } catch (Exception ignored) {
    }

    operatingSystemMXBean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
    long upTime = runtimeMXBean.getUptime();
    long processCpuTime = operatingSystemMXBean.getProcessCpuTime();
    long elapsedCpu = processCpuTime - prevProcessCpuTime;
    long elapsedTime = upTime - prevUpTime;
    cpuUsage = Math.min(99F, elapsedCpu / (elapsedTime * 10000F * availableProcessors));
    return cpuUsage;
  }
}
