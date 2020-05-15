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

  import com.orientechnologies.agent.OEnterpriseAgent;
  import com.orientechnologies.common.log.OLogManager;
  import com.orientechnologies.common.profiler.OAbstractProfiler;
  import com.orientechnologies.common.profiler.OProfilerEntry;
  import com.orientechnologies.common.profiler.OProfilerListener;
  import com.orientechnologies.enterprise.server.OEnterpriseServer;
  import com.orientechnologies.orient.core.config.OGlobalConfiguration;
  import com.orientechnologies.orient.core.record.impl.ODocument;
  import com.orientechnologies.orient.core.storage.OStorage;
  import com.orientechnologies.orient.core.storage.disk.OLocalPaginatedStorage;
  import com.orientechnologies.orient.server.OServer;
  import com.orientechnologies.orient.server.distributed.listener.ODistributedLifecycleListener;
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
  import java.util.concurrent.ConcurrentHashMap;
  import java.util.concurrent.atomic.AtomicBoolean;
  import java.util.concurrent.atomic.AtomicInteger;
  import java.util.concurrent.atomic.AtomicLong;
  import java.util.concurrent.atomic.AtomicReference;

  /**
   * Profiling utility class. Handles chronos (times), statistics and counters. By default it's used as Singleton but you can create
   * any instances you want for separate profiling contexts.
   * <p/>
   * To start the recording use call startRecording(). By default record is turned off to avoid a run-time execution cost.
   *
   * @author Luca Garulli
   * @copyrights Orient Technologies.com
   */
  public class OEnterpriseProfiler extends OAbstractProfiler implements ODistributedLifecycleListener {
    protected final        Timer                          timer                   = new Timer(true);
    protected static final int                            BUFFER_SIZE             = 2048;
    public static final    int                            KEEP_ALIVE              = 60 * 1000;
    protected final        int                            metricProcessors        = Runtime.getRuntime().availableProcessors();
    protected              Date                           lastReset               = new Date();
    protected              OProfilerData                  realTime                = new OProfilerData();
    protected final        AtomicReference<OProfilerData> lastSnapshot            = new AtomicReference<OProfilerData>();
    protected              int                            elapsedToCreateSnapshot = 0;
    protected              TimerTask                      archiverTask;
    protected              TimerTask                      autoPause;
    protected              OEnterpriseServer              server;
    protected              AtomicBoolean                  paused                  = new AtomicBoolean(false);
    protected              AtomicLong                     timestamp               = new AtomicLong(System.currentTimeMillis());

    protected Set<OEnterpriseProfilerListener> profilerListeners = Collections
        .newSetFromMap(new ConcurrentHashMap<OEnterpriseProfilerListener, Boolean>());

    public OEnterpriseProfiler() {
      init();
    }

    public OEnterpriseProfiler(final int iElapsedToCreateSnapshot, final OAbstractProfiler iParentProfiler,
        OEnterpriseServer server) {
      super(iParentProfiler);
      elapsedToCreateSnapshot = iElapsedToCreateSnapshot;
      this.server = server;
      init();
    }

    public void configure(final String iConfiguration) {
      if (iConfiguration == null || iConfiguration.length() == 0)
        return;

      final String[] parts = iConfiguration.split(",");
      elapsedToCreateSnapshot = Integer.parseInt(parts[0].trim());

      if (isRecording())
        stopRecording();

      startRecording();
    }

    public void shutdown() {
      autoPause.cancel();

      super.shutdown();
      hooks.clear();

      lastSnapshot.set(null);
    }

    @Override
    protected void setTip(final String iMessage, final AtomicInteger counter) {
      realTime.setTip(iMessage, counter);
    }

    @Override
    protected AtomicInteger getTip(final String iMessage) {
      return realTime.getTip(iMessage);
    }

    @Override
    public ODocument getContext() {
      return new ODocument().field("enterprise", true).field("cloud", false).field("monitoringUrl", "");
    }

    public boolean startRecording() {
      if (!super.startRecording())
        return false;

      OLogManager.instance().info(this, "Profiler is recording metrics with configuration: %d", elapsedToCreateSnapshot);

      if (elapsedToCreateSnapshot > 0) {
        lastSnapshot.set(new OProfilerData());

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

      return true;
    }

    public boolean stopRecording() {
      if (!super.stopRecording())
        return false;

      OLogManager.instance().config(this, "Profiler has stopped recording metrics");

      lastSnapshot.set(null);
      realTime.clear();
      dictionary.clear();
      types.clear();
      if (archiverTask != null)
        archiverTask.cancel();

      return true;
    }

    public void updateCounter(final String iName, final String iDescription, final long iPlus) {
      updateCounter(iName, iDescription, iPlus, iName);
    }

    public void updateCounter(final String iName, final String iDescription, final long iPlus, final String iMetadata) {
      if (iName == null || recordingFrom < 0)
        return;

      updateMetadata(iMetadata, iDescription, METRIC_TYPE.COUNTER);

      final OProfilerData snapshot = lastSnapshot.get();
      if (snapshot != null)
        snapshot.updateCounter(iName, iPlus);

      realTime.updateCounter(iName, iPlus);

      long counter = realTime.getCounter(iName);
      for (OProfilerListener listener : listeners) {
        listener.onUpdateCounter(iName, counter, realTime.getRecordingFrom(), System.currentTimeMillis());
      }
    }

    public long getCounter(final String iStatName) {
      if (iStatName == null || recordingFrom < 0)
        return -1;

      return realTime.getCounter(iStatName);
    }

    @Override
    public boolean isEnterpriseEdition() {
      return true;
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

      if (iQuery.equals("realtime")) {
        realTime.setHookValues(hookValuesSnapshots);
        realTime.toJSON(buffer, iPar1);

      } else if (iQuery.equals("last")) {

        final OProfilerData snapshot = lastSnapshot.get();
        if (snapshot != null) {
          snapshot.setHookValues(hookValuesSnapshots);
          snapshot.toJSON(buffer, iPar1);
        }

      } else {
        // GET THE RANGES
        if (iPar1 == null)
          throw new IllegalArgumentException("Invalid range format. Use: <from>, where * means any");

        final long from = iPar1.equals("*") ? 0 : Long.parseLong(iPar1);

        buffer.append("[");
        if (iQuery.equals("archive")) {
          // ARCHIVE
          final OProfilerData snapshot = lastSnapshot.get();

          if (snapshot != null && snapshot.getRecordingFrom() >= from)
            snapshot.toJSON(buffer, null);

        } else
          throw new IllegalArgumentException("Invalid archive query: use realtime|last|archive");

        buffer.append("]");
      }

      buffer.append("}");

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

      final StringBuilder buffer = new StringBuilder(BUFFER_SIZE);
      buffer.append("\nOrientDB profiler dump of ");
      buffer.append(new Date(now));
      buffer.append(" after ");
      buffer.append((now - recordingFrom) / 1000);
      buffer.append(" secs of profiling");
      buffer.append(String
          .format("\nFree memory: %2.2fMb (%2.2f%%) - Total memory: %2.2fMb - Max memory: %2.2fMb - CPUs: %d", freeMem,
              (freeMem * 100 / (float) maxMem), totMem, maxMem, Runtime.getRuntime().availableProcessors()));
      buffer.append("\n");
      buffer.append(dumpHookValues());
      buffer.append("\n");
      buffer.append(dumpCounters());
      buffer.append("\n\n");
      buffer.append(dumpStats());
      buffer.append("\n\n");
      buffer.append(dumpChronos());
      return buffer.toString();
    }

    public long startChrono() {
      // CHECK IF CHRONOS ARE ACTIVED
      if (recordingFrom < 0)
        return -1;

      return System.currentTimeMillis();
    }

    public void registerProfilerListener(OEnterpriseProfilerListener listener) {
      profilerListeners.add(listener);
    }

    public void unregisterProfilerListener(OEnterpriseProfilerListener listener) {
      profilerListeners.remove(listener);
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

      final OProfilerData snapshot = lastSnapshot.get();
      if (snapshot != null)
        snapshot.stopChrono(iName, iStartTime, iPayload, user);

      final long stopChrono = realTime.stopChrono(iName, iStartTime, iPayload, user);
      final OProfilerEntry chrono = realTime.getChrono(iName);
      for (OProfilerListener listener : listeners) {
        listener.onUpdateChrono(chrono);
      }
      return stopChrono;
    }

    public long updateStat(final String iName, final String iDescription, final long iValue) {
      // CHECK IF CHRONOS ARE ACTIVED
      if (recordingFrom < 0)
        return -1;

      updateMetadata(iName, iDescription, METRIC_TYPE.STAT);

      final OProfilerData snapshot = lastSnapshot.get();
      if (snapshot != null)
        snapshot.updateStat(iName, iValue);

      return realTime.updateStat(iName, iValue);
    }

    public String dumpCounters() {
      // CHECK IF STATISTICS ARE ACTIVED
      if (recordingFrom < 0)
        return "Counters: <no recording>";

      return realTime.dumpCounters();
    }

    public String dumpChronos() {
      return realTime.dumpChronos();
    }

    public String dumpStats() {
      return realTime.dumpStats();
    }

    public String dumpHookValues() {
      if (recordingFrom < 0)
        return "HookValues: <no recording>";

      final StringBuilder buffer = new StringBuilder(BUFFER_SIZE);

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

      buffer.append(String.format("\n%50s +-------------------------------------------------------------------+", ""));
      return buffer.toString();
    }

    @Override
    public Object getHookValue(final String iName) {
      final OProfilerHookRuntime v = hooks.get(iName);
      return v != null ? v.hook.getValue() : null;
    }

    public String[] getCountersAsString() {
      return realTime.getCountersAsString();
    }

    public String[] getStatsAsString() {
      return realTime.getStatsAsString();
    }

    public Date getLastReset() {
      return lastReset;
    }

    public List<String> getCounters() {
      return realTime.getCounters();
    }

    public OProfilerEntry getStat(final String iStatName) {
      return realTime.getStat(iStatName);
    }

    @Override
    public List<String> getChronos() {
      return realTime.getChronos();
    }

    public OProfilerEntry getChrono(final String iChronoName) {
      return realTime.getChrono(iChronoName);
    }

    public void setAutoDump(final int iSeconds) {
      if (iSeconds > 0) {
        if (autoPause != null)
          autoPause.cancel();

        final int ms = iSeconds * 1000;

        timer.schedule(new TimerTask() {

          @Override
          public void run() {
            final String dumpType = OGlobalConfiguration.PROFILER_AUTODUMP_TYPE.getValueAsString();

            final StringBuilder output = new StringBuilder();
            output.append(
                "\n*******************************************************************************************************************************************");
            output
                .append("\nPROFILER AUTO DUMP '" + dumpType + "' OUTPUT (to disabled it set 'profiler.autoDump.interval' = 0):\n");
            output.append(dump(dumpType));
            output.append(
                "\n*******************************************************************************************************************************************");

            OLogManager.instance().info(null, output.toString());
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

      final Map<String, OProfilerHookStatic> result = new HashMap<String, OProfilerHookStatic>(hooks.size());

      for (Entry<String, OProfilerHookRuntime> entry : hooks.entrySet())
        result.put(entry.getKey(), new OProfilerHookStatic(entry.getValue().hook.getValue(), entry.getValue().type));

      return result;
    }

    private void init() {
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
      registerHookValue(getSystemMetric("config.os.version"), "Operative System version", METRIC_TYPE.TEXT,
          new OProfilerHookValue() {
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

      if (OGlobalConfiguration.PROFILER_AUTODUMP_INTERVAL.getValueAsInteger() > 0)
        setAutoDump(OGlobalConfiguration.PROFILER_AUTODUMP_INTERVAL.getValueAsInteger());
      else {
        // CREATE AUTO PAUSE TASK TO STOP RECORDING IF STUDIO IS NOT ASKING FOR METRICS
        autoPause = new TimerTask() {
          @Override
          public void run() {
            long current = System.currentTimeMillis();

            long old = timestamp.get();

            if (current - old > KEEP_ALIVE) {

              boolean canSleep = true;

              for (OProfilerListener listener : listeners) {
                canSleep = canSleep && listener.canSleep();
              }
              if (canSleep) {
                stopRecording();
                paused.set(true);
              }
            }
            timer.schedule(autoPause, KEEP_ALIVE, KEEP_ALIVE);
          }
        };
      }
    }

    @Override
    public String getStatsAsJson() {
      String json = null;

      updateStats();

      json = toJSON("realtime", null);

      for (OEnterpriseProfilerListener profilerListener : profilerListeners) {
        profilerListener.onStatsPublished(json);
      }

      return json;
    }

    public void updateStats() {
      double cpuUsage = cpuUsage();
      updateStat(getProcessMetric("runtime.cpu"), "Total cpu used by the process", (long) (cpuUsage * 100));
      updateStat(getProcessMetric("runtime.availableMemory"), "Available memory for the process",
          Runtime.getRuntime().freeMemory());
      updateStat(getProcessMetric("runtime.maxMemory"), "Maximum memory usable for the process", Runtime.getRuntime().maxMemory());
      updateStat(getProcessMetric("runtime.totalMemory"), "Total memory used by the process", Runtime.getRuntime().totalMemory());

      long diskCacheUsed = 0;
      long diskCacheTotal = 0;
      for (OStorage stg : server.getDatabases().getStorages()) {
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
        try {
          cpuMethod.setAccessible(true);
        } catch (RuntimeException e) {
          //This fail in jdk9
        }
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

    @Override
    public boolean onNodeJoining(String iNode) {
      return true;
    }

    @Override
    public void onNodeJoined(String iNode) {
    }

    @Override
    public void onNodeLeft(final String iNode) {
    }

    @Override
    public void onDatabaseChangeStatus(String iNode, String iDatabaseName, ODistributedServerManager.DB_STATUS iNewStatus) {

    }

    private void createSnapshot() {
      final OProfilerData snapshot = lastSnapshot.getAndSet(new OProfilerData());
      if (snapshot == null)
        return;

      final Map<String, OProfilerHookStatic> hookValuesSnapshots = archiveHooks();

      // ARCHIVE IT
      snapshot.setHookValues(hookValuesSnapshots);
      snapshot.endRecording();

      for (OProfilerListener listener : listeners) {
        listener.onSnapshotCreated(snapshot);
      }
    }
  }
