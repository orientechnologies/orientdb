package com.orientechnologies.agent.services.metrics.server.resources;

import com.orientechnologies.agent.profiler.OMetricsRegistry;
import com.orientechnologies.agent.profiler.metrics.*;
import com.orientechnologies.agent.services.metrics.OGlobalMetrics;
import com.orientechnologies.agent.services.metrics.OrientDBMetric;
import com.orientechnologies.enterprise.server.OEnterpriseServer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OLocalPaginatedStorage;
import com.sun.management.OperatingSystemMXBean;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Enrico Risa on 19/07/2018.
 */
public class OrientDBServerResourcesMetrics implements OrientDBMetric {

  private final OEnterpriseServer server;
  private final OMetricsRegistry  registry;

  public OrientDBServerResourcesMetrics(OEnterpriseServer server, OMetricsRegistry registry) {
    this.server = server;
    this.registry = registry;
  }

  @Override
  public void start() {

    this.registry.register(OGlobalMetrics.SERVER_RUNTIME_GC.name, OGlobalMetrics.SERVER_RUNTIME_GC.description, GCMetric.class);
    this.registry
        .register(OGlobalMetrics.SERVER_RUNTIME_MEMORY.name, OGlobalMetrics.SERVER_RUNTIME_MEMORY.description, MemoryMetric.class);
    this.registry.register(OGlobalMetrics.SERVER_RUNTIME_THREADS.name, OGlobalMetrics.SERVER_RUNTIME_THREADS.description,
        ThreadsMetric.class);

    this.registry.gauge(OGlobalMetrics.SERVER_RUNTIME_CPU.name, OGlobalMetrics.SERVER_RUNTIME_CPU.description, this::cpuUsage);

    this.registry.registerAll(OGlobalMetrics.SERVER_RUNTIME_DISK_CACHE.name, new OMetricSet() {
      @Override
      public Map<String, OMetric> getMetrics() {

        Map<String, OMetric> metrics = new HashMap<>();
        metrics
            .put("total", registry.newGauge("total", "Total disk cache", OrientDBServerResourcesMetrics.this::getDiskCacheTotal));
        metrics
            .put("used", registry.newGauge("used", "Total used disk cache", OrientDBServerResourcesMetrics.this::getDiskCacheUsed));
        return metrics;
      }

      @Override
      public String getName() {
        return OGlobalMetrics.SERVER_RUNTIME_DISK_CACHE.name;
      }

      @Override
      public String getDescription() {
        return OGlobalMetrics.SERVER_RUNTIME_DISK_CACHE.description;
      }
    });
  }

  protected long getDiskCacheTotal() {
    long diskCacheTotal = 0;
    for (OStorage stg : server.getDatabases().getStorages()) {
      if (stg instanceof OLocalPaginatedStorage) {
        diskCacheTotal += OGlobalConfiguration.DISK_CACHE_SIZE.getValueAsLong() * 1024 * 1024;
        break;
      }
    }
    return diskCacheTotal;
  }

  public long getDiskCacheUsed() {
    long diskCacheUsed = 0;
    for (OStorage stg : server.getDatabases().getStorages()) {
      if (stg instanceof OLocalPaginatedStorage) {
        diskCacheUsed += ((OLocalPaginatedStorage) stg).getReadCache().getUsedMemory();
        break;
      }
    }

    return diskCacheUsed;
  }

  @Override
  public void stop() {
    this.registry.remove(OGlobalMetrics.SERVER_RUNTIME_GC.name);
    this.registry.remove(OGlobalMetrics.SERVER_RUNTIME_MEMORY.name);
    this.registry.remove(OGlobalMetrics.SERVER_RUNTIME_THREADS.name);
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
}
