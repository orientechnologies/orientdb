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

package com.orientechnologies.orient.core.profiler;

import java.io.File;

import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.memory.OMemoryWatchDog;

/**
 * Profiling utility class. Handles chronos (times), statistics and counters. By default it's used as Singleton but you can create
 * any instances you want for separate profiling contexts.
 * 
 * To start the recording use call startRecording(). By default record is turned off to avoid a run-time execution cost.
 * 
 * @author Luca Garulli
 * @copyrights Orient Technologies.com
 */
public class OJVMProfiler extends OProfiler implements OMemoryWatchDog.Listener {
  private final int           metricProcessors = Runtime.getRuntime().availableProcessors();
  private final static String basePath         = Orient.getHomePath() + "/databases";

  public OJVMProfiler() {
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
    registerHookValue(getProcessMetric("runtime.availableMemory"), "Available memory for the process", METRIC_TYPE.SIZE,
        new OProfilerHookValue() {
          @Override
          public Object getValue() {
            return Runtime.getRuntime().freeMemory();
          }
        });
    registerHookValue(getProcessMetric("runtime.maxMemory"), "Maximum memory usable for the process", METRIC_TYPE.SIZE,
        new OProfilerHookValue() {
          @Override
          public Object getValue() {
            return Runtime.getRuntime().maxMemory();
          }
        });
    registerHookValue(getProcessMetric("runtime.totalMemory"), "Total memory used by the process", METRIC_TYPE.SIZE,
        new OProfilerHookValue() {
          @Override
          public Object getValue() {
            return Runtime.getRuntime().totalMemory();
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

  }

  public String getSystemMetric(final String iMetricName) {
    final StringBuilder buffer = new StringBuilder();
    buffer.append("system.");
    buffer.append(iMetricName);
    return buffer.toString();
  }

  public String getProcessMetric(final String iMetricName) {
    final StringBuilder buffer = new StringBuilder();
    buffer.append("process.");
    buffer.append(iMetricName);
    return buffer.toString();
  }

  public String getDatabaseMetric(final String iDatabaseURL, final String iMetricName) {
    final StringBuilder buffer = new StringBuilder();
    buffer.append("db.");

    final String url = iDatabaseURL;

    buffer.append(url);
    buffer.append('.');
    buffer.append(iMetricName);
    return buffer.toString();
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
}
