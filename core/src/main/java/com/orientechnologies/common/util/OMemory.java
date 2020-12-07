/*
 *
 *  *  Copyright 2016 OrientDB LTD (info(at)orientdb.com)
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
 *  * For more information: http://www.orientdb.com
 */

package com.orientechnologies.common.util;

import com.orientechnologies.common.jnr.ONative;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;

/**
 * Provides various utilities related to memory management and configuration.
 *
 * @author Sergey Sitnikov
 */
public class OMemory {
  /**
   * @param unlimitedCap the upper limit on reported memory, if JVM reports unlimited memory.
   * @return same as {@link Runtime#maxMemory()} except that {@code unlimitedCap} limit is applied
   *     if JVM reports {@link Long#MAX_VALUE unlimited memory}.
   */
  public static long getCappedRuntimeMaxMemory(long unlimitedCap) {
    final long jvmMaxMemory = Runtime.getRuntime().maxMemory();
    return jvmMaxMemory == Long.MAX_VALUE ? unlimitedCap : jvmMaxMemory;
  }

  /**
   * Calculates the total configured maximum size of all OrientDB caches.
   *
   * @return the total maximum size of all OrientDB caches in bytes.
   */
  private static long getMaxCacheMemorySize() {
    return OGlobalConfiguration.DISK_CACHE_SIZE.getValueAsLong() * 1024 * 1024;
  }

  /**
   * Checks the OrientDB cache memory configuration and emits a warning if configuration is invalid.
   */
  public static void checkCacheMemoryConfiguration() {
    final long maxHeapSize = Runtime.getRuntime().maxMemory();
    final long maxCacheSize = getMaxCacheMemorySize();
    final ONative.MemoryLimitResult physicalMemory = ONative.instance().getMemoryLimit(false);

    if (maxHeapSize != Long.MAX_VALUE
        && physicalMemory != null
        && maxHeapSize + maxCacheSize > physicalMemory.memoryLimit)
      OLogManager.instance()
          .warnNoDb(
              OMemory.class,
              "The sum of the configured JVM maximum heap size ("
                  + maxHeapSize
                  + " bytes) "
                  + "and the OrientDB maximum cache size ("
                  + maxCacheSize
                  + " bytes) is larger than the available physical memory size "
                  + "("
                  + physicalMemory.memoryLimit
                  + " bytes). That may cause out of memory errors, please tune the configuration up. Use the "
                  + "-Xmx JVM option to lower the JVM maximum heap memory size or storage.diskCache.bufferSize OrientDB option to "
                  + "lower memory requirements of the cache.");
  }

  /**
   * Tries to fix some common cache/memory configuration problems:
   *
   * <ul>
   *   <li>Cache size is larger than direct memory size.
   *   <li>Memory chunk size is larger than cache size.
   *       <ul/>
   */
  public static void fixCommonConfigurationProblems() {
    long diskCacheSize = OGlobalConfiguration.DISK_CACHE_SIZE.getValueAsLong();

    final int max32BitCacheSize = 512;
    if (getJavaBitWidth() == 32 && diskCacheSize > max32BitCacheSize) {
      OLogManager.instance()
          .infoNoDb(
              OGlobalConfiguration.class,
              "32 bit JVM is detected. Lowering disk cache size from %,dMB to %,dMB.",
              diskCacheSize,
              max32BitCacheSize);
      OGlobalConfiguration.DISK_CACHE_SIZE.setValue(max32BitCacheSize);
    }
  }

  private static int getJavaBitWidth() {
    // Figure out whether bit width of running JVM
    // Most of JREs support property "sun.arch.data.model" which is exactly what we need here
    String dataModel = System.getProperty("sun.arch.data.model", "64"); // By default assume 64bit
    int size = 64;
    try {
      size = Integer.parseInt(dataModel);
    } catch (NumberFormatException ignore) {
      // Ignore
    }
    return size;
  }

  private OMemory() {}
}
