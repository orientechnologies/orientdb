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

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.OConfigurationException;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

/**
 * Provides various utilities related to memory management and configuration.
 *
 * @author Sergey Sitnikov
 */
public class OMemory {
  // JVM accepts this option exactly as it appears here, no lowercase/uppercase mixing and additional spacing allowed
  private static final String XX_MAX_DIRECT_MEMORY_SIZE = "-XX:MaxDirectMemorySize=";

  /**
   * @param unlimitedCap the upper limit on reported memory, if JVM reports unlimited memory.
   * @return same as {@link Runtime#maxMemory()} except that {@code unlimitedCap} limit is applied if JVM reports
   * {@link Long#MAX_VALUE unlimited memory}.
   */
  public static long getCappedRuntimeMaxMemory(long unlimitedCap) {
    final long jvmMaxMemory = Runtime.getRuntime().maxMemory();
    return jvmMaxMemory == Long.MAX_VALUE ? unlimitedCap : jvmMaxMemory;
  }

  /**
   * Obtains the total size in bytes of the installed physical memory on this machine.
   * Note that on some VMs it's impossible to obtain the physical memory size, in this
   * case the return value will {@code -1}.
   *
   * @return the total physical memory size in bytes or {@code -1} if the size can't be obtained.
   */
  public static long getPhysicalMemorySize() {
    long osMemory = -1;

    final OperatingSystemMXBean mxBean = ManagementFactory.getOperatingSystemMXBean();
    try {
      final Method memorySize = mxBean.getClass().getDeclaredMethod("getTotalPhysicalMemorySize");
      memorySize.setAccessible(true);
      osMemory = (Long) memorySize.invoke(mxBean);
    } catch (NoSuchMethodException e) {
      if (!OLogManager.instance().isDebugEnabled())
        OLogManager.instance().warn(OMemory.class, "Unable to determine the amount of installed RAM.");
      else
        OLogManager.instance().debug(OMemory.class, "Unable to determine the amount of installed RAM.", e);
    } catch (InvocationTargetException e) {
      if (!OLogManager.instance().isDebugEnabled())
        OLogManager.instance().warn(OMemory.class, "Unable to determine the amount of installed RAM.");
      else
        OLogManager.instance().debug(OMemory.class, "Unable to determine the amount of installed RAM.", e);
    } catch (IllegalAccessException e) {
      if (!OLogManager.instance().isDebugEnabled())
        OLogManager.instance().warn(OMemory.class, "Unable to determine the amount of installed RAM.");
      else
        OLogManager.instance().debug(OMemory.class, "Unable to determine the amount of installed RAM.", e);
    }

    return osMemory;
  }

  /**
   * Obtains the configured value of the {@code -XX:MaxDirectMemorySize} JVM option in bytes.
   *
   * @return the configured maximum direct memory size or {@code -1} if no configuration provided.
   */
  public static long getConfiguredMaxDirectMemory() {
    long maxDirectMemorySize = -1;

    final RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
    final List<String> vmArgs = runtimeMXBean.getInputArguments();
    for (String arg : vmArgs)
      if (arg.startsWith(XX_MAX_DIRECT_MEMORY_SIZE)) {
        try {
          maxDirectMemorySize = parseVmArgsSize(arg.substring(XX_MAX_DIRECT_MEMORY_SIZE.length()));
        } catch (IllegalArgumentException e) {
          OLogManager.instance().error(OMemory.class, "Unable to parse the value of -XX:MaxDirectMemorySize option.", e);
        }
        break;
      }

    return maxDirectMemorySize;
  }

  /**
   * Calculates the total configured maximum size of all OrientDB caches.
   *
   * @return the total maximum size of all OrientDB caches in bytes.
   */
  public static long getMaxCacheMemorySize() {
    return OGlobalConfiguration.DISK_CACHE_SIZE.getValueAsLong() * 1024 * 1024;
  }

  /**
   * Checks the direct memory configuration and emits a warning if configuration is invalid.
   */
  public static void checkDirectMemoryConfiguration() throws OConfigurationException {
    final long physicalMemory = getPhysicalMemorySize();
    final long maxDirectMemory = getConfiguredMaxDirectMemory();

    if (maxDirectMemory == -1) {
      if (physicalMemory != -1)
        OLogManager.instance().warn(OMemory.class, "MaxDirectMemorySize JVM option is not set or has invalid value, "
            + "that may cause out of memory errors. Please set the -XX:MaxDirectMemorySize=" + physicalMemory / (1024 * 1024)
            + "m option when you start the JVM.");
      else
        OLogManager.instance().warn(OMemory.class, "MaxDirectMemorySize JVM option is not set or has invalid value, "
            + "that may cause out of memory errors. Please set the -XX:MaxDirectMemorySize=<SIZE>m JVM option "
            + "when you start the JVM, where <SIZE> is the memory size of this machine in megabytes.");
    }
  }

  /**
   * Checks the OrientDB cache memory configuration and emits a warning if configuration is invalid.
   */
  public static void checkCacheMemoryConfiguration() {
    final long maxHeapSize = Runtime.getRuntime().maxMemory();
    final long maxCacheSize = getMaxCacheMemorySize();
    final long physicalMemory = getPhysicalMemorySize();
    final long maxDirectMemory = getConfiguredMaxDirectMemory();

    if (maxDirectMemory != -1 && maxCacheSize > maxDirectMemory)
      OLogManager.instance().warn(OMemory.class, "Configured maximum amount of memory available to the cache (" + maxCacheSize
          + " bytes) is larger than configured JVM maximum direct memory size (" + maxDirectMemory + " bytes). That may cause "
          + "out of memory errors, please tune the configuration up. Use the -XX:MaxDirectMemorySize JVM option to raise the JVM "
          + "maximum direct memory size or storage.diskCache.bufferSize OrientDB option to lower memory requirements of the "
          + "cache.");

    if (maxHeapSize != Long.MAX_VALUE && physicalMemory != -1 && maxHeapSize + maxCacheSize > physicalMemory)
      OLogManager.instance().warn(OMemory.class,
          "The sum of the configured JVM maximum heap size (" + maxHeapSize + " bytes) " + "and the OrientDB maximum cache size ("
              + maxCacheSize + " bytes) is larger than the available physical memory size " + "(" + physicalMemory
              + " bytes). That may cause out of memory errors, please tune the configuration up. Use the "
              + "-Xmx JVM option to lower the JVM maximum heap memory size or storage.diskCache.bufferSize OrientDB option to "
              + "lower memory requirements of the cache.");
  }

  /**
   * Checks the {@link com.orientechnologies.common.directmemory.OByteBufferPool} configuration and emits a warning
   * if configuration is invalid.
   */
  public static void checkByteBufferPoolConfiguration() {
    final long maxDirectMemory = OMemory.getConfiguredMaxDirectMemory();
    final long memoryChunkSize = OGlobalConfiguration.MEMORY_CHUNK_SIZE.getValueAsLong();
    final long maxCacheSize = getMaxCacheMemorySize();

    if (maxDirectMemory != -1 && memoryChunkSize > maxDirectMemory)
      OLogManager.instance().warn(OMemory.class,
          "The configured memory chunk size (" + memoryChunkSize + " bytes) is larger than the configured maximum amount of "
              + "JVM direct memory (" + maxDirectMemory + " bytes). That may cause out of memory errors, please tune the "
              + "configuration up. Use the -XX:MaxDirectMemorySize JVM option to raise the JVM maximum direct memory size "
              + "or memory.chunk.size OrientDB option to lower memory chunk size.");

    if (memoryChunkSize > maxCacheSize)
      OLogManager.instance().warn(OMemory.class,
          "The configured memory chunk size (" + memoryChunkSize + " bytes) is larger than the configured maximum cache size ("
              + maxCacheSize + " bytes). That may cause overallocation of a memory which will be wasted, please tune the "
              + "configuration up. Use the storage.diskCache.bufferSize OrientDB option to raise the cache memory size "
              + "or memory.chunk.size OrientDB option to lower memory chunk size.");
  }

  /**
   * Tries to fix some common cache/memory configuration problems:
   * <ul>
   * <li>Cache size is larger than direct memory size.</li>
   * <li>Memory chunk size is larger than cache size.</li>
   * <ul/>
   */
  public static void fixCommonConfigurationProblems() {
    final long maxDirectMemory = OMemory.getConfiguredMaxDirectMemory();
    long diskCacheSize = OGlobalConfiguration.DISK_CACHE_SIZE.getValueAsLong();

    if (maxDirectMemory != -1) {
      final long maxDiskCacheSize = Math.min(maxDirectMemory / 1024 / 1024, Integer.MAX_VALUE);

      if (diskCacheSize > maxDiskCacheSize) {
        OLogManager.instance()
            .info(OGlobalConfiguration.class, "Lowering disk cache size from %,dMB to %,dMB.", diskCacheSize, maxDiskCacheSize);
        OGlobalConfiguration.DISK_CACHE_SIZE.setValue(maxDiskCacheSize);
        diskCacheSize = maxDiskCacheSize;
      }
    }

    final int max32BitCacheSize = 512;
    if (getJavaBitWidth() == 32 && diskCacheSize > max32BitCacheSize) {
      OLogManager.instance()
          .info(OGlobalConfiguration.class, "32 bit JVM is detected. Lowering disk cache size from %,dMB to %,dMB.", diskCacheSize,
              max32BitCacheSize);
      OGlobalConfiguration.DISK_CACHE_SIZE.setValue(max32BitCacheSize);
    }

    if (OGlobalConfiguration.MEMORY_CHUNK_SIZE.getValueAsLong()
        > OGlobalConfiguration.DISK_CACHE_SIZE.getValueAsLong() * 1024 * 1024) {
      final long newChunkSize = Math.min(OGlobalConfiguration.DISK_CACHE_SIZE.getValueAsLong() * 1024 * 1024, Integer.MAX_VALUE);
      OLogManager.instance().info(OGlobalConfiguration.class, "Lowering memory chunk size from %,dB to %,dB.",
          OGlobalConfiguration.MEMORY_CHUNK_SIZE.getValueAsLong(), newChunkSize);
      OGlobalConfiguration.MEMORY_CHUNK_SIZE.setValue(newChunkSize);
    }
  }

  private static int getJavaBitWidth() {
    // Figure out whether bit width of running JVM
    // Most of JREs support property "sun.arch.data.model" which is exactly what we need here
    String dataModel = System.getProperty("sun.arch.data.model", "64"); // By default assume 64bit
    int size = 64;
    try {
      size = Integer.parseInt(dataModel);
    } catch (Throwable t) {
      // Ignore
    }
    return size;
  }

  /**
   * Parses the size specifier formatted in the JVM style, like 1024k or 4g.
   * Following units are supported: k or K – kilobytes, m or M – megabytes, g or G – gigabytes.
   * If no unit provided, it is bytes.
   *
   * @param text the text to parse.
   * @return the parsed size value.
   * @throws IllegalArgumentException if size specifier is not recognized as valid.
   */
  public static long parseVmArgsSize(String text) throws IllegalArgumentException {
    if (text == null)
      throw new IllegalArgumentException("text can't be null");
    if (text.length() == 0)
      throw new IllegalArgumentException("text can't be empty");

    final char unit = text.charAt(text.length() - 1);
    if (Character.isDigit(unit))
      return Long.parseLong(text);

    final long value = Long.parseLong(text.substring(0, text.length() - 1));
    switch (Character.toLowerCase(unit)) {
    case 'g':
      return value * 1024 * 1024 * 1024;
    case 'm':
      return value * 1024 * 1024;
    case 'k':
      return value * 1024;
    }

    throw new IllegalArgumentException("text '" + text + "' is not a size specifier.");
  }

  private OMemory() {
  }
}
