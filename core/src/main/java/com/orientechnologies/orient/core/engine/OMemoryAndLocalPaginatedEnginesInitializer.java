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

package com.orientechnologies.orient.core.engine;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.jnr.ONative;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OMemory;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.storage.cache.OReadCache;
import java.util.Locale;

/**
 * Manages common initialization logic for memory and plocal engines. These engines are tight
 * together through dependency to {@link com.orientechnologies.common.directmemory.OByteBufferPool},
 * which is hard to reconfigure if initialization logic is separate.
 *
 * @author Sergey Sitnikov
 */
public class OMemoryAndLocalPaginatedEnginesInitializer {

  /** Shared initializer instance. */
  public static final OMemoryAndLocalPaginatedEnginesInitializer INSTANCE =
      new OMemoryAndLocalPaginatedEnginesInitializer();

  private boolean initialized = false;

  /**
   * Initializes common parts of memory and plocal engines if not initialized yet. Does nothing if
   * engines already initialized.
   */
  public void initialize() {
    if (initialized) return;
    initialized = true;

    configureDefaults();

    OMemory.checkCacheMemoryConfiguration();
    OMemory.fixCommonConfigurationProblems();
  }

  private void configureDefaults() {
    if (!OGlobalConfiguration.DISK_CACHE_SIZE.isChanged()) configureDefaultDiskCacheSize();

    if (!OGlobalConfiguration.WAL_RESTORE_BATCH_SIZE.isChanged())
      configureDefaultWalRestoreBatchSize();
  }

  private void configureDefaultWalRestoreBatchSize() {
    final long jvmMaxMemory = Runtime.getRuntime().maxMemory();
    if (jvmMaxMemory > 2L * OFileUtils.GIGABYTE)
      // INCREASE WAL RESTORE BATCH SIZE TO 50K INSTEAD OF DEFAULT 1K
      OGlobalConfiguration.WAL_RESTORE_BATCH_SIZE.setValue(50000);
    else if (jvmMaxMemory > 512 * OFileUtils.MEGABYTE)
      // INCREASE WAL RESTORE BATCH SIZE TO 10K INSTEAD OF DEFAULT 1K
      OGlobalConfiguration.WAL_RESTORE_BATCH_SIZE.setValue(10000);
  }

  private void configureDefaultDiskCacheSize() {
    final ONative.MemoryLimitResult osMemory = ONative.instance().getMemoryLimit(true);
    if (osMemory == null) {
      OLogManager.instance()
          .warnNoDb(
              this,
              "Can not determine amount of memory installed on machine, default size of disk cache will be used");
      return;
    }

    final long jvmMaxMemory = OMemory.getCappedRuntimeMaxMemory(2L * 1024 * 1024 * 1024 /* 2GB */);
    OLogManager.instance()
        .infoNoDb(this, "JVM can use maximum %dMB of heap memory", jvmMaxMemory / (1024 * 1024));

    long diskCacheInMB;
    if (osMemory.insideContainer) {
      OLogManager.instance()
          .infoNoDb(
              this,
              "Because OrientDB is running inside a container %s of memory will be left unallocated according to the setting '%s'"
                  + " not taking into account heap memory",
              OGlobalConfiguration.MEMORY_LEFT_TO_CONTAINER.getValueAsString(),
              OGlobalConfiguration.MEMORY_LEFT_TO_CONTAINER.getKey());

      diskCacheInMB =
          (calculateMemoryLeft(
                      osMemory.memoryLimit,
                      OGlobalConfiguration.MEMORY_LEFT_TO_CONTAINER.getKey(),
                      OGlobalConfiguration.MEMORY_LEFT_TO_CONTAINER.getValueAsString())
                  - jvmMaxMemory)
              / (1024 * 1024);
    } else {
      OLogManager.instance()
          .infoNoDb(
              this,
              "Because OrientDB is running outside a container %s of memory will be left "
                  + "unallocated according to the setting '%s' not taking into account heap memory",
              OGlobalConfiguration.MEMORY_LEFT_TO_OS.getValueAsString(),
              OGlobalConfiguration.MEMORY_LEFT_TO_OS.getKey());

      diskCacheInMB =
          (calculateMemoryLeft(
                      osMemory.memoryLimit,
                      OGlobalConfiguration.MEMORY_LEFT_TO_OS.getKey(),
                      OGlobalConfiguration.MEMORY_LEFT_TO_OS.getValueAsString())
                  - jvmMaxMemory)
              / (1024 * 1024);
    }

    if (diskCacheInMB > 0) {
      OLogManager.instance()
          .infoNoDb(
              null,
              "OrientDB auto-config DISKCACHE=%,dMB (heap=%,dMB os=%,dMB)",
              diskCacheInMB,
              jvmMaxMemory / 1024 / 1024,
              osMemory.memoryLimit / 1024 / 1024);

      OGlobalConfiguration.DISK_CACHE_SIZE.setValue(diskCacheInMB);
    } else {
      // LOW MEMORY: SET IT TO 256MB ONLY
      diskCacheInMB = OReadCache.MIN_CACHE_SIZE;
      OLogManager.instance()
          .warnNoDb(
              null,
              "Not enough physical memory available for DISKCACHE: %,dMB (heap=%,dMB). Set lower Maximum Heap (-Xmx "
                  + "setting on JVM) and restart OrientDB. Now running with DISKCACHE="
                  + diskCacheInMB
                  + "MB",
              osMemory.memoryLimit / 1024 / 1024,
              jvmMaxMemory / 1024 / 1024);
      OGlobalConfiguration.DISK_CACHE_SIZE.setValue(diskCacheInMB);

      OLogManager.instance()
          .infoNoDb(
              null,
              "OrientDB config DISKCACHE=%,dMB (heap=%,dMB os=%,dMB)",
              diskCacheInMB,
              jvmMaxMemory / 1024 / 1024,
              osMemory.memoryLimit / 1024 / 1024);
    }
  }

  private long calculateMemoryLeft(long memoryLimit, String parameter, String memoryLeft) {
    if (memoryLeft == null) {
      warningInvalidMemoryLeftValue(parameter, null);
      return memoryLimit;
    }

    memoryLeft = memoryLeft.toLowerCase(Locale.ENGLISH);
    if (memoryLeft.length() < 2) {
      warningInvalidMemoryLeftValue(parameter, memoryLeft);
      return memoryLimit;
    }

    final char lastChar = memoryLeft.charAt(memoryLeft.length() - 1);
    if (lastChar == '%') {
      final String percentValue = memoryLeft.substring(0, memoryLeft.length() - 1);

      final int percent;
      try {
        percent = Integer.parseInt(percentValue);
      } catch (NumberFormatException e) {
        warningInvalidMemoryLeftValue(parameter, memoryLeft);
        return memoryLimit;
      }

      if (percent < 0 || percent >= 100) {
        warningInvalidMemoryLeftValue(parameter, memoryLeft);
        return memoryLimit;
      }

      return (int) ((memoryLimit * (100.0 - percent)) / 100.0);
    } else if (lastChar == 'b') {
      final String bytesValue = memoryLeft.substring(0, memoryLeft.length() - 1);
      final long bytes;
      try {
        bytes = Long.parseLong(bytesValue);
      } catch (NumberFormatException e) {
        warningInvalidMemoryLeftValue(parameter, memoryLeft);
        return memoryLimit;
      }

      if (bytes < 0) {
        warningInvalidMemoryLeftValue(parameter, memoryLeft);
        return memoryLimit;
      }

      return memoryLimit - bytes;
    } else if (lastChar == 'k') {
      final String kbytesValue = memoryLeft.substring(0, memoryLeft.length() - 1);
      final long kbytes;
      try {
        kbytes = Long.parseLong(kbytesValue);
      } catch (NumberFormatException e) {
        warningInvalidMemoryLeftValue(parameter, memoryLeft);
        return memoryLimit;
      }

      final long bytes = kbytes * 1024;
      if (bytes < 0) {
        warningInvalidMemoryLeftValue(parameter, memoryLeft);
        return memoryLimit;
      }

      return memoryLimit - bytes;
    } else if (lastChar == 'm') {
      final String mbytesValue = memoryLeft.substring(0, memoryLeft.length() - 1);
      final long mbytes;
      try {
        mbytes = Long.parseLong(mbytesValue);
      } catch (NumberFormatException e) {
        warningInvalidMemoryLeftValue(parameter, memoryLeft);
        return memoryLimit;
      }

      final long bytes = mbytes * 1024 * 1024;
      if (bytes < 0) {
        warningInvalidMemoryLeftValue(parameter, memoryLeft);
        return memoryLimit;
      }

      return memoryLimit - bytes;
    } else if (lastChar == 'g') {
      final String gbytesValue = memoryLeft.substring(0, memoryLeft.length() - 1);
      final long gbytes;
      try {
        gbytes = Long.parseLong(gbytesValue);
      } catch (NumberFormatException e) {
        warningInvalidMemoryLeftValue(parameter, memoryLeft);
        return memoryLimit;
      }

      final long bytes = gbytes * 1024 * 1024 * 1024;
      if (bytes < 0) {
        warningInvalidMemoryLeftValue(parameter, memoryLeft);
        return memoryLimit;
      }

      return memoryLimit - bytes;
    } else {
      warningInvalidMemoryLeftValue(parameter, memoryLeft);
      return memoryLimit;
    }
  }

  private void warningInvalidMemoryLeftValue(String parameter, String memoryLeft) {
    OLogManager.instance()
        .warnNoDb(
            this,
            "Invalid value of '%s' parameter ('%s') memory limit will not be decreased",
            memoryLeft,
            parameter);
  }
}
