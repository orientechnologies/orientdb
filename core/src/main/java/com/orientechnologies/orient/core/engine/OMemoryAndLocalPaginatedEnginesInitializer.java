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
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OMemory;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.storage.cache.local.twoq.O2QCache;

/**
 * Manages common initialization logic for memory and plocal engines. These engines are tight together through
 * dependency to {@link com.orientechnologies.common.directmemory.OByteBufferPool}, which is hard to reconfigure
 * if initialization logic is separate.
 *
 * @author Sergey Sitnikov
 */
public class OMemoryAndLocalPaginatedEnginesInitializer {

  /**
   * Shared initializer instance.
   */
  public static final OMemoryAndLocalPaginatedEnginesInitializer INSTANCE = new OMemoryAndLocalPaginatedEnginesInitializer();

  private boolean initialized = false;

  /**
   * Initializes common parts of memory and plocal engines if not initialized yet. Does nothing if engines already initialized.
   */
  public void initialize() {
    if (initialized)
      return;
    initialized = true;

    configureDefaults();

    OMemory.checkDirectMemoryConfiguration();
    OMemory.checkByteBufferPoolConfiguration();
    OMemory.checkCacheMemoryConfiguration();

    OMemory.fixCommonConfigurationProblems();
  }

  private void configureDefaults() {
    if (System.getProperty(OGlobalConfiguration.DISK_CACHE_SIZE.getKey()) == null)
      configureDefaultDiskCacheSize();

    if (System.getProperty(OGlobalConfiguration.WAL_RESTORE_BATCH_SIZE.getKey()) == null)
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
    final long osMemory = OMemory.getPhysicalMemorySize();
    final long jvmMaxMemory = OMemory.getCappedRuntimeMaxMemory(2L * 1024 * 1024 * 1024 /* 2GB */);
    final long maxDirectMemory = OMemory.getConfiguredMaxDirectMemory();

    if (maxDirectMemory == -1) {
      final long diskCacheInMB = jvmMaxMemory / 1024 / 1024;
      OLogManager.instance().info(this,
          "OrientDB auto-config DISKCACHE=%,dMB (heap=%,dMB direct=%,dMB os=%,dMB), assuming maximum direct memory size "
              + "equals to maximum JVM heap size", diskCacheInMB, diskCacheInMB, diskCacheInMB, osMemory / 1024 / 1024);
      OGlobalConfiguration.DISK_CACHE_SIZE.setValue(diskCacheInMB);
      OGlobalConfiguration.MEMORY_CHUNK_SIZE
          .setValue(Math.min(diskCacheInMB * 1024 * 1024, OGlobalConfiguration.MEMORY_CHUNK_SIZE.getValueAsLong()));
      return;
    }

    final long maxDirectMemoryInMB = maxDirectMemory / 1024 / 1024;

    // DISK-CACHE IN MB = OS MEMORY - MAX HEAP JVM MEMORY - 2 GB
    long diskCacheInMB = (osMemory - jvmMaxMemory) / (1024 * 1024) - 2 * 1024;
    if (diskCacheInMB > 0) {
      diskCacheInMB = Math.min(diskCacheInMB, maxDirectMemoryInMB);
      OLogManager.instance().info(this, "OrientDB auto-config DISKCACHE=%,dMB (heap=%,dMB direct=%,dMB os=%,dMB)", diskCacheInMB,
          jvmMaxMemory / 1024 / 1024, maxDirectMemoryInMB, osMemory / 1024 / 1024);

      OGlobalConfiguration.DISK_CACHE_SIZE.setValue(diskCacheInMB);
      OGlobalConfiguration.MEMORY_CHUNK_SIZE
          .setValue(Math.min(diskCacheInMB * 1024 * 1024, OGlobalConfiguration.MEMORY_CHUNK_SIZE.getValueAsLong()));
    } else {
      // LOW MEMORY: SET IT TO 256MB ONLY
      diskCacheInMB = Math.min(O2QCache.MIN_CACHE_SIZE, maxDirectMemoryInMB);
      OLogManager.instance().warn(this,
          "Not enough physical memory available for DISKCACHE: %,dMB (heap=%,dMB direct=%,dMB). Set lower Maximum Heap (-Xmx "
              + "setting on JVM) and restart OrientDB. Now running with DISKCACHE=" + diskCacheInMB + "MB", osMemory / 1024 / 1024,
          jvmMaxMemory / 1024 / 1024, maxDirectMemoryInMB);
      OGlobalConfiguration.DISK_CACHE_SIZE.setValue(diskCacheInMB);
      OGlobalConfiguration.MEMORY_CHUNK_SIZE
          .setValue(Math.min(diskCacheInMB * 1024 * 1024, OGlobalConfiguration.MEMORY_CHUNK_SIZE.getValueAsLong()));

      OLogManager.instance().info(this, "OrientDB config DISKCACHE=%,dMB (heap=%,dMB direct=%,dMB os=%,dMB)", diskCacheInMB,
          jvmMaxMemory / 1024 / 1024, maxDirectMemoryInMB, osMemory / 1024 / 1024);
    }
  }
}
