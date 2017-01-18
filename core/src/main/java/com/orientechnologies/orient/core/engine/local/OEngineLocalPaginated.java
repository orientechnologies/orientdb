/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
 *  * For more information: http://www.orientechnologies.com
 *
 */

package com.orientechnologies.orient.core.engine.local;

import com.orientechnologies.common.collection.closabledictionary.OClosableLinkedContainer;
import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.engine.OEngineAbstract;
import com.orientechnologies.orient.core.engine.OMemoryAndLocalPaginatedEnginesInitializer;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.cache.local.twoq.O2QCache;
import com.orientechnologies.orient.core.storage.fs.OFileClassic;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OLocalPaginatedStorage;

import java.util.Map;

/**
 * @author Andrey Lomakin
 * @since 28.03.13
 */
public class OEngineLocalPaginated extends OEngineAbstract {
  public static final String NAME = "plocal";

  private volatile O2QCache readCache;

  protected final OClosableLinkedContainer<Long, OFileClassic> files = new OClosableLinkedContainer<Long, OFileClassic>(
      OGlobalConfiguration.OPEN_FILES_LIMIT.getValueAsInteger());

  public OEngineLocalPaginated() {
  }

  @Override
  public void startup() {
    OMemoryAndLocalPaginatedEnginesInitializer.INSTANCE.initialize();
    super.startup();

    readCache = new O2QCache(calculateReadCacheMaxMemory(OGlobalConfiguration.DISK_CACHE_SIZE.getValueAsLong() * 1024 * 1024),
        OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024, true,
        OGlobalConfiguration.DISK_CACHE_PINNED_PAGES.getValueAsInteger());

    try {
      if (OByteBufferPool.instance() != null)
        OByteBufferPool.instance().registerMBean();
    } catch (Exception e) {
      OLogManager.instance().error(this, "MBean for byte buffer pool cannot be registered", e);
    }
  }

  private long calculateReadCacheMaxMemory(final long cacheSize) {
    return (long) (cacheSize * ((100 - OGlobalConfiguration.DISK_WRITE_CACHE_PART.getValueAsInteger()) / 100.0));
  }

  /**
   * @param cacheSize Cache size in bytes.
   * @see O2QCache#changeMaximumAmountOfMemory(long)
   */
  public void changeCacheSize(final long cacheSize) {
    if (readCache != null)
      readCache.changeMaximumAmountOfMemory(calculateReadCacheMaxMemory(cacheSize));

    //otherwise memory size will be set during cache initialization.
  }

  public OStorage createStorage(final String dbName, final Map<String, String> configuration) {
    try {

      return new OLocalPaginatedStorage(dbName, dbName, getMode(configuration), generateStorageId(), readCache, files);
    } catch (Exception e) {
      final String message =
          "Error on opening database: " + dbName + ". Current location is: " + new java.io.File(".").getAbsolutePath();
      OLogManager.instance().error(this, message, e);

      throw OException.wrapException(new ODatabaseException(message), e);
    }
  }

  public String getName() {
    return NAME;
  }

  public O2QCache getReadCache() {
    return readCache;
  }

  @Override
  public String getNameFromPath(String dbPath) {
    return OIOUtils.getRelativePathIfAny(dbPath, null);
  }

  @Override
  public void shutdown() {
    try {
      readCache.clear();
      files.clear();

      try {
        if (OByteBufferPool.instance() != null)
          OByteBufferPool.instance().unregisterMBean();
      } catch (Exception e) {
        OLogManager.instance().error(this, "MBean for byte buffer pool cannot be unregistered", e);
      }
    } finally {
      super.shutdown();
    }
  }
}
