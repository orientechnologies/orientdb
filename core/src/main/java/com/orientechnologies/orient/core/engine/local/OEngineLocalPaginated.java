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

import java.io.IOException;
import java.util.Map;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.engine.OEngineAbstract;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.cache.local.O2QCache;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OLocalPaginatedStorage;

/**
 * @author Andrey Lomakin
 * @since 28.03.13
 */
public class OEngineLocalPaginated extends OEngineAbstract {
  public static final String NAME = "plocal";

  private final O2QCache     readCache;

  public OEngineLocalPaginated() {
    readCache = new O2QCache(calculateReadCacheMaxMemory(OGlobalConfiguration.DISK_CACHE_SIZE.getValueAsLong() * 1024 * 1024),
        OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024, true,
        OGlobalConfiguration.DISK_CACHE_PINNED_PAGES.getValueAsInteger());
    try {
      readCache.registerMBean();
    } catch (Exception e) {
      OLogManager.instance().error(this, "MBean for read cache cannot be registered", e);
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
    readCache.changeMaximumAmountOfMemory(calculateReadCacheMaxMemory(cacheSize));
  }

  public OStorage createStorage(final String dbName, final Map<String, String> configuration) {
    try {
      // GET THE STORAGE
      return new OLocalPaginatedStorage(dbName, dbName, getMode(configuration), generateStorageId(), readCache);

    } catch (Throwable t) {
      OLogManager.instance().error(this,
          "Error on opening database: " + dbName + ". Current location is: " + new java.io.File(".").getAbsolutePath(), t,
          ODatabaseException.class);
    }
    return null;
  }

  public String getName() {
    return NAME;
  }

  public boolean isShared() {
    return true;
  }

  @Override
  public void shutdown() {
    super.shutdown();

    readCache.clear();
    try {
      readCache.unregisterMBean();
    } catch (Exception e) {
      OLogManager.instance().error(this, "MBean for read cache cannot be unregistered", e);
    }

  }
}
