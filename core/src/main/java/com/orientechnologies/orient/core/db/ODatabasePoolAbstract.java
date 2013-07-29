/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.orient.core.db;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.orientechnologies.common.concur.lock.OLockException;
import com.orientechnologies.common.concur.resource.OResourcePool;
import com.orientechnologies.common.concur.resource.OResourcePoolListener;
import com.orientechnologies.common.concur.resource.OSharedResourceAdaptive;
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.OOrientListener;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.storage.OStorage;

public abstract class ODatabasePoolAbstract<DB extends ODatabase> extends OSharedResourceAdaptive implements
    OResourcePoolListener<String, DB>, OOrientListener {

  private final HashMap<String, OResourcePool<String, DB>> pools = new HashMap<String, OResourcePool<String, DB>>();
  private int                                              maxSize;
  private int                                              timeout;
  protected Object                                         owner;

  public ODatabasePoolAbstract(final Object iOwner, final int iMinSize, final int iMaxSize) {
    this(iOwner, iMinSize, iMaxSize, OGlobalConfiguration.CLIENT_CONNECT_POOL_WAIT_TIMEOUT.getValueAsInteger());
  }

  public ODatabasePoolAbstract(final Object iOwner, final int iMinSize, final int iMaxSize, final int iTimeout) {
    super(OGlobalConfiguration.ENVIRONMENT_CONCURRENT.getValueAsBoolean(), OGlobalConfiguration.STORAGE_LOCK_TIMEOUT
        .getValueAsInteger(), true);

    maxSize = iMaxSize;
    timeout = iTimeout;
    owner = iOwner;
    Orient.instance().registerListener(this);
  }

  public DB acquire(final String iURL, final String iUserName, final String iUserPassword) throws OLockException {
    return acquire(iURL, iUserName, iUserPassword, null);
  }

  public DB acquire(final String iURL, final String iUserName, final String iUserPassword, final Map<String, Object> iOptionalParams)
      throws OLockException {
    final String dbPooledName = OIOUtils.getUnixFileName(iUserName + "@" + iURL);

    acquireExclusiveLock();
    try {

      OResourcePool<String, DB> pool = pools.get(dbPooledName);
      if (pool == null)
        // CREATE A NEW ONE
        pool = new OResourcePool<String, DB>(maxSize, this);

      final DB db = pool.getResource(iURL, timeout, iUserName, iUserPassword, iOptionalParams);

      // PUT IN THE POOL MAP ONLY IF AUTHENTICATION SUCCEED
      pools.put(dbPooledName, pool);
      return db;

    } finally {
      releaseExclusiveLock();
    }
  }

  public void release(final DB iDatabase) {
    final String dbPooledName = iDatabase instanceof ODatabaseComplex ? ((ODatabaseComplex<?>) iDatabase).getUser().getName() + "@"
        + iDatabase.getURL() : iDatabase.getURL();

    acquireSharedLock();
    try {

      final OResourcePool<String, DB> pool = pools.get(dbPooledName);
      if (pool == null)
        throw new OLockException("Cannot release a database URL not acquired before. URL: " + iDatabase.getName());

      pool.returnResource(iDatabase);

    } finally {
      releaseSharedLock();
    }
  }

  public DB reuseResource(final String iKey, final DB iValue) {
    return iValue;
  }

  public Map<String, OResourcePool<String, DB>> getPools() {
    acquireSharedLock();
    try {

      return Collections.unmodifiableMap(pools);

    } finally {
      releaseSharedLock();
    }
  }

  /**
   * Closes all the databases.
   */
  public void close() {
    acquireSharedLock();
    try {

      for (Entry<String, OResourcePool<String, DB>> pool : pools.entrySet()) {
        for (DB db : pool.getValue().getResources()) {
          pool.getValue().close();
          try {
            OLogManager.instance().debug(this, "Closing pooled database '%s'...", db.getName());
            ((ODatabasePooled) db).forceClose();
            OLogManager.instance().debug(this, "OK", db.getName());
          } catch (Exception e) {
            OLogManager.instance().debug(this, "Error: %d", e.toString());
          }
        }
      }

    } finally {
      releaseSharedLock();
    }
  }

  public void remove(final String iName, final String iUser) {
    remove(iUser + "@" + iName);
  }

  public void remove(final String iPoolName) {
    acquireExclusiveLock();
    try {

      final OResourcePool<String, DB> pool = pools.get(iPoolName);

      if (pool != null) {
        for (DB db : pool.getResources()) {
          if (db.getStorage().getStatus() == OStorage.STATUS.OPEN)
            try {
              OLogManager.instance().debug(this, "Closing pooled database '%s'...", db.getName());
              ((ODatabasePooled) db).forceClose();
              OLogManager.instance().debug(this, "OK", db.getName());
            } catch (Exception e) {
              OLogManager.instance().debug(this, "Error: %d", e.toString());
            }

        }
        pool.close();
        pools.remove(iPoolName);
      }

    } finally {
      releaseExclusiveLock();
    }
  }

  public int getMaxSize() {
    return maxSize;
  }

  public void onStorageRegistered(final OStorage iStorage) {
  }

  /**
   * Removes from memory the pool associated to the closed storage. This avoids pool open against closed storages.
   */
  public void onStorageUnregistered(final OStorage iStorage) {
    final String storageURL = iStorage.getURL();

    acquireSharedLock();
    try {
      Set<String> poolToClose = null;

      for (Entry<String, OResourcePool<String, DB>> e : pools.entrySet()) {
        final int pos = e.getKey().indexOf("@");
        final String dbName = e.getKey().substring(pos + 1);
        if (storageURL.equals(dbName)) {
          if (poolToClose == null)
            poolToClose = new HashSet<String>();

          poolToClose.add(e.getKey());
        }
      }

      if (poolToClose != null)
        for (String pool : poolToClose)
          remove(pool);

    } finally {
      releaseExclusiveLock();
    }
  }
}
