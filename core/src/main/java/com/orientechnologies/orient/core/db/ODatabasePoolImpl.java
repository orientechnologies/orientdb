/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.core.db;

import static com.orientechnologies.orient.core.config.OGlobalConfiguration.DB_POOL_ACQUIRE_TIMEOUT;
import static com.orientechnologies.orient.core.config.OGlobalConfiguration.DB_POOL_MAX;
import static com.orientechnologies.orient.core.config.OGlobalConfiguration.DB_POOL_MIN;

import com.orientechnologies.common.concur.resource.OResourcePool;
import com.orientechnologies.common.concur.resource.OResourcePoolListener;
import com.orientechnologies.orient.core.exception.OAcquireTimeoutException;
import com.orientechnologies.orient.core.exception.ODatabaseException;

/** Created by tglman on 07/07/16. */
public class ODatabasePoolImpl implements ODatabasePoolInternal {
  private volatile OResourcePool<Void, ODatabaseDocumentInternal> pool;
  private final OrientDBInternal factory;
  private final OrientDBConfig config;
  private volatile long lastCloseTime = System.currentTimeMillis();

  public ODatabasePoolImpl(
      OrientDBInternal factory,
      String database,
      String user,
      String password,
      OrientDBConfig config) {
    int max = config.getConfigurations().getValueAsInteger(DB_POOL_MAX);
    int min = config.getConfigurations().getValueAsInteger(DB_POOL_MIN);
    this.factory = factory;
    this.config = config;
    pool =
        new OResourcePool(
            min,
            max,
            new OResourcePoolListener<Void, ODatabaseDocumentInternal>() {
              @Override
              public ODatabaseDocumentInternal createNewResource(
                  Void iKey, Object... iAdditionalArgs) {
                return factory.poolOpen(database, user, password, ODatabasePoolImpl.this);
              }

              @Override
              public boolean reuseResource(
                  Void iKey, Object[] iAdditionalArgs, ODatabaseDocumentInternal iValue) {
                if (iValue.getStorage().isClosed()) {
                  return false;
                }
                iValue.reuse();
                return true;
              }
            });

    ODatabaseRecordThreadLocal.instance().remove();
  }

  @Override
  public ODatabaseSession acquire() throws OAcquireTimeoutException {
    OResourcePool<Void, ODatabaseDocumentInternal> p;
    synchronized (this) {
      p = pool;
    }
    if (p != null) {
      return p.getResource(
          null, config.getConfigurations().getValueAsLong(DB_POOL_ACQUIRE_TIMEOUT));
    } else {
      throw new ODatabaseException("The pool is closed");
    }
  }

  @Override
  public synchronized void close() {
    OResourcePool<Void, ODatabaseDocumentInternal> p;
    synchronized (this) {
      p = pool;
      pool = null;
    }
    if (p != null) {
      for (ODatabaseDocumentInternal res : p.getAllResources()) {
        res.realClose();
      }
      p.close();
      factory.removePool(this);
    }
  }

  public void release(ODatabaseDocumentInternal database) {
    OResourcePool<Void, ODatabaseDocumentInternal> p;
    synchronized (this) {
      p = pool;
    }
    if (p != null) {
      pool.returnResource(database);
    } else {
      throw new ODatabaseException("The pool is closed");
    }
    lastCloseTime = System.currentTimeMillis();
  }

  public boolean isUnused() {
    if (pool == null) {
      return true;
    } else {
      return pool.getResourcesOutCount() == 0;
    }
  }

  public long getLastCloseTime() {
    return lastCloseTime;
  }

  public OrientDBConfig getConfig() {
    return config;
  }

  @Override
  public boolean isClosed() {
    return pool == null;
  }
}
