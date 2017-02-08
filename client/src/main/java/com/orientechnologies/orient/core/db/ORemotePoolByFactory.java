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

import com.orientechnologies.common.concur.resource.OResourcePool;
import com.orientechnologies.common.concur.resource.OResourcePoolListener;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;

/**
 * Created by tglman on 07/07/16.
 */
public class ORemotePoolByFactory implements ODatabasePoolInternal {
  private final OResourcePool<Void, ORemoteDatabasePool> pool;
  private final OrientDBRemote                           factory;
  private final OrientDBConfig                           config;

  public ORemotePoolByFactory(OrientDBRemote factory, String database, String user, String password, OrientDBConfig config) {
    int max = factory.getConfigurations().getConfigurations().getValueAsInteger(OGlobalConfiguration.DB_POOL_MAX);
    pool = new OResourcePool(max, new OResourcePoolListener<Void, ORemoteDatabasePool>() {
      @Override
      public ORemoteDatabasePool createNewResource(Void iKey, Object... iAdditionalArgs) {
        return factory.poolOpen(database, user, password, ORemotePoolByFactory.this);
      }

      @Override
      public boolean reuseResource(Void iKey, Object[] iAdditionalArgs, ORemoteDatabasePool iValue) {
        iValue.reuse();
        return true;
      }
    });
    this.factory = factory;
    this.config = config;
  }

  @Override
  public synchronized ODatabaseDocument acquire() {
    // TODO:use configured timeout no property exist yet
    return pool.getResource(null, 1000);
  }

  @Override
  public synchronized void close() {
    for (ORemoteDatabasePool res : pool.getAllResources()) {
      res.realClose();
    }
    pool.close();
    factory.removePool(this);
  }

  public OrientDBConfig getConfig() {
    return config;
  }

  public synchronized void release(ORemoteDatabasePool oPoolDatabaseDocument) {
    pool.returnResource(oPoolDatabaseDocument);
  }
}