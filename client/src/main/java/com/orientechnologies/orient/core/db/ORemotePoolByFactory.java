package com.orientechnologies.orient.core.db;

import com.orientechnologies.common.concur.resource.OResourcePool;
import com.orientechnologies.common.concur.resource.OResourcePoolListener;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;

/**
 * Created by tglman on 07/07/16.
 */
public class ORemotePoolByFactory implements OPool<ODatabaseDocument> {
  private final OResourcePool<Void, ORemoteDatabasePool> pool;
  private final ORemoteDBFactory                         factory;

  public ORemotePoolByFactory(ORemoteDBFactory factory, String database, String user, String password) {
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
  }

  @Override
  public synchronized ODatabaseDocument acquire() {
    //TODO:use configured timeout no property exist yet
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

  public synchronized void release(ORemoteDatabasePool oPoolDatabaseDocument) {
    pool.returnResource(oPoolDatabaseDocument);
  }
}