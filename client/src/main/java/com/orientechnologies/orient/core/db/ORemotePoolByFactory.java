package com.orientechnologies.orient.core.db;

import com.orientechnologies.common.concur.resource.OResourcePool;
import com.orientechnologies.common.concur.resource.OResourcePoolListener;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;

/**
 * Created by tglman on 07/07/16.
 */
public class ORemotePoolByFactory implements OPool<ODatabaseDocument> {
  private OResourcePool<Void, ORemoteDatabasePool> pool;

  public ORemotePoolByFactory(ORemoteDBFactory factory, String database, String user, String password, OrientDBSettings config) {
    //TODO use configured max
    pool = new OResourcePool(10, new OResourcePoolListener<Void, ORemoteDatabasePool>() {
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
  }

  @Override
  public synchronized ODatabaseDocument acquire() {
    //TODO:use configured timeout
    return pool.getResource(null, 1000);
  }

  @Override
  public synchronized void close() {
    for (ORemoteDatabasePool res : pool.getAllResources()) {
      res.realClose();
    }
    pool.close();
  }

  public synchronized void release(ORemoteDatabasePool oPoolDatabaseDocument) {
    pool.returnResource(oPoolDatabaseDocument);
  }
}