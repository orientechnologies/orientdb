package com.orientechnologies.orient.core.db;

import com.orientechnologies.common.concur.resource.OResourcePool;
import com.orientechnologies.common.concur.resource.OResourcePoolListener;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;

/**
 * Created by tglman on 07/07/16.
 */
public class OEmbeddedPoolByFactory implements OPool<ODatabaseDocument> {
  private final OResourcePool<Void, OEmbeddedDatabasePool> pool;
  private final OEmbeddedDBFactory                         factory;

  public OEmbeddedPoolByFactory(OEmbeddedDBFactory factory, String database, String user, String password) {
    int max = factory.getConfigurations().getConfigurations().getValueAsInteger(OGlobalConfiguration.DB_POOL_MAX);
    //TODO use configured max
    pool = new OResourcePool(max, new OResourcePoolListener<Void, OEmbeddedDatabasePool>() {
      @Override
      public OEmbeddedDatabasePool createNewResource(Void iKey, Object... iAdditionalArgs) {
        return factory.poolOpen(database, user, password, OEmbeddedPoolByFactory.this);
      }

      @Override
      public boolean reuseResource(Void iKey, Object[] iAdditionalArgs, OEmbeddedDatabasePool iValue) {
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
    for (OEmbeddedDatabasePool res : pool.getAllResources()) {
      res.realClose();
    }
    pool.close();
    factory.removePool(this);
  }

  public synchronized void release(OEmbeddedDatabasePool oPoolDatabaseDocument) {
    pool.returnResource(oPoolDatabaseDocument);
  }
}
