package com.orientechnologies.orient.object.db;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.object.ODatabaseObject;

/**
 * Created by tglman on 13/01/17.
 */
public class ODatabaseObjectPool implements AutoCloseable {
  private ODatabasePool pool;

  public ODatabaseObjectPool(ODatabasePool pool) {
    this.pool = pool;
  }

  public ODatabaseObject acquire() {
    return new OObjectDatabaseTx((ODatabaseDocumentInternal) pool.acquire());
  }

  public void close() {
    this.pool.close();
  }

}
