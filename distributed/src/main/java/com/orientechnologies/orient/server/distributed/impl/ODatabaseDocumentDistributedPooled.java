package com.orientechnologies.orient.server.distributed.impl;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabasePoolInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.server.hazelcast.OHazelcastPlugin;

/**
 * Created by tglman on 30/03/17.
 */
public class ODatabaseDocumentDistributedPooled extends ODatabaseDocumentDistributed {

  private ODatabasePoolInternal pool;

  public ODatabaseDocumentDistributedPooled(ODatabasePoolInternal pool, OStorage storage, OHazelcastPlugin hazelcastPlugin) {
    super(storage, hazelcastPlugin);
    this.pool = pool;
  }

  @Override
  public void close() {
    if (isClosed())
      return;
    closeActiveQueries();
    rollback(true);
    super.setStatus(STATUS.CLOSED);
    ODatabaseRecordThreadLocal.instance().remove();
    getLocalCache().clear();
    pool.release(this);
  }

  @Override
  public ODatabaseDocumentInternal copy() {
    return (ODatabaseDocumentInternal) pool.acquire();
  }

  public void reuse() {
    activateOnCurrentThread();
    setStatus(STATUS.OPEN);
  }

  public void realClose() {
    activateOnCurrentThread();
    super.close();
  }
}
