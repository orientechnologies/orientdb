package com.orientechnologies.orient.server.distributed.impl;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabasePoolInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.storage.OStorage;

/** Created by tglman on 30/03/17. */
public class ODatabaseDocumentDistributedPooled extends ODatabaseDocumentDistributed {

  private ODatabasePoolInternal pool;

  public ODatabaseDocumentDistributedPooled(
      ODatabasePoolInternal pool, OStorage storage, ODistributedPlugin distributedPlugin) {
    super(storage, distributedPlugin);
    this.pool = pool;
  }

  @Override
  public void close() {
    if (isClosed()) return;
    internalClose(true);
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
    ODatabaseDocumentInternal old = ODatabaseRecordThreadLocal.instance().getIfDefined();
    try {
      activateOnCurrentThread();
      super.close();
    } finally {
      if (old == null) {
        ODatabaseRecordThreadLocal.instance().remove();
      } else {
        ODatabaseRecordThreadLocal.instance().set(old);
      }
    }
  }
}
