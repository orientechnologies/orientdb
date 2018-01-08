package com.orientechnologies.orient.core.db;

import com.orientechnologies.orient.client.remote.OStorageRemote;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentRemote;

/**
 * Created by tglman on 07/07/16.
 */
public class ODatabaseDocumentRemotePooled extends ODatabaseDocumentRemote {

  private ODatabasePoolInternal pool;

  public ODatabaseDocumentRemotePooled(ODatabasePoolInternal pool, OStorageRemote storage) {
    super(storage);
    this.pool = pool;
  }

  @Override
  public void close() {
    if (isClosed())
      return;
    closeActiveQueries();
    rollback(true);
    super.setStatus(ODatabase.STATUS.CLOSED);
    getLocalCache().clear();
    ODatabaseRecordThreadLocal.instance().remove();
    pool.release(this);
  }

  @Override
  public ODatabaseDocumentInternal copy() {
    return (ODatabaseDocumentInternal) pool.acquire();
  }

  public void reuse() {
    activateOnCurrentThread();
    setStatus(ODatabase.STATUS.OPEN);
  }

  public void realClose() {
    activateOnCurrentThread();
    super.close();
  }
}