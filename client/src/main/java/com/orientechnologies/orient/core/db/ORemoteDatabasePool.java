package com.orientechnologies.orient.core.db;

import com.orientechnologies.orient.client.remote.OStorageRemote;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentRemote;

/**
 * Created by tglman on 07/07/16.
 */
public class ORemoteDatabasePool extends ODatabaseDocumentRemote {

  private ODatabasePoolInternal pool;

  public ORemoteDatabasePool(ODatabasePoolInternal pool, OStorageRemote storage) {
    super(storage);
    this.pool = pool;
  }

  @Override
  public void close() {
    super.setStatus(ODatabase.STATUS.CLOSED);
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