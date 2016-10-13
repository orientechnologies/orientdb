package com.orientechnologies.orient.core.db;

import com.orientechnologies.orient.client.remote.OStorageRemote;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentRemote;

/**
 * Created by tglman on 07/07/16.
 */
public class ORemoteDatabasePool extends ODatabaseDocumentRemote {

  private ORemotePoolByFactory pool;

  public ORemoteDatabasePool(ORemotePoolByFactory pool, OStorageRemote storage) {
    super(storage);
    this.pool = pool;
  }

  @Override
  public void close() {
    super.setStatus(ODatabase.STATUS.CLOSED);
    pool.release(this);
  }

  public void reuse() {
    setStatus(ODatabase.STATUS.OPEN);
  }

  public void realClose() {
    activateOnCurrentThread();
    super.close();
  }
}