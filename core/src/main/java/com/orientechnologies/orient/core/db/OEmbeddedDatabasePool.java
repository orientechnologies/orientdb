package com.orientechnologies.orient.core.db;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentEmbedded;
import com.orientechnologies.orient.core.storage.OStorage;

/**
 * Created by tglman on 07/07/16.
 */
public class OEmbeddedDatabasePool extends ODatabaseDocumentEmbedded {

  private OEmbeddedPoolByFactory pool;

  public OEmbeddedDatabasePool(OEmbeddedPoolByFactory pool, OStorage storage) {
    super(storage);
    this.pool = pool;
  }

  @Override
  public void close() {
    super.setStatus(STATUS.CLOSED);
    pool.release(this);
  }

  public void reuse() {
    setStatus(STATUS.OPEN);
  }

  public void realClose() {
    activateOnCurrentThread();
    super.close();
  }

}
