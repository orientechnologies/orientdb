package com.orientechnologies.orient.server.distributed.impl.metadata;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.OScenarioThreadLocal;
import com.orientechnologies.orient.core.metadata.schema.OClassImpl;
import com.orientechnologies.orient.core.metadata.schema.OSchemaEmbedded;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OAutoshardedStorage;

/**
 * Created by tglman on 22/06/17.
 */
public class OSchemaDistributed extends OSchemaEmbedded {

  private volatile boolean acquiredDistributeLock = false;

  protected OClassImpl createClassInstance(String className, int[] clusterIds) {
    return new OClassDistributed(this, className, clusterIds);
  }

  @Override
  protected OClassImpl createClassInstance(ODocument c) {
    return new OClassDistributed(this, c, (String) c.field("name"));
  }

  public void acquireSchemaWriteLock(ODatabaseDocumentInternal database) {
    if (!OScenarioThreadLocal.INSTANCE.isRunModeDistributed()) {
      ((OAutoshardedStorage) database.getStorage()).acquireDistributedExclusiveLock(0);
      acquiredDistributeLock = true;
    }
    super.acquireSchemaWriteLock(database);
  }

  @Override
  public void releaseSchemaWriteLock(ODatabaseDocumentInternal database) {
    try {
      super.releaseSchemaWriteLock(database);
    } finally {
      if (acquiredDistributeLock) {
        acquiredDistributeLock = false;
        ((OAutoshardedStorage) database.getStorage()).releaseDistributedExclusiveLock();
      }
    }
  }
}
