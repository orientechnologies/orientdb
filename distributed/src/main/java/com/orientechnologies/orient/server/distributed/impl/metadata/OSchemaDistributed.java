package com.orientechnologies.orient.server.distributed.impl.metadata;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseLifecycleListener;
import com.orientechnologies.orient.core.db.ODatabaseListener;
import com.orientechnologies.orient.core.db.OScenarioThreadLocal;
import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexManager;
import com.orientechnologies.orient.core.metadata.schema.*;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.ORule;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.storage.OAutoshardedStorage;
import com.orientechnologies.orient.core.storage.OStorage;

import java.util.*;

/**
 * Created by tglman on 22/06/17.
 */
public class OSchemaDistributed extends OSchemaEmbedded {

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
    }
    super.acquireSchemaWriteLock(database);
  }

  @Override
  public void releaseSchemaWriteLock(ODatabaseDocumentInternal database) {
    try {
      super.releaseSchemaWriteLock(database);
    } finally {
      if (!OScenarioThreadLocal.INSTANCE.isRunModeDistributed()) {
        ((OAutoshardedStorage) database.getStorage()).releaseDistributedExclusiveLock();
      }
    }
  }
}
