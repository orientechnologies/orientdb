package com.orientechnologies.orient.server.distributed.impl.metadata;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.OScenarioThreadLocal;
import com.orientechnologies.orient.core.index.OIndexManagerAbstract;
import com.orientechnologies.orient.core.index.OIndexManagerShared;
import com.orientechnologies.orient.core.type.ODocumentWrapper;

/**
 * Created by tglman on 23/06/17.
 */
public class OIndexManagerDistributed extends OIndexManagerShared {

  @Override
  public OIndexManagerAbstract load(ODatabaseDocumentInternal database) {
    OScenarioThreadLocal.executeAsDistributed(() -> {
      super.load(database);
      return null;
    });
    return this;
  }
}
