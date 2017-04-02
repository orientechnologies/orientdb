package com.orientechnologies.orient.server.distributed.impl;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentEmbedded;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.server.distributed.ODistributedException;
import com.orientechnologies.orient.server.hazelcast.OHazelcastPlugin;

/**
 * Created by tglman on 30/03/17.
 */
public class ODatabaseDocumentDistributed extends ODatabaseDocumentEmbedded {

  private final OHazelcastPlugin    hazelcastPlugin;

  public ODatabaseDocumentDistributed(OStorage storage, OHazelcastPlugin hazelcastPlugin) {
    super(storage);
    this.hazelcastPlugin = hazelcastPlugin;
  }

  @Override
  public ODatabaseDocumentInternal copy() {
    ODatabaseDocumentDistributed database = new ODatabaseDocumentDistributed(getStorage(), hazelcastPlugin);
    database.internalOpen(getUser().getName(), null, getConfig(), false);
    this.activateOnCurrentThread();
    return database;
  }

  @Override
  public boolean sync() {
    final OStorage stg = getStorage();
    if (!(stg instanceof ODistributedStorage))
      throw new ODistributedException("SYNC DATABASE command cannot be executed against a non distributed server");

    final ODistributedStorage dStg = (ODistributedStorage) stg;

    final OHazelcastPlugin dManager = (OHazelcastPlugin) dStg.getDistributedManager();
    if (dManager == null || !dManager.isEnabled())
      throw new OCommandExecutionException("OrientDB is not started in distributed mode");

    final String databaseName = getName();

    return dManager.installDatabase(true, databaseName, dStg.getDistributedConfiguration().getDocument(), false, true);

  }
}
