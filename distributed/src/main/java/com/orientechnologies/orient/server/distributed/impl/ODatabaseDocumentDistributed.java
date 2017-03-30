package com.orientechnologies.orient.server.distributed.impl;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentEmbedded;
import com.orientechnologies.orient.core.storage.OStorage;
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
}
