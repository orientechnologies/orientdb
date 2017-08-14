package com.orientechnologies.orient.server.hazelcast;

import com.orientechnologies.orient.core.db.ODatabasePoolInternal;
import com.orientechnologies.orient.core.db.OEmbeddedDatabaseInstanceFactory;
import com.orientechnologies.orient.core.db.ODatabaseDocumentEmbeddedPooled;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentEmbedded;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.server.OSystemDatabase;
import com.orientechnologies.orient.server.distributed.impl.ODatabaseDocumentDistributed;
import com.orientechnologies.orient.server.distributed.impl.ODatabaseDocumentDistributedPooled;

/**
 * Created by tglman on 11/07/17.
 */
class ODistributedEmbeddedDatabaseInstanceFactory implements OEmbeddedDatabaseInstanceFactory {
  private OHazelcastPlugin plugin;

  public ODistributedEmbeddedDatabaseInstanceFactory(OHazelcastPlugin plugin) {
    this.plugin = plugin;
  }

  @Override
  public ODatabaseDocumentEmbedded newInstance(OStorage storage) {
    if (OSystemDatabase.SYSTEM_DB_NAME.equals(storage.getName())) {
      return new ODatabaseDocumentEmbedded(storage);
    }
    plugin.registerNewDatabaseIfNeeded(storage.getName(), plugin.getDatabaseConfiguration(storage.getName()));
    return new ODatabaseDocumentDistributed(plugin.getStorage(storage.getName(), (OAbstractPaginatedStorage) storage), plugin);
  }

  @Override
  public ODatabaseDocumentEmbedded newPoolInstance(ODatabasePoolInternal pool, OStorage storage) {
    if (OSystemDatabase.SYSTEM_DB_NAME.equals(storage.getName())) {
      return new ODatabaseDocumentEmbeddedPooled(pool, storage);
    }
    plugin.registerNewDatabaseIfNeeded(storage.getName(), plugin.getDatabaseConfiguration(storage.getName()));
    return new ODatabaseDocumentDistributedPooled(pool, plugin.getStorage(storage.getName(), (OAbstractPaginatedStorage) storage),
        plugin);
  }
}
