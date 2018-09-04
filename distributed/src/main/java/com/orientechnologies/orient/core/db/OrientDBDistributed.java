package com.orientechnologies.orient.core.db;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentEmbedded;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerAware;
import com.orientechnologies.orient.server.OSystemDatabase;
import com.orientechnologies.orient.server.distributed.impl.ODatabaseDocumentDistributed;
import com.orientechnologies.orient.server.distributed.impl.ODatabaseDocumentDistributedPooled;
import com.orientechnologies.orient.server.distributed.impl.metadata.OSharedContextDistributed;
import com.orientechnologies.orient.server.hazelcast.OHazelcastPlugin;

import java.util.HashMap;

/**
 * Created by tglman on 08/08/17.
 */
public class OrientDBDistributed extends OrientDBEmbedded implements OServerAware {

  private          OServer          server;
  private volatile OHazelcastPlugin plugin;

  public OrientDBDistributed(String directoryPath, OrientDBConfig config, Orient instance) {
    super(directoryPath, config, instance);
  }

  @Override
  public void init(OServer server) {
    //Cannot get the plugin from here, is too early, doing it lazy  
    this.server = server;
  }

  public synchronized OHazelcastPlugin getPlugin() {
    if (plugin == null) {
      if (server != null && server.isActive())
        plugin = server.getPlugin("cluster");
    }
    return plugin;
  }

  protected OSharedContext createSharedContext(OAbstractPaginatedStorage storage) {
    if (OSystemDatabase.SYSTEM_DB_NAME.equals(storage.getName()) || getPlugin() == null || !getPlugin().isEnabled()) {
      return new OSharedContextEmbedded(storage, this);
    }
    return new OSharedContextDistributed(storage, this);
  }

  protected ODatabaseDocumentEmbedded newSessionInstance(OAbstractPaginatedStorage storage) {
    if (OSystemDatabase.SYSTEM_DB_NAME.equals(storage.getName()) || getPlugin() == null || !getPlugin().isEnabled()) {
      return new ODatabaseDocumentEmbedded(storage);
    }
    plugin.registerNewDatabaseIfNeeded(storage.getName(), plugin.getDatabaseConfiguration(storage.getName()));
    return new ODatabaseDocumentDistributed(plugin.getStorage(storage.getName(), storage), plugin);
  }

  protected ODatabaseDocumentEmbedded newPooledSessionInstance(ODatabasePoolInternal pool, OAbstractPaginatedStorage storage) {
    if (OSystemDatabase.SYSTEM_DB_NAME.equals(storage.getName()) || getPlugin() == null || !getPlugin().isEnabled()) {
      return new ODatabaseDocumentEmbeddedPooled(pool, storage);
    }
    plugin.registerNewDatabaseIfNeeded(storage.getName(), plugin.getDatabaseConfiguration(storage.getName()));
    return new ODatabaseDocumentDistributedPooled(pool, plugin.getStorage(storage.getName(), storage), plugin);

  }

  public void setPlugin(OHazelcastPlugin plugin) {
    this.plugin = plugin;
  }

  public void fullSync(String dbName, String backupPath, OrientDBConfig config) {
    final ODatabaseDocumentEmbedded embedded;
    OAbstractPaginatedStorage storage;
    synchronized (this) {

      try {
        storage = storages.get(dbName);

        if (storage != null) {
          storage.delete();
          storages.remove(dbName);
        }
        storage = (OAbstractPaginatedStorage) disk.createStorage(buildName(dbName), new HashMap<>());
        embedded = internalCreate(config, storage);
        storages.put(dbName, storage);
      } catch (Exception e) {
        throw OException.wrapException(new ODatabaseException("Cannot restore database '" + dbName + "'"), e);
      }
    }
    storage.restoreFromIncrementalBackup(backupPath);
  }

}
