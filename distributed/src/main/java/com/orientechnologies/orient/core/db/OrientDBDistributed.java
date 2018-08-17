package com.orientechnologies.orient.core.db;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentEmbedded;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerAware;
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

  public void fullSync(String dbName, String user, String password, ODatabaseType type, String backupPath, OrientDBConfig config) {
    final ODatabaseDocumentEmbedded embedded;
    synchronized (this) {
      if (!exists(dbName, null, null)) {
        try {
          OAbstractPaginatedStorage storage = storages.get(dbName);
          if (storage != null) {
            storage.delete();
            storages.remove(dbName);
          }
          storage = (OAbstractPaginatedStorage) disk.createStorage(buildName(dbName), new HashMap<>());
          embedded = internalCreate(config, storage);
          storage.restoreFromIncrementalBackup(backupPath);
          storages.put(dbName, storage);
        } catch (Exception e) {
          throw OException.wrapException(new ODatabaseException("Cannot restore database '" + dbName + "'"), e);
        }
      } else
        throw new ODatabaseException("Cannot create new storage '" + dbName + "' because it already exists");
    }
    embedded.callOnCreateListeners();
  }
}
