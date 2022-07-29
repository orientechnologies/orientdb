package com.orientechnologies.orient.core.db;

import static com.orientechnologies.orient.core.config.OGlobalConfiguration.FILE_DELETE_DELAY;
import static com.orientechnologies.orient.core.config.OGlobalConfiguration.FILE_DELETE_RETRY;

import com.orientechnologies.common.concur.OOfflineNodeException;
import com.orientechnologies.common.concur.lock.OModificationOperationProhibitedException;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentEmbedded;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.disk.OLocalPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinary;
import com.orientechnologies.orient.server.OClientConnection;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerAware;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.impl.ODatabaseDocumentDistributed;
import com.orientechnologies.orient.server.distributed.impl.ODatabaseDocumentDistributedPooled;
import com.orientechnologies.orient.server.distributed.impl.ODistributedDatabaseImpl;
import com.orientechnologies.orient.server.distributed.impl.ODistributedPlugin;
import com.orientechnologies.orient.server.distributed.impl.metadata.OSharedContextDistributed;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;

/** Created by tglman on 08/08/17. */
public class OrientDBDistributed extends OrientDBEmbedded implements OServerAware {

  private volatile OServer server;
  private volatile ODistributedPlugin plugin;

  public OrientDBDistributed(String directoryPath, OrientDBConfig config, Orient instance) {
    super(directoryPath, config, instance);
    // This now si simple but should be replaced by a factory depending to the protocol version
  }

  @Override
  public void init(OServer server) {
    // Cannot get the plugin from here, is too early, doing it lazy
    this.server = server;
  }

  public synchronized ODistributedPlugin getPlugin() {
    if (plugin == null) {
      if (server != null && server.isActive()) plugin = server.getPlugin("cluster");
    }
    return plugin;
  }

  protected OSharedContext createSharedContext(OAbstractPaginatedStorage storage) {
    if (OSystemDatabase.SYSTEM_DB_NAME.equals(storage.getName())
        || plugin == null
        || !plugin.isEnabled()) {
      return new OSharedContextEmbedded(storage, this);
    }
    return new OSharedContextDistributed(storage, this);
  }

  protected ODatabaseDocumentEmbedded newSessionInstance(OAbstractPaginatedStorage storage) {
    if (OSystemDatabase.SYSTEM_DB_NAME.equals(storage.getName())
        || plugin == null
        || !plugin.isEnabled()) {
      return new ODatabaseDocumentEmbedded(storage);
    }
    plugin.registerNewDatabaseIfNeeded(storage.getName());
    return new ODatabaseDocumentDistributed(storage, plugin);
  }

  protected ODatabaseDocumentEmbedded newPooledSessionInstance(
      ODatabasePoolInternal pool, OAbstractPaginatedStorage storage) {
    if (OSystemDatabase.SYSTEM_DB_NAME.equals(storage.getName())
        || plugin == null
        || !plugin.isEnabled()) {
      return new ODatabaseDocumentEmbeddedPooled(pool, storage);
    }
    plugin.registerNewDatabaseIfNeeded(storage.getName());
    return new ODatabaseDocumentDistributedPooled(pool, storage, plugin);
  }

  public void setPlugin(ODistributedPlugin plugin) {
    this.plugin = plugin;
  }

  public OStorage fullSync(String dbName, InputStream backupStream, OrientDBConfig config) {
    OAbstractPaginatedStorage storage = null;
    ODatabaseDocumentEmbedded embedded;
    synchronized (this) {
      try {
        storage = storages.get(dbName);

        if (storage != null) {
          // The underlying storage instance will be closed so no need to closed it
          ODatabaseDocumentEmbedded deleteInstance = newSessionInstance(storage);
          deleteInstance.init(config, getOrCreateSharedContext(storage));
          dropStorageFiles((OLocalPaginatedStorage) storage);
          OSharedContext context = sharedContexts.remove(dbName);
          context.close();

          storage.delete();
          storages.remove(dbName);
          ODatabaseRecordThreadLocal.instance().remove();
        }
        storage =
            (OAbstractPaginatedStorage)
                disk.createStorage(
                    buildName(dbName),
                    new HashMap<>(),
                    maxWALSegmentSize,
                    doubleWriteLogMaxSegSize,
                    generateStorageId());
        embedded = internalCreate(config, storage);
        storages.put(dbName, storage);
      } catch (OModificationOperationProhibitedException e) {
        throw e;
      } catch (Exception e) {
        if (storage != null) {
          storage.delete();
        }

        throw OException.wrapException(
            new ODatabaseException("Cannot restore database '" + dbName + "'"), e);
      }
    }
    try {
      storage.restoreFullIncrementalBackup(backupStream);
    } catch (RuntimeException e) {
      try {
        if (storage != null) {
          storage.delete();
        }
      } catch (Exception e1) {
        OLogManager.instance()
            .warn(this, "Error doing cleanups, should be safe do progress anyway", e1);
      }
      synchronized (this) {
        sharedContexts.remove(dbName);
        storages.remove(dbName);
      }

      OContextConfiguration configs = getConfigurations().getConfigurations();
      OLocalPaginatedStorage.deleteFilesFromDisc(
          dbName,
          configs.getValueAsInteger(FILE_DELETE_RETRY),
          configs.getValueAsInteger(FILE_DELETE_DELAY),
          buildName(dbName));
      throw e;
    }
    embedded.getSharedContext().reInit(storage, embedded);
    ODatabaseRecordThreadLocal.instance().remove();
    return storage;
  }

  @Override
  public ODatabaseDocumentInternal poolOpen(
      String name, String user, String password, ODatabasePoolInternal pool) {
    ODatabaseDocumentInternal session = super.poolOpen(name, user, password, pool);
    return session;
  }

  @Override
  public void internalDrop(String name) {
    synchronized (this) {
      checkOpen();
      // This is a temporary fix for distributed drop that avoid scheduled view update to re-open
      // the distributed database while is dropped
      OSharedContext sharedContext = sharedContexts.get(name);
      if (sharedContext != null) {
        sharedContext.getViewManager().close();
      }
    }

    ODatabaseDocumentInternal current = ODatabaseRecordThreadLocal.instance().getIfDefined();
    try {
      ODatabaseDocumentInternal db = openNoAuthenticate(name, null);
      for (Iterator<ODatabaseLifecycleListener> it = orient.getDbLifecycleListeners();
          it.hasNext(); ) {
        it.next().onDrop(db);
      }
      db.close();
    } finally {
      ODatabaseRecordThreadLocal.instance().set(current);
    }

    synchronized (this) {
      if (exists(name, null, null)) {
        OAbstractPaginatedStorage storage = getOrInitStorage(name);
        OSharedContext sharedContext = sharedContexts.get(name);
        if (sharedContext != null) {
          sharedContext.close();
        }
        if (storage instanceof OLocalPaginatedStorage) {
          dropStorageFiles((OLocalPaginatedStorage) storage);
        }
        storage.delete();
        storages.remove(name);
        sharedContexts.remove(name);
      }
    }
  }

  @Override
  public void drop(String name, String user, String password) {
    if (getPlugin() != null && getPlugin().isEnabled()) {
      plugin.executeInDistributedDatabaseLock(
          name,
          20000,
          null,
          (cfg) -> {
            plugin.dropOnAllServers(name);
            return null;
          });
      plugin.dropConfig(name);
    } else {
      super.drop(name, user, password);
    }
  }

  private boolean checkDbAvailable(String name) {
    if (getPlugin() == null || !getPlugin().isEnabled()) {
      return true;
    }
    if (OSystemDatabase.SYSTEM_DB_NAME.equals(name)) return true;
    ODistributedServerManager.DB_STATUS dbStatus =
        plugin.getDatabaseStatus(plugin.getLocalNodeName(), name);
    return dbStatus == ODistributedServerManager.DB_STATUS.ONLINE
        || dbStatus == ODistributedServerManager.DB_STATUS.BACKUP;
  }

  @Override
  public ODatabaseDocumentInternal open(String name, String user, String password) {
    if (checkDbAvailable(name)) {
      return super.open(name, user, password);
    } else {
      if (exists(name, user, password)) {
        super.open(name, user, password);
      }
      throw new OOfflineNodeException(
          "database " + name + " not online on " + plugin.getLocalNodeName());
    }
  }

  @Override
  public ODatabaseDocumentInternal open(
      String name, String user, String password, OrientDBConfig config) {

    if (checkDbAvailable(name)) {
      return super.open(name, user, password, config);
    } else {
      if (exists(name, user, password)) {
        super.open(name, user, password, config);
      }
      throw new OOfflineNodeException(
          "database " + name + " not online on " + plugin.getLocalNodeName());
    }
  }

  @Override
  public void coordinatedRequest(
      OClientConnection connection, int requestType, int clientTxId, OChannelBinary channel)
      throws IOException {
    throw new UnsupportedOperationException("old implementation do not support new flow");
  }

  public static void dropStorageFiles(OLocalPaginatedStorage storage) {
    // REMOVE distributed-config.json and distributed-sync.json files to allow removal of directory
    final File dCfg =
        new File(
            storage.getStoragePath() + "/" + ODistributedServerManager.FILE_DISTRIBUTED_DB_CONFIG);

    try {
      if (dCfg.exists()) {
        for (int i = 0; i < 10; ++i) {
          if (dCfg.delete()) break;
          Thread.sleep(100);
        }
      }

      final File dCfg2 =
          new File(
              storage.getStoragePath()
                  + "/"
                  + ODistributedDatabaseImpl.DISTRIBUTED_SYNC_JSON_FILENAME);
      if (dCfg2.exists()) {
        for (int i = 0; i < 10; ++i) {
          if (dCfg2.delete()) break;
          Thread.sleep(100);
        }
      }
    } catch (InterruptedException e) {
      // IGNORE IT
    }
  }

  @Override
  public ODistributedServerManager getDistributedManager() {
    return this.plugin;
  }
}
