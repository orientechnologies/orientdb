package com.orientechnologies.orient.core.db;

import static com.orientechnologies.orient.core.config.OGlobalConfiguration.FILE_DELETE_DELAY;
import static com.orientechnologies.orient.core.config.OGlobalConfiguration.FILE_DELETE_RETRY;

import com.orientechnologies.common.concur.OOfflineNodeException;
import com.orientechnologies.common.concur.lock.OModificationOperationProhibitedException;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.cache.OCommandCacheSoftRefs;
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
import com.orientechnologies.orient.server.OSystemDatabase;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.impl.ODatabaseDocumentDistributed;
import com.orientechnologies.orient.server.distributed.impl.ODatabaseDocumentDistributedPooled;
import com.orientechnologies.orient.server.distributed.impl.ODistributedStorage;
import com.orientechnologies.orient.server.distributed.impl.metadata.OSharedContextDistributed;
import com.orientechnologies.orient.server.hazelcast.OHazelcastPlugin;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;

/** Created by tglman on 08/08/17. */
public class OrientDBDistributed extends OrientDBEmbedded implements OServerAware {

  private volatile OServer server;
  private volatile OHazelcastPlugin plugin;

  public OrientDBDistributed(String directoryPath, OrientDBConfig config, Orient instance) {
    super(directoryPath, config, instance);
    // This now si simple but should be replaced by a factory depending to the protocol version
  }

  @Override
  public void init(OServer server) {
    // Cannot get the plugin from here, is too early, doing it lazy
    this.server = server;
  }

  private boolean isDistributedEnabled() {
    if (server == null) {
      return false;
    }
    return server.isDistributedPluginEnabled();
  }

  public synchronized OHazelcastPlugin getPlugin() {
    if (plugin == null) {
      if (server != null && server.isActive()) plugin = server.getPlugin("cluster");
    }
    return plugin;
  }

  @Override
  public void create(
      String name, String user, String password, ODatabaseType type, OrientDBConfig config) {
    if (isDistributedEnabled() && (plugin == null)) {
      throw new OOfflineNodeException("Distributed plugin is not active");
    }
    super.create(name, user, password, type, config);
  }

  protected OSharedContext createSharedContext(OAbstractPaginatedStorage storage) {
    if (!isDistributedEnabled() || OSystemDatabase.SYSTEM_DB_NAME.equals(storage.getName())) {
      return new OSharedContextEmbedded(storage, this);
    }
    return new OSharedContextDistributed(storage, this);
  }

  protected ODatabaseDocumentEmbedded newSessionInstance(OAbstractPaginatedStorage storage) {
    if (!isDistributedEnabled() || OSystemDatabase.SYSTEM_DB_NAME.equals(storage.getName())) {
      return new ODatabaseDocumentEmbedded(storage);
    }
    if (plugin == null) {
      throw new OOfflineNodeException("Distributed plugin is not active");
    }
    plugin.registerNewDatabaseIfNeeded(
        storage.getName(), plugin.getDatabaseConfiguration(storage.getName()));
    return new ODatabaseDocumentDistributed(plugin.getStorage(storage.getName(), storage), plugin);
  }

  protected ODatabaseDocumentEmbedded newPooledSessionInstance(
      ODatabasePoolInternal pool, OAbstractPaginatedStorage storage) {
    if (!isDistributedEnabled() || OSystemDatabase.SYSTEM_DB_NAME.equals(storage.getName())) {
      return new ODatabaseDocumentEmbeddedPooled(pool, storage);
    }
    if (plugin == null) {
      throw new OOfflineNodeException("Distributed plugin is not active");
    }
    plugin.registerNewDatabaseIfNeeded(
        storage.getName(), plugin.getDatabaseConfiguration(storage.getName()));
    return new ODatabaseDocumentDistributedPooled(
        pool, plugin.getStorage(storage.getName(), storage), plugin);
  }

  public void setPlugin(OHazelcastPlugin plugin) {
    this.plugin = plugin;
  }

  public OStorage fullSync(String dbName, InputStream backupStream, OrientDBConfig config) {
    OAbstractPaginatedStorage storage = null;
    synchronized (this) {
      try {
        storage = storages.get(dbName);

        if (storage != null) {
          OCommandCacheSoftRefs.clearFiles(storage);
          ODistributedStorage.dropStorageFiles((OLocalPaginatedStorage) storage);
          OSharedContext context = sharedContexts.remove(dbName);
          context.close();
          storage.delete();
          storages.remove(dbName);
        }
        storage =
            (OAbstractPaginatedStorage)
                disk.createStorage(
                    buildName(dbName),
                    new HashMap<>(),
                    maxWALSegmentSize,
                    doubleWriteLogMaxSegSize,
                    generateStorageId());
        internalCreate(config, storage);
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
    // DROP AND CREATE THE SHARED CONTEXT SU HAS CORRECT INFORMATION.
    synchronized (this) {
      OSharedContext context = sharedContexts.remove(dbName);
      context.close();
    }
    ODatabaseRecordThreadLocal.instance().remove();
    return storage;
  }

  @Override
  public ODatabaseDocumentInternal poolOpen(
      String name, String user, String password, ODatabasePoolInternal pool) {
    checkDbAvailable(name);
    return super.poolOpen(name, user, password, pool);
  }

  @Override
  public ODatabasePoolInternal openPool(
      String name, String user, String password, OrientDBConfig config) {
    checkDbAvailable(name);
    return super.openPool(name, user, password, config);
  }

  @Override
  public ODatabasePoolInternal cachedPool(
      String name, String user, String password, OrientDBConfig config) {
    checkDbAvailable(name);
    return super.cachedPool(name, user, password, config);
  }

  @Override
  public void drop(String name, String user, String password) {
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
      ODatabaseDocumentInternal db = openNoAuthenticate(name, user);
      for (Iterator<ODatabaseLifecycleListener> it = orient.getDbLifecycleListeners();
          it.hasNext(); ) {
        it.next().onDrop(db);
      }
      db.close();
    } finally {
      ODatabaseRecordThreadLocal.instance().set(current);
    }

    synchronized (this) {
      if (exists(name, user, password)) {
        OAbstractPaginatedStorage storage = getOrInitStorage(name);
        OSharedContext sharedContext = sharedContexts.get(name);
        if (sharedContext != null) sharedContext.close();
        storage.delete();
        storages.remove(name);
        sharedContexts.remove(name);
      }
    }
  }

  private void checkDbAvailable(String name) {
    if (!isDistributedEnabled() || OSystemDatabase.SYSTEM_DB_NAME.equals(name)) {
      return;
    }
    if (getPlugin() == null || !getPlugin().isRunning()) {
      // The configuration specifies distributed mode, but the distributed plugin has not
      // started yet (and a client has requested database access before the server is up)
      throw new OOfflineNodeException("Distributed server is not yet ONLINE");
    }
    ODistributedServerManager.DB_STATUS dbStatus =
        plugin.getDatabaseStatus(plugin.getLocalNodeName(), name);
    if (dbStatus != ODistributedServerManager.DB_STATUS.ONLINE
        && dbStatus != ODistributedServerManager.DB_STATUS.BACKUP) {
      throw new OOfflineNodeException(
          "database " + name + " not online on " + plugin.getLocalNodeName());
    }
  }

  @Override
  public ODatabaseDocumentInternal open(
      String name, String user, String password, OrientDBConfig config) {
    checkDbAvailable(name);
    return super.open(name, user, password, config);
  }

  @Override
  public ODatabaseDocumentEmbedded openNoAuthenticate(String name, String user) {
    checkDbAvailable(name);
    return super.openNoAuthenticate(name, user);
  }

  @Override
  public ODatabaseDocumentEmbedded openNoAuthorization(String name) {
    checkDbAvailable(name);
    return super.openNoAuthorization(name);
  }

  @Override
  public void coordinatedRequest(
      OClientConnection connection, int requestType, int clientTxId, OChannelBinary channel)
      throws IOException {
    throw new UnsupportedOperationException("old implementation do not support new flow");
  }
}
