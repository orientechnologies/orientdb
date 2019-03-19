package com.orientechnologies.orient.core.db;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.cache.OCommandCacheSoftRefs;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
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
import com.orientechnologies.orient.server.distributed.impl.ODatabaseDocumentDistributed;
import com.orientechnologies.orient.server.distributed.impl.ODatabaseDocumentDistributedPooled;
import com.orientechnologies.orient.server.distributed.impl.ODistributedStorage;
import com.orientechnologies.orient.server.distributed.impl.coordinator.OCoordinateMessagesFactory;
import com.orientechnologies.orient.server.distributed.impl.coordinator.ODistributedChannel;
import com.orientechnologies.orient.server.distributed.impl.coordinator.ODistributedCoordinator;
import com.orientechnologies.orient.server.distributed.impl.coordinator.ODistributedMember;
import com.orientechnologies.orient.server.distributed.impl.coordinator.network.*;
import com.orientechnologies.orient.server.distributed.impl.metadata.ODistributedContext;
import com.orientechnologies.orient.server.distributed.impl.metadata.OSharedContextDistributed;
import com.orientechnologies.orient.server.distributed.impl.structural.OStructuralCoordinator;
import com.orientechnologies.orient.server.distributed.impl.structural.OStructuralDistributedContext;
import com.orientechnologies.orient.server.distributed.impl.structural.OStructuralDistributedMember;
import com.orientechnologies.orient.server.hazelcast.OCoordinatedExecutorMessageHandler;
import com.orientechnologies.orient.server.hazelcast.OHazelcastPlugin;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Callable;

import static com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol.*;
import static com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol.DISTRIBUTED_STRUCTURAL_OPERATION_RESPONSE;

/**
 * Created by tglman on 08/08/17.
 */
public class OrientDBDistributed extends OrientDBEmbedded implements OServerAware {

  private          OServer                            server;
  private volatile OHazelcastPlugin                   plugin;
  private final    Map<String, ODistributedChannel>   members     = new HashMap<>();
  private volatile boolean                            coordinator = false;
  private volatile String                             coordinatorName;
  private final    OStructuralDistributedContext      structuralDistributedContext;
  private          OCoordinatedExecutorMessageHandler requestHandler;
  private          OCoordinateMessagesFactory         coordinateMessagesFactory;

  public OrientDBDistributed(String directoryPath, OrientDBConfig config, Orient instance) {
    super(directoryPath, config, instance);
    structuralDistributedContext = new OStructuralDistributedContext();
    //This now si simple but should be replaced by a factory depending to the protocol version
    coordinateMessagesFactory = new OCoordinateMessagesFactory();
    requestHandler = new OCoordinatedExecutorMessageHandler(this);
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

  public OStorage fullSync(String dbName, InputStream backupStream, OrientDBConfig config) {
    final ODatabaseDocumentEmbedded embedded;
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
        storage = (OAbstractPaginatedStorage) disk.createStorage(buildName(dbName), new HashMap<>(), maxWALSegmentSize);
        embedded = internalCreate(config, storage);
        storages.put(dbName, storage);
      } catch (Exception e) {
        if (storage != null) {
          storage.delete();
        }

        throw OException.wrapException(new ODatabaseException("Cannot restore database '" + dbName + "'"), e);
      }
    }
    try {
      storage.restoreFullIncrementalBackup(backupStream);
    } catch (RuntimeException e) {
      storage.delete();
      throw e;
    }
    //DROP AND CREATE THE SHARED CONTEXT SU HAS CORRECT INFORMATION.
    synchronized (this) {
      OSharedContext context = sharedContexts.remove(dbName);
      context.close();
    }
    ODatabaseDocumentEmbedded instance = openNoAuthorization(dbName);
    instance.close();
    checkCoordinator(dbName);
    ODatabaseRecordThreadLocal.instance().remove();
    return storage;
  }

  @Override
  public void restore(String name, InputStream in, Map<String, Object> options, Callable<Object> callable,
      OCommandOutputListener iListener) {
    super.restore(name, in, options, callable, iListener);
    //THIS MAKE SURE THAT THE SHARED CONTEXT IS INITED.
    ODatabaseDocumentEmbedded instance = openNoAuthorization(name);
    instance.close();
    checkCoordinator(name);
  }

  @Override
  public void restore(String name, String user, String password, ODatabaseType type, String path, OrientDBConfig config) {
    super.restore(name, user, password, type, path, config);
    //THIS MAKE SURE THAT THE SHARED CONTEXT IS INITED.
    ODatabaseDocumentEmbedded instance = openNoAuthorization(name);
    instance.close();
    checkCoordinator(name);
  }

  @Override
  public void create(String name, String user, String password, ODatabaseType type, OrientDBConfig config) {
    //if (isDistributedVersionTwo()) {
    if (false) {
      //This initialize the distributed configuration.
      plugin.getDatabaseConfiguration(name);
      checkCoordinator(name);
    } else {
      super.create(name, user, password, type, config);
    }
  }

  @Override
  public ODatabaseDocumentEmbedded openNoAuthenticate(String name, String user) {
    ODatabaseDocumentEmbedded session = super.openNoAuthenticate(name, user);
    checkCoordinator(name);
    return session;
  }

  @Override
  public ODatabaseDocumentEmbedded openNoAuthorization(String name) {
    ODatabaseDocumentEmbedded session = super.openNoAuthorization(name);
    checkCoordinator(name);
    return session;

  }

  @Override
  public ODatabaseDocumentInternal open(String name, String user, String password, OrientDBConfig config) {
    ODatabaseDocumentInternal session = super.open(name, user, password, config);
    checkCoordinator(name);
    return session;
  }

  @Override
  public ODatabaseDocumentInternal poolOpen(String name, String user, String password, ODatabasePoolInternal pool) {
    ODatabaseDocumentInternal session = super.poolOpen(name, user, password, pool);
    checkCoordinator(name);
    return session;

  }

  private synchronized void checkCoordinator(String database) {
    if (!isDistributedVersionTwo())
      return;

    if (!database.equals(OSystemDatabase.SYSTEM_DB_NAME)) {
      OSharedContext shared = sharedContexts.get(database);
      if (shared instanceof OSharedContextDistributed) {
        ODistributedContext distributed = ((OSharedContextDistributed) shared).getDistributedContext();
        if (distributed.getCoordinator() == null) {
          if (coordinator) {
            distributed.makeCoordinator(plugin.getLocalNodeName(), shared);
            for (Map.Entry<String, ODistributedChannel> node : members.entrySet()) {
              ODistributedMember member = new ODistributedMember(node.getKey(), database, node.getValue());
              distributed.getCoordinator().join(member);
            }
          } else {
            ODistributedMember member = new ODistributedMember(coordinatorName, database, members.get(coordinatorName));
            distributed.setExternalCoordinator(member);
          }
        }

        for (Map.Entry<String, ODistributedChannel> node : members.entrySet()) {
          ODistributedMember member = new ODistributedMember(node.getKey(), database, node.getValue());
          distributed.getExecutor().join(member);
        }
      }
    }

  }

  private boolean isDistributedVersionTwo() {
    return getConfigurations().getConfigurations().getValueAsInteger(OGlobalConfiguration.DISTRIBUTED_REPLICATION_PROTOCOL_VERSION)
        == 2;
  }

  public synchronized void nodeJoin(String nodeName, ODistributedChannel channel) {
    if (!isDistributedVersionTwo())
      return;
    members.put(nodeName, channel);
    for (OSharedContext context : sharedContexts.values()) {
      if (isContextToIgnore(context))
        continue;
      ODistributedContext distributed = ((OSharedContextDistributed) context).getDistributedContext();
      ODistributedMember member = new ODistributedMember(nodeName, context.getStorage().getName(), channel);
      distributed.getExecutor().join(member);
      if (coordinator) {
        ODistributedCoordinator c = distributed.getCoordinator();
        if (c == null) {
          distributed.makeCoordinator(plugin.getLocalNodeName(), context);
          c = distributed.getCoordinator();
        }
        c.join(member);
      }
    }
    if (coordinator && structuralDistributedContext.getCoordinator() != null) {
      OStructuralDistributedMember member = new OStructuralDistributedMember(nodeName, channel);
      structuralDistributedContext.getCoordinator().join(member);
    }
  }

  public synchronized void nodeLeave(String nodeName) {
    if (!isDistributedVersionTwo())
      return;
    members.remove(nodeName);
    for (OSharedContext context : sharedContexts.values()) {
      if (isContextToIgnore(context))
        continue;
      ODistributedContext distributed = ((OSharedContextDistributed) context).getDistributedContext();
      distributed.getExecutor().leave(distributed.getExecutor().getMember(nodeName));
      if (coordinator) {
        ODistributedCoordinator c = distributed.getCoordinator();
        if (c == null) {
          c.leave(c.getMember(nodeName));
        }
        OStructuralCoordinator s = structuralDistributedContext.getCoordinator();
        if (s != null) {
          s.leave(s.getMember(nodeName));
        }
      }
    }
  }

  private boolean isContextToIgnore(OSharedContext context) {
    return context.getStorage().getName().equals(OSystemDatabase.SYSTEM_DB_NAME) || context.getStorage().isClosed();
  }

  public synchronized void setCoordinator(String lockManager, boolean isSelf) {
    if (!isDistributedVersionTwo())
      return;
    this.coordinatorName = lockManager;
    if (isSelf) {
      if (!coordinator) {
        for (OSharedContext context : sharedContexts.values()) {
          if (isContextToIgnore(context))
            continue;
          ODistributedContext distributed = ((OSharedContextDistributed) context).getDistributedContext();
          distributed.makeCoordinator(lockManager, context);
          for (Map.Entry<String, ODistributedChannel> node : members.entrySet()) {
            ODistributedMember member = new ODistributedMember(node.getKey(), context.getStorage().getName(), node.getValue());
            distributed.getCoordinator().join(member);
          }
        }
        structuralDistributedContext.makeCoordinator(lockManager);
        OStructuralCoordinator structuralCoordinator = structuralDistributedContext.getCoordinator();
        for (Map.Entry<String, ODistributedChannel> node : members.entrySet()) {
          OStructuralDistributedMember member = new OStructuralDistributedMember(node.getKey(), node.getValue());
          structuralCoordinator.join(member);
        }
        coordinator = true;
      }
    } else {
      for (OSharedContext context : sharedContexts.values()) {
        if (isContextToIgnore(context))
          continue;
        ODistributedContext distributed = ((OSharedContextDistributed) context).getDistributedContext();
        ODistributedMember member = new ODistributedMember(lockManager, context.getStorage().getName(), members.get(lockManager));
        distributed.setExternalCoordinator(member);
      }
      structuralDistributedContext.setExternalCoordinator(new OStructuralDistributedMember(lockManager, members.get(lockManager)));
      coordinator = false;
    }
  }

  public synchronized ODistributedContext getDistributedContext(String database) {
    OSharedContext shared = sharedContexts.get(database);
    if (shared != null) {
      return ((OSharedContextDistributed) shared).getDistributedContext();
    }
    return null;
  }

  public OStructuralDistributedContext getStructuralDistributedContext() {
    return structuralDistributedContext;
  }

  @Override
  public void drop(String name, String user, String password) {
    synchronized (this) {
      checkOpen();
      //This is a temporary fix for distributed drop that avoid scheduled view update to re-open the distributed database while is dropped
      OSharedContext sharedContext = sharedContexts.get(name);
      if (sharedContext != null) {
        sharedContext.getViewManager().close();
      }
    }

    ODatabaseDocumentInternal db = openNoAuthenticate(name, user);
    for (Iterator<ODatabaseLifecycleListener> it = orient.getDbLifecycleListeners(); it.hasNext(); ) {
      it.next().onDrop(db);
    }
    db.close();
    synchronized (this) {
      if (exists(name, user, password)) {
        OAbstractPaginatedStorage storage = getOrInitStorage(name);
        OSharedContext sharedContext = sharedContexts.get(name);
        if (sharedContext != null)
          sharedContext.close();
        storage.delete();
        storages.remove(name);
        sharedContexts.remove(name);
      }
    }
  }

  public void coordinatedRequest(OClientConnection connection, int requestType, int clientTxId, OChannelBinary channel)
      throws IOException {
    try {
      getPlugin().waitUntilNodeOnline();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    OBinaryRequest<OBinaryResponse> request = newDistributedRequest(requestType);
    try {
      request.read(channel, 0, null);
    } catch (IOException e) {
      //impossible to read request ... probably need to notify this back.
      throw e;
    }
    ODistributedExecutable executable = (ODistributedExecutable) request;
    executable.executeDistributed(requestHandler);
  }

  private OBinaryRequest<OBinaryResponse> newDistributedRequest(int requestType) {
    switch (requestType) {
    case DISTRIBUTED_SUBMIT_REQUEST:
      return new ONetworkSubmitRequest(coordinateMessagesFactory);
    case DISTRIBUTED_SUBMIT_RESPONSE:
      return new ONetworkSubmitResponse(coordinateMessagesFactory);
    case DISTRIBUTED_OPERATION_REQUEST:
      return new OOperationRequest(coordinateMessagesFactory);
    case DISTRIBUTED_OPERATION_RESPONSE:
      return new OOperationResponse(coordinateMessagesFactory);
    case DISTRIBUTED_STRUCTURAL_SUBMIT_REQUEST:
      return new ONetworkStructuralSubmitRequest(coordinateMessagesFactory);
    case DISTRIBUTED_STRUCTURAL_SUBMIT_RESPONSE:
      return new ONetworkStructuralSubmitResponse(coordinateMessagesFactory);
    case DISTRIBUTED_STRUCTURAL_OPERATION_REQUEST:
      return new OStructuralOperationRequest(coordinateMessagesFactory);
    case DISTRIBUTED_STRUCTURAL_OPERATION_RESPONSE:
      return new OStructuralOperationResponse(coordinateMessagesFactory);
    }
    return null;
  }


}
