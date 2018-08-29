package com.orientechnologies.orient.server;

import com.orientechnologies.orient.client.remote.message.*;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.OMetadataUpdateListener;
import com.orientechnologies.orient.core.index.OIndexManager;
import com.orientechnologies.orient.core.index.OIndexManagerShared;
import com.orientechnologies.orient.core.metadata.schema.OSchemaShared;
import com.orientechnologies.orient.server.network.protocol.binary.ONetworkProtocolBinary;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.*;

public class OPushManager implements OMetadataUpdateListener {

  protected final Set<WeakReference<ONetworkProtocolBinary>>              distributedConfigPush = new HashSet<>();
  protected final Map<String, Set<WeakReference<ONetworkProtocolBinary>>> storageConfigurations = new HashMap<>();
  protected final Map<String, Set<WeakReference<ONetworkProtocolBinary>>> schema                = new HashMap<>();
  protected final Map<String, Set<WeakReference<ONetworkProtocolBinary>>> indexManager          = new HashMap<>();
  protected final Map<String, Set<WeakReference<ONetworkProtocolBinary>>> functions             = new HashMap<>();
  protected final Map<String, Set<WeakReference<ONetworkProtocolBinary>>> sequences             = new HashMap<>();
  private         Set<String>                                             registerDatabase      = new HashSet<>();

  public synchronized void pushDistributedConfig(String database, List<String> hosts) {
    Iterator<WeakReference<ONetworkProtocolBinary>> iter = distributedConfigPush.iterator();
    while (iter.hasNext()) {
      WeakReference<ONetworkProtocolBinary> ref = iter.next();
      ONetworkProtocolBinary protocolBinary = ref.get();
      if (protocolBinary != null) {
        //TODO Filter by database, push just list of active server for a specific database
        OPushDistributedConfigurationRequest request = new OPushDistributedConfigurationRequest(hosts);
        try {
          OBinaryPushResponse response = protocolBinary.push(request);
        } catch (IOException e) {
          iter.remove();
        }
      } else {
        iter.remove();
      }
    }
  }

  public synchronized void subscribeDistributeConfig(ONetworkProtocolBinary channel) {
    distributedConfigPush.add(new WeakReference<ONetworkProtocolBinary>(channel));
  }

  public synchronized void cleanPushSockets() {
    Iterator<WeakReference<ONetworkProtocolBinary>> iter = distributedConfigPush.iterator();
    while (iter.hasNext()) {
      if (iter.next().get() == null) {
        iter.remove();
      }
    }
    cleanListeners(storageConfigurations);
    cleanListeners(schema);
    cleanListeners(indexManager);
    cleanListeners(functions);
    cleanListeners(sequences);
  }

  private void cleanListeners(Map<String, Set<WeakReference<ONetworkProtocolBinary>>> toClean) {
    for (Set<WeakReference<ONetworkProtocolBinary>> value : toClean.values()) {
      Iterator<WeakReference<ONetworkProtocolBinary>> iter = value.iterator();
      while (iter.hasNext()) {
        if (iter.next().get() == null) {
          iter.remove();
        }
      }
    }
  }

  public void shutdown() {

  }

  private void genericSubscribe(Map<String, Set<WeakReference<ONetworkProtocolBinary>>> context, ODatabaseDocumentInternal database,
      ONetworkProtocolBinary protocol) {
    if (!registerDatabase.contains(database.getName())) {
      database.getSharedContext().registerListener(this);
      registerDatabase.add(database.getName());
    }
    Set<WeakReference<ONetworkProtocolBinary>> pushSockets = context.get(database.getName());
    if (pushSockets == null) {
      pushSockets = new HashSet<>();
      context.put(database.getName(), pushSockets);
    }
    pushSockets.add(new WeakReference<>(protocol));
  }

  public synchronized void subscribeStorageConfiguration(ODatabaseDocumentInternal database, ONetworkProtocolBinary protocol) {
    genericSubscribe(storageConfigurations, database, protocol);
  }

  public synchronized void subscribeSchema(ODatabaseDocumentInternal database, ONetworkProtocolBinary protocol) {
    genericSubscribe(schema, database, protocol);
  }

  public synchronized void subscribeIndexManager(ODatabaseDocumentInternal database, ONetworkProtocolBinary protocol) {
    genericSubscribe(indexManager, database, protocol);
  }

  public synchronized void subscribeFunctions(ODatabaseDocumentInternal database, ONetworkProtocolBinary protocol) {
    genericSubscribe(functions, database, protocol);
  }

  public synchronized void subscribeSequences(ODatabaseDocumentInternal database, ONetworkProtocolBinary protocol) {
    genericSubscribe(sequences, database, protocol);
  }

  @Override
  public void onSchemaUpdate(String database, OSchemaShared schema) {
    OPushSchemaRequest request = new OPushSchemaRequest(schema.toNetworkStream());
    genericNotify(this.schema, database, request);
  }

  @Override
  public void onIndexManagerUpdate(String database, OIndexManager indexManager) {
    OPushIndexManagerRequest request = new OPushIndexManagerRequest(((OIndexManagerShared) indexManager).toNetworkStream());
    genericNotify(this.indexManager, database, request);
  }

  @Override
  public void onFunctionLibraryUpdate(String database) {
    OPushFunctionsRequest request = new OPushFunctionsRequest();
    genericNotify(this.functions, database, request);
  }

  @Override
  public void onSequenceLibraryUpdate(String database) {
    OPushSequencesRequest request = new OPushSequencesRequest();
    genericNotify(this.functions, database, request);
  }

  @Override
  public void onStorageConfigurationUpdate(String database, OStorageConfiguration update) {
    OPushStorageConfigurationRequest request = new OPushStorageConfigurationRequest(update);
    genericNotify(storageConfigurations, database, request);
  }

  private void genericNotify(Map<String, Set<WeakReference<ONetworkProtocolBinary>>> context, String database,
      OBinaryPushRequest<?> request) {
    Orient.instance().submit(() -> {
      synchronized (OPushManager.this) {
        Set<WeakReference<ONetworkProtocolBinary>> clients = context.get(database);
        if (clients != null) {
          Iterator<WeakReference<ONetworkProtocolBinary>> iter = clients.iterator();
          while (iter.hasNext()) {
            WeakReference<ONetworkProtocolBinary> ref = iter.next();
            ONetworkProtocolBinary protocolBinary = ref.get();
            if (protocolBinary != null) {
              try {
                OBinaryPushResponse response = protocolBinary.push(request);
              } catch (IOException e) {
                iter.remove();
              }
            } else {
              iter.remove();
            }
          }
        }
      }
    });
  }

}
