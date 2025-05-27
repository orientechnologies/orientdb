package com.orientechnologies.orient.server;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.thread.OThreadPoolExecutors;
import com.orientechnologies.orient.client.remote.message.OBinaryPushRequest;
import com.orientechnologies.orient.client.remote.message.OBinaryPushResponse;
import com.orientechnologies.orient.client.remote.message.OPushDistributedConfigurationRequest;
import com.orientechnologies.orient.client.remote.message.OPushFunctionsRequest;
import com.orientechnologies.orient.client.remote.message.OPushIndexManagerRequest;
import com.orientechnologies.orient.client.remote.message.OPushSchemaRequest;
import com.orientechnologies.orient.client.remote.message.OPushSequencesRequest;
import com.orientechnologies.orient.client.remote.message.OPushStorageConfigurationRequest;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.OMetadataUpdateListener;
import com.orientechnologies.orient.core.index.OIndexManagerAbstract;
import com.orientechnologies.orient.core.index.OIndexManagerShared;
import com.orientechnologies.orient.core.metadata.schema.OSchemaShared;
import com.orientechnologies.orient.server.network.protocol.binary.ONetworkProtocolBinary;
import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

public class OPushManager implements OMetadataUpdateListener {

  protected final Set<OPushInfo> distributedConfigPush =
      Collections.newSetFromMap(new ConcurrentHashMap<>());
  protected final OPushEventType storageConfigurations = new OPushEventType();
  protected final OPushEventType schema = new OPushEventType();
  protected final OPushEventType indexManager = new OPushEventType();
  protected final OPushEventType functions = new OPushEventType();
  protected final OPushEventType sequences = new OPushEventType();
  private final Set<String> registerDatabase = Collections.newSetFromMap(new ConcurrentHashMap<>());
  private final ExecutorService executor;
  private final OClientConnectionManager sessions;

  public OPushManager(
      OContextConfiguration contextConfiguration, OClientConnectionManager sessions) {
    int timeout =
        contextConfiguration.getValueAsInteger(OGlobalConfiguration.NETWORK_REQUEST_TIMEOUT);
    executor =
        OThreadPoolExecutors.newScalingThreadPool(
            "Push Requests", 0, 5, 2000, timeout, TimeUnit.MILLISECONDS);
    this.sessions = sessions;
  }

  public synchronized void pushDistributedConfig(String database, List<String> hosts) {
    Iterator<OPushInfo> iter = distributedConfigPush.iterator();
    while (iter.hasNext()) {
      OPushInfo ref = iter.next();
      ONetworkProtocolBinary protocolBinary = ref.protocol().get();
      if (protocolBinary != null) {
        // TODO Filter by database, push just list of active server for a specific database
        OPushDistributedConfigurationRequest request =
            new OPushDistributedConfigurationRequest(hosts);
        try {
          OBinaryPushResponse response = protocolBinary.push(request);
        } catch (IOException e) {
          close(ref);
          iter.remove();
        }
      } else {
        close(ref);
        iter.remove();
      }
    }
  }

  public synchronized void subscribeDistributeConfig(
      ONetworkProtocolBinary channel, OClientConnection session) {
    distributedConfigPush.add(
        new OPushInfo(session, new WeakReference<ONetworkProtocolBinary>(channel)));
  }

  public synchronized void cleanPushSockets() {
    Iterator<OPushInfo> iter = distributedConfigPush.iterator();
    while (iter.hasNext()) {
      OPushInfo ref = iter.next();
      if (ref.protocol().get() == null) {
        close(ref);
        iter.remove();
      }
    }
    storageConfigurations.cleanListeners(this);
    schema.cleanListeners(this);
    indexManager.cleanListeners(this);
    functions.cleanListeners(this);
    sequences.cleanListeners(this);
  }

  public void shutdown() {
    executor.shutdownNow();
  }

  private void genericSubscribe(
      OPushEventType context,
      ODatabaseDocumentInternal database,
      ONetworkProtocolBinary protocol,
      OClientConnection session) {
    if (!registerDatabase.contains(database.getName())) {
      database.getSharedContext().registerListener(this);
      registerDatabase.add(database.getName());
    }
    context.subscribe(
        database.getName(),
        new OPushInfo(session, new WeakReference<ONetworkProtocolBinary>(protocol)));
  }

  public synchronized void subscribeStorageConfiguration(
      ODatabaseDocumentInternal database,
      ONetworkProtocolBinary protocol,
      OClientConnection session) {
    genericSubscribe(storageConfigurations, database, protocol, session);
  }

  public synchronized void subscribeSchema(
      ODatabaseDocumentInternal database,
      ONetworkProtocolBinary protocol,
      OClientConnection session) {
    genericSubscribe(schema, database, protocol, session);
  }

  public synchronized void subscribeIndexManager(
      ODatabaseDocumentInternal database,
      ONetworkProtocolBinary protocol,
      OClientConnection session) {
    genericSubscribe(indexManager, database, protocol, session);
  }

  public synchronized void subscribeFunctions(
      ODatabaseDocumentInternal database,
      ONetworkProtocolBinary protocol,
      OClientConnection session) {
    genericSubscribe(functions, database, protocol, session);
  }

  public synchronized void subscribeSequences(
      ODatabaseDocumentInternal database,
      ONetworkProtocolBinary protocol,
      OClientConnection session) {
    genericSubscribe(sequences, database, protocol, session);
  }

  @Override
  public void onSchemaUpdate(String database, OSchemaShared schema) {
    OPushSchemaRequest request = new OPushSchemaRequest(schema.toNetworkStream());
    this.schema.send(database, request, this);
  }

  @Override
  public void onIndexManagerUpdate(String database, OIndexManagerAbstract indexManager) {
    OPushIndexManagerRequest request =
        new OPushIndexManagerRequest(((OIndexManagerShared) indexManager).toNetworkStream());
    this.indexManager.send(database, request, this);
  }

  @Override
  public void onFunctionLibraryUpdate(String database) {
    OPushFunctionsRequest request = new OPushFunctionsRequest();
    this.functions.send(database, request, this);
  }

  @Override
  public void onSequenceLibraryUpdate(String database) {
    OPushSequencesRequest request = new OPushSequencesRequest();
    this.sequences.send(database, request, this);
  }

  @Override
  public void onStorageConfigurationUpdate(String database, OStorageConfiguration update) {
    OPushStorageConfigurationRequest request = new OPushStorageConfigurationRequest(update);
    storageConfigurations.send(database, request, this);
  }

  public void close(OPushInfo info) {
    ONetworkProtocolBinary protocol = info.protocol().get();
    if (protocol != null) {
      protocol.shutdown();
    }
    this.sessions.disconnect(info.connection());
  }

  public void genericNotify(
      Map<String, Set<OPushInfo>> context, String database, OPushEventType pack) {
    try {
      executor.submit(
          () -> {
            Set<OPushInfo> clients = context.get(database);
            if (clients != null) {
              Iterator<OPushInfo> iter = clients.iterator();
              while (iter.hasNext()) {
                OPushInfo ref = iter.next();
                ONetworkProtocolBinary protocolBinary = ref.protocol().get();
                if (protocolBinary != null) {
                  try {
                    OBinaryPushRequest<?> request = pack.getRequest(database);
                    if (request != null) {
                      OBinaryPushResponse response = protocolBinary.push(request);
                    }
                  } catch (IOException e) {
                    close(ref);
                    iter.remove();
                  }
                } else {
                  close(ref);
                  iter.remove();
                }
              }
            }
          });
    } catch (RejectedExecutionException e) {
      OLogManager.instance()
          .info(this, "Cannot send push request to client for database '%s'", database);
    }
  }
}
