/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.client.remote;

import com.orientechnologies.common.concur.lock.OModificationOperationProhibitedException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.index.OIndexManager;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.security.OSecurity;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Remote administration class of OrientDB Server instances.
 */
public class OServerAdmin {
  private OStorageRemote storage;
  private int            sessionId = -1;

  /**
   * Creates the object passing a remote URL to connect.
   * 
   * @param iURL
   *          URL to connect. It supports only the "remote" storage type.
   * @throws IOException
   */
  public OServerAdmin(String iURL) throws IOException {
    if (iURL.startsWith(OEngineRemote.NAME))
      iURL = iURL.substring(OEngineRemote.NAME.length() + 1);

    if (!iURL.contains("/"))
      iURL += "/";

    storage = new OStorageRemote(null, iURL, "", OStorage.STATUS.OPEN);
  }

  /**
   * Creates the object starting from an existent remote storage.
   * 
   * @param iStorage
   */
  public OServerAdmin(final OStorageRemote iStorage) {
    storage = iStorage;
  }

  /**
   * Connects to a remote server.
   * 
   * @param iUserName
   *          Server's user name
   * @param iUserPassword
   *          Server's password for the user name used
   * @return The instance itself. Useful to execute method in chain
   * @throws IOException
   */
  public synchronized OServerAdmin connect(final String iUserName, final String iUserPassword) throws IOException {
    storage.setSessionId(null, -1);

    try {
      final OChannelBinaryAsynchClient network = storage.beginRequest(OChannelBinaryProtocol.REQUEST_CONNECT);

      storage.sendClientInfo(network);

      try {
        network.writeString(iUserName);
        network.writeString(iUserPassword);
      } finally {
        storage.endRequest(network);
      }

      try {
        storage.beginResponse(network);
        sessionId = network.readInt();
        storage.setSessionId(network.getServerURL(), sessionId);
      } finally {
        storage.endResponse(network);
      }

    } catch (Exception e) {
      OLogManager.instance().error(this, "Cannot connect to the remote server/database '%s'", e, OStorageException.class,
          storage.getURL());
      storage.close(true, false);
    }
    return this;
  }

  /**
   * Returns the list of databases on the connected remote server.
   * 
   * @throws IOException
   */
  @SuppressWarnings("unchecked")
  public synchronized Map<String, String> listDatabases() throws IOException {

    final ODocument result = new ODocument();
    try {
      final OChannelBinaryAsynchClient network = storage.beginRequest(OChannelBinaryProtocol.REQUEST_DB_LIST);
      storage.endRequest(network);

      try {
        storage.beginResponse(network);
        result.fromStream(network.readBytes());
      } finally {
        storage.endResponse(network);
      }

    } catch (Exception e) {
      OLogManager.instance().exception("Cannot retrieve the configuration list", e, OStorageException.class);
      storage.close(true, false);
    }
    return (Map<String, String>) result.field("databases");
  }

  public int getSessionId() {
    return sessionId;
  }

  /**
   * Deprecated. Use the {@link #createDatabase(String, String)} instead.
   */
  @Deprecated
  public synchronized OServerAdmin createDatabase(final String iStorageMode) throws IOException {
    return createDatabase("document", iStorageMode);
  }

  /**
   * Creates a database in a remote server.
   * 
   * @param iDatabaseType
   *          'document' or 'graph'
   * @param iStorageMode
   *          local or memory
   * @return The instance itself. Useful to execute method in chain
   * @throws IOException
   */
  public synchronized OServerAdmin createDatabase(final String iDatabaseType, String iStorageMode) throws IOException {
    return createDatabase(storage.getName(), iDatabaseType, iStorageMode);
  }

  /**
   * Creates a database in a remote server.
   * 
   * @param iDatabaseName
   *          The database name
   * @param iDatabaseType
   *          'document' or 'graph'
   * @param iStorageMode
   *          local or memory
   * @return The instance itself. Useful to execute method in chain
   * @throws IOException
   */
  public synchronized OServerAdmin createDatabase(final String iDatabaseName, final String iDatabaseType, String iStorageMode)
      throws IOException {

    try {
      if (iDatabaseName == null || iDatabaseName.length() <= 0) {
        OLogManager.instance().error(this, "Cannot create unnamed remote storage. Check your syntax", OStorageException.class);
      } else {
        if (iStorageMode == null)
          iStorageMode = "csv";

        final OChannelBinaryAsynchClient network = storage.beginRequest(OChannelBinaryProtocol.REQUEST_DB_CREATE);
        try {
          network.writeString(iDatabaseName);
          if (network.getSrvProtocolVersion() >= 8)
            network.writeString(iDatabaseType);
          network.writeString(iStorageMode);
        } finally {
          storage.endRequest(network);
        }

        storage.getResponse(network);

      }

    } catch (Exception e) {
      OLogManager.instance().error(this, "Cannot create the remote storage: " + storage.getName(), e, OStorageException.class);
      storage.close(true, false);
    }
    return this;
  }

  /**
   * Checks if a database exists in the remote server.
   * 
   * @return true if exists, otherwise false
   */
  public synchronized boolean existsDatabase() throws IOException {
    return existsDatabase(null);
  }

  /**
   * Checks if a database exists in the remote server.
   * 
   * @return true if exists, otherwise false
   * @throws IOException
   * @param storageType
   *          The storage type to check between memory, local and plocal.
   */
  public synchronized boolean existsDatabase(final String storageType) throws IOException {

    try {
      final OChannelBinaryAsynchClient network = storage.beginRequest(OChannelBinaryProtocol.REQUEST_DB_EXIST);
      try {
        network.writeString(storage.getName());
        network.writeString(storageType);
      } finally {
        storage.endRequest(network);
      }

      try {
        storage.beginResponse(network);
        return network.readByte() == 1;
      } finally {
        storage.endResponse(network);
      }

    } catch (Exception e) {
      OLogManager.instance().exception("Error on checking existence of the remote storage: " + storage.getName(), e,
          OStorageException.class);
      storage.close(true, false);
    }
    return false;
  }

  /**
   * Deprecated. Use dropDatabase() instead.
   * 
   * @return The instance itself. Useful to execute method in chain
   * @see #dropDatabase(String)
   * @throws IOException
   * @param storageType
   *          Type of storage of server database.
   */
  @Deprecated
  public OServerAdmin deleteDatabase(String storageType) throws IOException {
    return dropDatabase(storageType);
  }

  /**
   * Drops a database from a remote server instance.
   * 
   * @return The instance itself. Useful to execute method in chain
   * @throws IOException
   * @param storageType
   */
  public synchronized OServerAdmin dropDatabase(String storageType) throws IOException {

    boolean retry = true;

    while (retry)
      try {

        final OChannelBinaryAsynchClient network = storage.beginRequest(OChannelBinaryProtocol.REQUEST_DB_DROP);
        try {
          network.writeString(storage.getName());
          network.writeString(storageType);
        } finally {
          storage.endRequest(network);
        }

        storage.getResponse(network);
        retry = false;
      } catch (OModificationOperationProhibitedException oope) {
        retry = handleDBFreeze();
      } catch (Exception e) {
        OLogManager.instance().exception("Cannot delete the remote storage: " + storage.getName(), e, OStorageException.class);
      }

    for (OStorage s : Orient.instance().getStorages()) {
      if (s.getURL().startsWith(getURL())) {
        s.removeResource(OSchema.class.getSimpleName());
        s.removeResource(OIndexManager.class.getSimpleName());
        s.removeResource(OSecurity.class.getSimpleName());
      }
    }

    ODatabaseRecordThreadLocal.INSTANCE.set(null);

    return this;
  }

  public synchronized OServerAdmin freezeDatabase(String storageType) throws IOException {

    try {
      final OChannelBinaryAsynchClient network = storage.beginRequest(OChannelBinaryProtocol.REQUEST_DB_FREEZE);

      try {
        network.writeString(storage.getName());
        network.writeString(storageType);
      } finally {
        storage.endRequest(network);
      }

      storage.getResponse(network);
    } catch (Exception e) {
      OLogManager.instance().exception("Cannot freeze the remote storage: " + storage.getName(), e, OStorageException.class);
    }

    return this;
  }

  public synchronized OServerAdmin releaseDatabase(String storageType) throws IOException {

    try {
      final OChannelBinaryAsynchClient network = storage.beginRequest(OChannelBinaryProtocol.REQUEST_DB_RELEASE);

      try {
        network.writeString(storage.getName());
        network.writeString(storageType);
      } finally {
        storage.endRequest(network);
      }

      storage.getResponse(network);
    } catch (Exception e) {
      OLogManager.instance().exception("Cannot release the remote storage: " + storage.getName(), e, OStorageException.class);
    }

    return this;
  }

  public synchronized OServerAdmin freezeCluster(int clusterId, String storageType) throws IOException {

    try {
      final OChannelBinaryAsynchClient network = storage.beginRequest(OChannelBinaryProtocol.REQUEST_DATACLUSTER_FREEZE);

      try {
        network.writeString(storage.getName());
        network.writeShort((short) clusterId);
        network.writeString(storageType);
      } finally {
        storage.endRequest(network);
      }

      storage.getResponse(network);
    } catch (IllegalArgumentException e) {
      throw e;
    } catch (Exception e) {
      OLogManager.instance().exception("Cannot freeze the remote cluster " + clusterId + " on storage: " + storage.getName(), e,
          OStorageException.class);
    }

    return this;
  }

  public synchronized OServerAdmin releaseCluster(int clusterId, String storageType) throws IOException {

    try {
      final OChannelBinaryAsynchClient network = storage.beginRequest(OChannelBinaryProtocol.REQUEST_DATACLUSTER_RELEASE);

      try {
        network.writeString(storage.getName());
        network.writeShort((short) clusterId);
        network.writeString(storageType);
      } finally {
        storage.endRequest(network);
      }

      storage.getResponse(network);
    } catch (IllegalArgumentException e) {
      throw e;
    } catch (Exception e) {
      OLogManager.instance().exception("Cannot release the remote cluster " + clusterId + " on storage: " + storage.getName(), e,
          OStorageException.class);
    }

    return this;
  }

  /**
   * Gets the cluster status.
   * 
   * @return the JSON containing the current cluster structure
   */
  public ODocument clusterStatus() {
    final ODocument response = sendRequest(OChannelBinaryProtocol.REQUEST_CLUSTER, new ODocument().field("operation", "status"),
        "Cluster status");

    OLogManager.instance().debug(this, "Cluster status %s", response.toJSON("prettyPrint"));
    return response;
  }

  /**
   * Copies a database to a remote server instance.
   * 
   * @param iDatabaseName
   * @param iDatabaseUserName
   * @param iDatabaseUserPassword
   * @param iRemoteName
   * @param iRemoteEngine
   * @return The instance itself. Useful to execute method in chain
   * @throws IOException
   */
  public synchronized OServerAdmin copyDatabase(final String iDatabaseName, final String iDatabaseUserName,
      final String iDatabaseUserPassword, final String iRemoteName, final String iRemoteEngine) throws IOException {

    try {

      final OChannelBinaryAsynchClient network = storage.beginRequest(OChannelBinaryProtocol.REQUEST_DB_COPY);
      try {
        network.writeString(iDatabaseName);
        network.writeString(iDatabaseUserName);
        network.writeString(iDatabaseUserPassword);
        network.writeString(iRemoteName);
        network.writeString(iRemoteEngine);
      } finally {
        storage.endRequest(network);
      }

      storage.getResponse(network);

      OLogManager.instance().debug(this, "Database '%s' has been copied to the server '%s'", iDatabaseName, iRemoteName);

    } catch (Exception e) {
      OLogManager.instance().exception("Cannot copy the database: " + iDatabaseName, e, OStorageException.class);
    }

    return this;
  }

  public synchronized Map<String, String> getGlobalConfigurations() throws IOException {

    final Map<String, String> config = new HashMap<String, String>();

    try {
      final OChannelBinaryAsynchClient network = storage.beginRequest(OChannelBinaryProtocol.REQUEST_CONFIG_LIST);
      storage.endRequest(network);

      try {
        storage.beginResponse(network);
        final int num = network.readShort();
        for (int i = 0; i < num; ++i)
          config.put(network.readString(), network.readString());
      } finally {
        storage.endResponse(network);
      }

    } catch (Exception e) {
      OLogManager.instance().exception("Cannot retrieve the configuration list", e, OStorageException.class);
      storage.close(true, false);
    }
    return config;
  }

  public synchronized String getGlobalConfiguration(final OGlobalConfiguration iConfig) throws IOException {

    try {
      final OChannelBinaryAsynchClient network = storage.beginRequest(OChannelBinaryProtocol.REQUEST_CONFIG_GET);
      network.writeString(iConfig.getKey());
      network.endRequest();

      try {
        storage.beginResponse(network);
        return network.readString();
      } finally {
        storage.endResponse(network);
      }

    } catch (Exception e) {
      OLogManager.instance().exception("Cannot retrieve the configuration value: " + iConfig.getKey(), e, OStorageException.class);
      storage.close(true, false);
    }
    return null;
  }

  public synchronized OServerAdmin setGlobalConfiguration(final OGlobalConfiguration iConfig, final Object iValue)
      throws IOException {

    try {
      final OChannelBinaryAsynchClient network = storage.beginRequest(OChannelBinaryProtocol.REQUEST_CONFIG_SET);
      network.writeString(iConfig.getKey());
      network.writeString(iValue != null ? iValue.toString() : "");
      storage.endRequest(network);
      storage.getResponse(network);

    } catch (Exception e) {
      OLogManager.instance().exception("Cannot set the configuration value: " + iConfig.getKey(), e, OStorageException.class);
      storage.close(true, false);
    }
    return this;
  }

  /**
   * Close the connection if open.
   */
  public synchronized void close() {
    storage.close();
  }

  public synchronized void close(boolean iForce) {
    storage.close(iForce, false);
  }

  public synchronized String getURL() {
    return storage != null ? storage.getURL() : null;
  }

  public boolean isConnected() {
    return storage != null && !storage.isClosed();
  }

  protected ODocument sendRequest(final byte iRequest, final ODocument iPayLoad, final String iActivity) {
    boolean retry = true;
    while (retry)
      try {

        final OChannelBinaryAsynchClient network = storage.beginRequest(iRequest);
        try {
          network.writeBytes(iPayLoad.toStream());
        } finally {
          storage.endRequest(network);
        }

        retry = false;
        try {
          storage.beginResponse(network);
          return new ODocument(network.readBytes());
        } finally {
          storage.endResponse(network);
        }
      } catch (OModificationOperationProhibitedException ompe) {
        retry = handleDBFreeze();
      } catch (Exception e) {
        OLogManager.instance().exception("Error on executing '%s'", e, OStorageException.class, iActivity);
      }
    return null;
  }

  private boolean handleDBFreeze() {
    boolean retry;
    OLogManager.instance().warn(this,
        "DB is frozen will wait for " + OGlobalConfiguration.CLIENT_DB_RELEASE_WAIT_TIMEOUT.getValue() + " ms. and then retry.");
    retry = true;
    try {
      Thread.sleep(OGlobalConfiguration.CLIENT_DB_RELEASE_WAIT_TIMEOUT.getValueAsInteger());
    } catch (InterruptedException ie) {
      retry = false;

      Thread.currentThread().interrupt();
    }
    return retry;
  }
}
