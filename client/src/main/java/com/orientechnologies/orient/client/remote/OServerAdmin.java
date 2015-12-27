/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.orient.client.remote;

import com.orientechnologies.common.concur.lock.OModificationOperationProhibitedException;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Remote administration class of OrientDB Server instances.
 */
public class OServerAdmin {
  private OStorageRemote storage;
  private int            sessionId    = -1;
  private byte[]         sessionToken = null;

  /**
   * Creates the object passing a remote URL to connect. sessionToken
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
    networkAdminOperation(new OStorageRemoteOperation<Void>() {
      @Override
      public Void execute(OChannelBinaryAsynchClient network) throws IOException {
        try {
          storage.beginRequest(network,OChannelBinaryProtocol.REQUEST_CONNECT);

          storage.sendClientInfo(network);

          network.writeString(iUserName);
          network.writeString(iUserPassword);
        } finally {
          storage.endRequest(network);
        }

        try {
          network.beginResponse(getSessionId(), false);
          sessionId = network.readInt();
          sessionToken = network.readBytes();
          if (sessionToken.length == 0) {
            sessionToken = null;
          } else {
            network.getServiceThread().setTokenBased(true);
          }
          storage.setSessionId(network.getServerURL(), sessionId, sessionToken);
        } finally {
          storage.endResponse(network);
        }

        return null;
      }
    },"Cannot connect to the remote server/database '" + storage.getURL() + "'");
    return this;
  }

  /**
   * Returns the list of databases on the connected remote server.
   *
   * @throws IOException
   */
  @SuppressWarnings("unchecked")
  public synchronized Map<String, String> listDatabases() throws IOException {
    return networkAdminOperation(new OStorageRemoteOperation<Map<String, String>>() {
      @Override
      public Map<String, String> execute(OChannelBinaryAsynchClient network) throws IOException {
        try {
          storage.beginRequest(network, OChannelBinaryProtocol.REQUEST_DB_LIST);
        } finally {
          storage.endRequest(network);
        }

        final ODocument result = new ODocument();
        try {
          storage.beginResponse(network);
          result.fromStream(network.readBytes());
        } finally {
          storage.endResponse(network);
        }

        return (Map<String, String>) result.field("databases");
      }
    }, "Cannot retrieve the configuration list");

  }

  /**
   * Returns the server information in form of document.
   *
   * @throws IOException
   */
  @SuppressWarnings("unchecked")
  public synchronized ODocument getServerInfo() throws IOException {
    return networkAdminOperation(new OStorageRemoteOperation<ODocument>() {
      @Override
      public ODocument execute(OChannelBinaryAsynchClient network) throws IOException {
        try {
          storage.beginRequest(network, OChannelBinaryProtocol.REQUEST_SERVER_INFO);
        } finally {
          storage.endRequest(network);
        }

        final ODocument result = new ODocument();
        try {
          storage.beginResponse(network);
          result.fromJSON(network.readString());
        } finally {
          storage.endResponse(network);
        }
        return result;
      }
    }, "Cannot retrieve server information");
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
  public synchronized OServerAdmin createDatabase(final String iDatabaseName, final String iDatabaseType, final String iStorageMode) throws IOException {

    if (iDatabaseName == null || iDatabaseName.length() <= 0) {
      final String message = "Cannot create unnamed remote storage. Check your syntax";
      OLogManager.instance().error(this, message);
      throw new OStorageException(message);
    } else {
      networkAdminOperation(new OStorageRemoteOperation<Void>() {
        @Override
        public Void execute(final OChannelBinaryAsynchClient network) throws IOException {
          String storageMode;
          if (iStorageMode == null)
            storageMode = "csv";
          else
            storageMode = iStorageMode;

          try {
            storage.beginRequest(network, OChannelBinaryProtocol.REQUEST_DB_CREATE);
            network.writeString(iDatabaseName);
            if (network.getSrvProtocolVersion() >= 8)
              network.writeString(iDatabaseType);
            network.writeString(storageMode);
          } finally {
            storage.endRequest(network);
          }

          storage.getResponse(network);
          return null;
        }
      }, "Cannot create the remote storage: \" + storage.getName();");

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
   * @param iDatabaseName
   *          The database name
   * @param storageType
   *          Storage type between "plocal" or "memory".
   */
  public synchronized boolean existsDatabase(final String iDatabaseName, final String storageType) throws IOException {

    return networkAdminOperation(new OStorageRemoteOperation<Boolean>() {
      @Override
      public Boolean execute(final OChannelBinaryAsynchClient network) throws IOException {

        try {
          storage.beginRequest(network, OChannelBinaryProtocol.REQUEST_DB_EXIST);
          network.writeString(iDatabaseName);
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
      }
    }, "Error on checking existence of the remote storage: " + storage.getName());

  }

  /**
   * Checks if a database exists in the remote server.
   *
   * @return true if exists, otherwise false
   * @throws IOException
   * @param storageType
   *          Storage type between "plocal" or "memory".
   */
  public synchronized boolean existsDatabase(final String storageType) throws IOException {
    return existsDatabase(storage.getName(), storageType);
  }

  /**
   * Deprecated. Use dropDatabase() instead.
   *
   * @return The instance itself. Useful to execute method in chain
   * @see #dropDatabase(String)
   * @throws IOException
   * @param storageType
   *          Storage type between "plocal" or "memory".
   */
  @Deprecated
  public OServerAdmin deleteDatabase(final String storageType) throws IOException {
    return dropDatabase(storageType);
  }

  /**
   * Drops a database from a remote server instance.
   *
   * @return The instance itself. Useful to execute method in chain
   * @throws IOException
   * @param iDatabaseName
   *          The database name
   * @param storageType
   *          Storage type between "plocal" or "memory".
   */
  public synchronized OServerAdmin dropDatabase(final String iDatabaseName, final String storageType) throws IOException {

    boolean retry = true;
    while (retry) {
      retry = networkAdminOperation(new OStorageRemoteOperation<Boolean>() {
        @Override
        public Boolean execute(final OChannelBinaryAsynchClient network) throws IOException {
          try {
            try {
              storage.beginRequest(network, OChannelBinaryProtocol.REQUEST_DB_DROP);
              network.writeString(iDatabaseName);
              network.writeString(storageType);
            } finally {
              storage.endRequest(network);
            }

            storage.getResponse(network);
            return false;
          } catch (OModificationOperationProhibitedException oope) {
            return handleDBFreeze();
          }

        }
      }, "Cannot delete the remote storage: " + storage.getName());
    }

    final Set<OStorage> underlyingStorages = new HashSet<OStorage>();

    for (OStorage s : Orient.instance().getStorages()) {
      if (s.getType().equals(storage.getType()) && s.getName().equals(storage.getName())) {
        underlyingStorages.add(s.getUnderlying());
        Orient.instance().unregisterStorage(s);
      }
    }

    for (OStorage s : underlyingStorages) {
      Orient.instance().unregisterStorage(s);
    }

    ODatabaseRecordThreadLocal.instance().remove();

    return this;
  }

  /**
   * Drops a database from a remote server instance.
   *
   * @return The instance itself. Useful to execute method in chain
   * @throws IOException
   * @param storageType
   *          Storage type between "plocal" or "memory".
   */
  public synchronized OServerAdmin dropDatabase(final String storageType) throws IOException {
    return dropDatabase(storage.getName(), storageType);
  }

  /**
   * Freezes the database by locking it in exclusive mode.
   *
   * @param storageType
   *          Storage type between "plocal" or "memory".
   * @return
   * @throws IOException
   * @see #releaseDatabase(String)
   */
  public synchronized OServerAdmin freezeDatabase(final String storageType) throws IOException {
    networkAdminOperation(new OStorageRemoteOperation<Void>() {
      @Override
      public Void execute(OChannelBinaryAsynchClient network) throws IOException {
        try {
          storage.beginRequest(network,OChannelBinaryProtocol.REQUEST_DB_FREEZE);
          network.writeString(storage.getName());
          network.writeString(storageType);
        } finally {
          storage.endRequest(network);
        }

        storage.getResponse(network);
        return null;
      }
    },"Cannot freeze the remote storage: " + storage.getName());

    return this;
  }

  /**
   * Releases a frozen database.
   *
   * @param storageType
   *          Storage type between "plocal" or "memory".
   * @return
   * @throws IOException
   * @see #freezeDatabase(String)
   */
  public synchronized OServerAdmin releaseDatabase(final String storageType) throws IOException {
    networkAdminOperation(new OStorageRemoteOperation<Void>() {
      @Override
      public Void execute(final OChannelBinaryAsynchClient network) throws IOException {
        try {
          storage.beginRequest(network, OChannelBinaryProtocol.REQUEST_DB_RELEASE);
          network.writeString(storage.getName());
          network.writeString(storageType);
        } finally {
          storage.endRequest(network);
        }

        storage.getResponse(network);
        return null;
      }
    }, "Cannot release the remote storage: " + storage.getName());

    return this;
  }

  /**
   * Freezes a cluster by locking it in exclusive mode.
   *
   * @param clusterId
   *          Id of cluster to freeze
   * @param storageType
   *          Storage type between "plocal" or "memory".
   * @return
   * @throws IOException
   * @see #releaseCluster(int, String)
   */

  public synchronized OServerAdmin freezeCluster(final int clusterId, final String storageType) throws IOException {

    networkAdminOperation(new OStorageRemoteOperation<Void>() {
      @Override
      public Void execute(final OChannelBinaryAsynchClient network) throws IOException {
        try {
          storage.beginRequest(network, OChannelBinaryProtocol.REQUEST_DATACLUSTER_FREEZE);
          network.writeString(storage.getName());
          network.writeShort((short) clusterId);
          network.writeString(storageType);
        } finally {
          storage.endRequest(network);
        }

        storage.getResponse(network);
        return null;
      }
    }, "Cannot freeze the remote cluster " + clusterId + " on storage: " + storage.getName());

    return this;
  }

  /**
   * Releases a frozen cluster.
   *
   * @param clusterId
   *          Id of cluster to freeze
   * @param storageType
   *          Storage type between "plocal" or "memory".
   * @return
   * @throws IOException
   * @see #freezeCluster(int, String)
   */
  public synchronized OServerAdmin releaseCluster(final int clusterId, final String storageType) throws IOException {

    networkAdminOperation(new OStorageRemoteOperation<Void>() {
      @Override
      public Void execute(final OChannelBinaryAsynchClient network) throws IOException {
        try {
          storage.beginRequest(network, OChannelBinaryProtocol.REQUEST_DATACLUSTER_RELEASE);
          network.writeString(storage.getName());
          network.writeShort((short) clusterId);
          network.writeString(storageType);
        } finally {
          storage.endRequest(network);
        }


        storage.getResponse(network);
        return null;
      }
    }, "Cannot release the remote cluster " + clusterId + " on storage: " + storage.getName());

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
   * @param databaseName
   * @param iDatabaseUserName
   * @param iDatabaseUserPassword
   * @param iRemoteName
   * @param iRemoteEngine
   * @return The instance itself. Useful to execute method in chain
   * @throws IOException
   */
  public synchronized OServerAdmin copyDatabase(final String databaseName, final String iDatabaseUserName, final String iDatabaseUserPassword, final String iRemoteName, final String iRemoteEngine) throws IOException {

    networkAdminOperation(new OStorageRemoteOperation<Void>() {
      @Override
      public Void execute(final OChannelBinaryAsynchClient network) throws IOException {

        try {
          storage.beginRequest(network,OChannelBinaryProtocol.REQUEST_DB_COPY);
          network.writeString(databaseName);
          network.writeString(iDatabaseUserName);
          network.writeString(iDatabaseUserPassword);
          network.writeString(iRemoteName);
          network.writeString(iRemoteEngine);
        } finally {
          storage.endRequest(network);
        }

        storage.getResponse(network);

        OLogManager.instance().debug(this, "Database '%s' has been copied to the server '%s'", databaseName, iRemoteName);
        return null;
      }
    }, "Cannot copy the database: " + databaseName);

    return this;
  }

  public synchronized Map<String, String> getGlobalConfigurations() throws IOException {
    return networkAdminOperation(new OStorageRemoteOperation<Map<String, String>>() {
      @Override
      public Map<String, String> execute(OChannelBinaryAsynchClient network) throws IOException {
        final Map<String, String> config = new HashMap<String, String>();
        storage.beginRequest(network,OChannelBinaryProtocol.REQUEST_CONFIG_LIST);
        storage.endRequest(network);

        try {
          storage.beginResponse(network);
          final int num = network.readShort();
          for (int i = 0; i < num; ++i)
            config.put(network.readString(), network.readString());
        } finally {
          storage.endResponse(network);
        }

        return config;
      }
    }, "Cannot retrieve the configuration list");

  }

  public synchronized String getGlobalConfiguration(final OGlobalConfiguration config) throws IOException {
    return networkAdminOperation(new OStorageRemoteOperation<String>() {
      @Override
      public String execute(OChannelBinaryAsynchClient network) throws IOException {
        storage.beginRequest(network,OChannelBinaryProtocol.REQUEST_CONFIG_GET);
        network.writeString(config.getKey());
        network.endRequest();

        try {
          storage.beginResponse(network);
          return network.readString();
        } finally {
          storage.endResponse(network);
        }
      }
    },"Cannot retrieve the configuration value: " + config.getKey());
  }

  public synchronized OServerAdmin setGlobalConfiguration(final OGlobalConfiguration config, final Object iValue) throws IOException {

    networkAdminOperation(new OStorageRemoteOperation<Void>() {
      @Override
      public Void execute(OChannelBinaryAsynchClient network) throws IOException {
        storage.beginRequest(network, OChannelBinaryProtocol.REQUEST_CONFIG_SET);
        network.writeString(config.getKey());
        network.writeString(iValue != null ? iValue.toString() : "");
        storage.endRequest(network);
        storage.getResponse(network);

        return null;
      }
    }, "Cannot set the configuration value: " + config.getKey());
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
    //Using here networkOperation because the original retry logic was lik networkOperation
    storage.setSessionId(getURL(),sessionId,sessionToken);
    return storage.networkOperation(new OStorageRemoteOperation<ODocument>() {
      @Override
      public ODocument execute(OChannelBinaryAsynchClient network) throws IOException {
        try {
          storage.beginRequest(network, iRequest);
          network.writeBytes(iPayLoad.toStream());
        } finally {
          storage.endRequest(network);
        }

        try {
          storage.beginResponse(network);
          return new ODocument(network.readBytes());
        } finally {
          storage.endResponse(network);
        }
      }
    }, "Error on executing  '" + iActivity + "'");
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

  protected <T> T networkAdminOperation(final OStorageRemoteOperation<T> operation, final String errorMessage) {

      OChannelBinaryAsynchClient network=null;
      try {
        storage.setSessionId(getURL(),sessionId,sessionToken);
        //TODO:replace this api with one that get connection for only the specified url.
        network = storage.getAvailableNetwork(getURL());
        return operation.execute(network);
      } catch (Exception e) {
        storage.close(true, false);
        throw OException.wrapException(new OStorageException(errorMessage), e);
      }
  }


}
