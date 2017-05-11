/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.client.remote;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.client.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.client.remote.message.*;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.security.OCredentialInterceptor;
import com.orientechnologies.orient.core.security.OSecurityManager;
import com.orientechnologies.orient.core.storage.OStorage;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Remote administration class of OrientDB Server instances.
 */
public class OServerAdmin {
  protected OStorageRemote storage;
  protected OStorageRemoteSession session      = new OStorageRemoteSession(-1);
  protected String                clientType   = OStorageRemote.DRIVER_NAME;
  protected boolean               collectStats = true;

  /**
   * Creates the object passing a remote URL to connect. sessionToken
   *
   * @param iURL URL to connect. It supports only the "remote" storage type.
   *
   * @throws IOException
   */
  public OServerAdmin(String iURL) throws IOException {
    if (iURL.startsWith(OEngineRemote.NAME))
      iURL = iURL.substring(OEngineRemote.NAME.length() + 1);

    if (!iURL.contains("/"))
      iURL += "/";

    ORemoteConnectionManager connectionManager = ((OEngineRemote) Orient.instance().getRunningEngine("remote")).getConnectionManager();

    storage = new OStorageRemote(iURL, "", connectionManager, OStorage.STATUS.OPEN) {
      @Override
      protected OStorageRemoteSession getCurrentSession() {
        return session;
      }
    };
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
   * @param iUserName     Server's user name
   * @param iUserPassword Server's password for the user name used
   *
   * @return The instance itself. Useful to execute method in chain
   *
   * @throws IOException
   */
  public synchronized OServerAdmin connect(final String iUserName, final String iUserPassword) throws IOException {
    final String username;
    final String password;

    OCredentialInterceptor ci = OSecurityManager.instance().newCredentialInterceptor();

    if (ci != null) {
      ci.intercept(storage.getURL(), iUserName, iUserPassword);
      username = ci.getUsername();
      password = ci.getPassword();
    } else {
      username = iUserName;
      password = iUserPassword;
    }
    OConnect37Request request = new OConnect37Request(username, password);

    networkAdminOperation((network, session) -> {
      OStorageRemoteNodeSession nodeSession = session.getOrCreateServerSession(network.getServerURL());
      try {
        network.beginRequest(request.getCommand(), session);
        request.write(network, session);
      } finally {
        network.endRequest();
      }
      OConnectResponse response = request.createResponse();
      try {
        network.beginResponse(nodeSession.getSessionId(), false);
        response.read(network, session);
      } finally {
        storage.endResponse(network);
      }
      storage.connectionManager.release(network);
      return null;
    }, "Cannot connect to the remote server/database '" + storage.getURL() + "'");

    return this;
  }

  /**
   * Returns the list of databases on the connected remote server.
   *
   * @throws IOException
   */
  public synchronized Map<String, String> listDatabases() throws IOException {
    OListDatabasesRequest request = new OListDatabasesRequest();
    OListDatabasesResponse response = networkAdminOperation(request, "Cannot retrieve the configuration list");
    return response.getDatabases();
  }

  /**
   * Returns the server information in form of document.
   *
   * @throws IOException
   */
  public synchronized ODocument getServerInfo() throws IOException {
    OServerInfoRequest request = new OServerInfoRequest();
    OServerInfoResponse response = networkAdminOperation(request, "Cannot retrieve server information");
    ODocument res = new ODocument();
    res.fromJSON(response.getResult());
    return res;
  }

  public int getSessionId() {
    return session.getSessionId();
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
   * @param iDatabaseType 'document' or 'graph'
   * @param iStorageMode  local or memory
   *
   * @return The instance itself. Useful to execute method in chain
   *
   * @throws IOException
   */
  public synchronized OServerAdmin createDatabase(final String iDatabaseType, String iStorageMode) throws IOException {
    return createDatabase(storage.getName(), iDatabaseType, iStorageMode);
  }

  public synchronized String getStorageName() {
    return storage.getName();
  }

  public synchronized OServerAdmin createDatabase(final String iDatabaseName, final String iDatabaseType, final String iStorageMode)
      throws IOException {
    return createDatabase(iDatabaseName, iDatabaseType, iStorageMode, null);
  }

  /**
   * Creates a database in a remote server.
   *
   * @param iDatabaseName The database name
   * @param iDatabaseType 'document' or 'graph'
   * @param iStorageMode  local or memory
   * @param backupPath    path to incremental backup which will be used to create database (optional)
   *
   * @return The instance itself. Useful to execute method in chain
   *
   * @throws IOException
   */
  public synchronized OServerAdmin createDatabase(final String iDatabaseName, final String iDatabaseType, final String iStorageMode,
      final String backupPath) throws IOException {

    if (iDatabaseName == null || iDatabaseName.length() <= 0) {
      final String message = "Cannot create unnamed remote storage. Check your syntax";
      OLogManager.instance().error(this, message);
      throw new OStorageException(message);
    } else {
      String storageMode;
      if (iStorageMode == null)
        storageMode = "plocal";
      else
        storageMode = iStorageMode;

      OCreateDatabaseRequest request = new OCreateDatabaseRequest(iDatabaseName, iDatabaseName, storageMode, backupPath);
      OCreateDatabaseResponse response = networkAdminOperation(request, "Cannot create the remote storage: " + storage.getName());

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
   * @param iDatabaseName The database name
   * @param storageType   Storage type between "plocal" or "memory".
   *
   * @return true if exists, otherwise false
   *
   * @throws IOException
   */
  public synchronized boolean existsDatabase(final String iDatabaseName, final String storageType) throws IOException {
    OExistsDatabaseRequest request = new OExistsDatabaseRequest(iDatabaseName, storageType);
    OExistsDatabaseResponse response = networkAdminOperation(request,
        "Error on checking existence of the remote storage: " + storage.getName());
    return response.isExists();

  }

  /**
   * Checks if a database exists in the remote server.
   *
   * @param storageType Storage type between "plocal" or "memory".
   *
   * @return true if exists, otherwise false
   *
   * @throws IOException
   */
  public synchronized boolean existsDatabase(final String storageType) throws IOException {
    return existsDatabase(storage.getName(), storageType);
  }

  /**
   * Deprecated. Use dropDatabase() instead.
   *
   * @param storageType Storage type between "plocal" or "memory".
   *
   * @return The instance itself. Useful to execute method in chain
   *
   * @throws IOException
   * @see #dropDatabase(String)
   */
  @Deprecated
  public OServerAdmin deleteDatabase(final String storageType) throws IOException {
    return dropDatabase(storageType);
  }

  /**
   * Drops a database from a remote server instance.
   *
   * @param iDatabaseName The database name
   * @param storageType   Storage type between "plocal" or "memory".
   *
   * @return The instance itself. Useful to execute method in chain
   *
   * @throws IOException
   */
  public synchronized OServerAdmin dropDatabase(final String iDatabaseName, final String storageType) throws IOException {

    ODropDatabaseRequest request = new ODropDatabaseRequest(iDatabaseName, storageType);
    ODropDatabaseResponse response = networkAdminOperation(request, "Cannot delete the remote storage: " + storage.getName());

    final Set<OStorage> underlyingStorages = new HashSet<OStorage>();

    for (OStorage s : Orient.instance().getStorages()) {
      if (s.getType().equals(storage.getType()) && s.getName().equals(storage.getName())) {
        underlyingStorages.add(s.getUnderlying());
      }
    }

    for (OStorage s : underlyingStorages) {
      s.close(true, true);
    }

    ODatabaseRecordThreadLocal.INSTANCE.remove();

    return this;
  }

  /**
   * Drops a database from a remote server instance.
   *
   * @param storageType Storage type between "plocal" or "memory".
   *
   * @return The instance itself. Useful to execute method in chain
   *
   * @throws IOException
   */
  public synchronized OServerAdmin dropDatabase(final String storageType) throws IOException {
    return dropDatabase(storage.getName(), storageType);
  }

  /**
   * Freezes the database by locking it in exclusive mode.
   *
   * @param storageType Storage type between "plocal" or "memory".
   *
   * @return
   *
   * @throws IOException
   * @see #releaseDatabase(String)
   */
  public synchronized OServerAdmin freezeDatabase(final String storageType) throws IOException {

    OFreezeDatabaseRequest request = new OFreezeDatabaseRequest(storage.getName(), storageType);
    OFreezeDatabaseResponse response = networkAdminOperation(request, "Cannot freeze the remote storage: " + storage.getName());

    return this;
  }

  /**
   * Releases a frozen database.
   *
   * @param storageType Storage type between "plocal" or "memory".
   *
   * @return
   *
   * @throws IOException
   * @see #freezeDatabase(String)
   */
  public synchronized OServerAdmin releaseDatabase(final String storageType) throws IOException {

    OReleaseDatabaseRequest request = new OReleaseDatabaseRequest(storage.getName(), storageType);
    OReleaseDatabaseResponse response = networkAdminOperation(request, "Cannot release the remote storage: " + storage.getName());

    return this;
  }

  /**
   * Gets the cluster status.
   *
   * @return the JSON containing the current cluster structure
   */
  public ODocument clusterStatus() {

    ODistributedStatusRequest request = new ODistributedStatusRequest();

    ODistributedStatusResponse response = storage.networkOperation(request, "Error on executing Cluster status ");

    OLogManager.instance().debug(this, "Cluster status %s", response.getClusterConfig().toJSON("prettyPrint"));
    return response.getClusterConfig();
  }

  public synchronized Map<String, String> getGlobalConfigurations() throws IOException {

    OListGlobalConfigurationsRequest request = new OListGlobalConfigurationsRequest();

    OListGlobalConfigurationsResponse response = networkAdminOperation(request, "Cannot retrieve the configuration list");
    return response.getConfigs();

  }

  public synchronized String getGlobalConfiguration(final OGlobalConfiguration config) throws IOException {

    OGetGlobalConfigurationRequest request = new OGetGlobalConfigurationRequest(config.getKey());

    OGetGlobalConfigurationResponse response = networkAdminOperation(request,
        "Cannot retrieve the configuration value: " + config.getKey());

    return response.getValue();
  }

  public synchronized OServerAdmin setGlobalConfiguration(final OGlobalConfiguration config, final Object iValue)
      throws IOException {

    OSetGlobalConfigurationRequest request = new OSetGlobalConfigurationRequest(config.getKey(),
        iValue != null ? iValue.toString() : "");
    OSetGlobalConfigurationResponse response = networkAdminOperation(request,
        "Cannot set the configuration value: " + config.getKey());
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

  protected <T extends OBinaryResponse> T networkAdminOperation(final OBinaryRequest<T> request, final String errorMessage) {
    return networkAdminOperation(new OStorageRemoteOperation<T>() {
      @Override
      public T execute(OChannelBinaryAsynchClient network, OStorageRemoteSession session) throws IOException {
        try {
          network.beginRequest(request.getCommand(), session);
          request.write(network, session);
        } finally {
          network.endRequest();
        }
        T response = request.createResponse();
        try {
          storage.beginResponse(network, session);
          response.read(network, session);
        } finally {
          storage.endResponse(network);
        }
        storage.connectionManager.release(network);
        return response;
      }
    }, errorMessage);
  }

  protected <T> T networkAdminOperation(final OStorageRemoteOperation<T> operation, final String errorMessage) {

    OChannelBinaryAsynchClient network = null;
    try {
      // TODO:replace this api with one that get connection for only the specified url.
      String serverUrl = storage.getNextAvailableServerURL(false, session);
      do {
        try {
          network = storage.getNetwork(serverUrl);
        } catch (OException e) {
          serverUrl = storage.useNewServerURL(serverUrl);
          if (serverUrl == null)
            throw e;
        }
      } while (network == null);

      T res = operation.execute(network, storage.getCurrentSession());
      storage.connectionManager.release(network);
      return res;
    } catch (Exception e) {
      if (network != null)
        storage.connectionManager.release(network);
      storage.close(true, false);
      throw OException.wrapException(new OStorageException(errorMessage), e);
    }
  }
}
