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

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.client.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.client.remote.message.OCreateDatabaseRequest;
import com.orientechnologies.orient.client.remote.message.OCreateDatabaseResponse;
import com.orientechnologies.orient.client.remote.message.ODistributedStatusRequest;
import com.orientechnologies.orient.client.remote.message.ODistributedStatusResponse;
import com.orientechnologies.orient.client.remote.message.ODropDatabaseRequest;
import com.orientechnologies.orient.client.remote.message.ODropDatabaseResponse;
import com.orientechnologies.orient.client.remote.message.OExistsDatabaseRequest;
import com.orientechnologies.orient.client.remote.message.OExistsDatabaseResponse;
import com.orientechnologies.orient.client.remote.message.OFreezeDatabaseRequest;
import com.orientechnologies.orient.client.remote.message.OFreezeDatabaseResponse;
import com.orientechnologies.orient.client.remote.message.OGetGlobalConfigurationRequest;
import com.orientechnologies.orient.client.remote.message.OGetGlobalConfigurationResponse;
import com.orientechnologies.orient.client.remote.message.OGetGlobalConfigurationsRequest;
import com.orientechnologies.orient.client.remote.message.OGetGlobalConfigurationsResponse;
import com.orientechnologies.orient.client.remote.message.OGetServerInfoRequest;
import com.orientechnologies.orient.client.remote.message.OGetServerInfoResponse;
import com.orientechnologies.orient.client.remote.message.OListDatabasesReponse;
import com.orientechnologies.orient.client.remote.message.OListDatabasesRequest;
import com.orientechnologies.orient.client.remote.message.OReleaseDatabaseRequest;
import com.orientechnologies.orient.client.remote.message.OReleaseDatabaseResponse;
import com.orientechnologies.orient.client.remote.message.OSetGlobalConfigurationRequest;
import com.orientechnologies.orient.client.remote.message.OSetGlobalConfigurationResponse;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.security.OCredentialInterceptor;
import com.orientechnologies.orient.core.security.OSecurityManager;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;

/**
 * Remote administration class of OrientDB Server instances.
 */
public class OServerAdmin {
  protected OStorageRemote        storage;
  protected OStorageRemoteSession session      = new OStorageRemoteSession(-1);
  protected String                clientType   = OStorageRemote.DRIVER_NAME;
  protected boolean               collectStats = true;

  /**
   * Creates the object passing a remote URL to connect. sessionToken
   *
   * @param iURL URL to connect. It supports only the "remote" storage type.
   * @throws IOException
   */
  public OServerAdmin(String iURL) throws IOException {
    if (iURL.startsWith(OEngineRemote.NAME))
      iURL = iURL.substring(OEngineRemote.NAME.length() + 1);

    if (!iURL.contains("/"))
      iURL += "/";

    storage = new OStorageRemote(null, iURL, "", OStorage.STATUS.OPEN, true) {
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
   * @param iUserName Server's user name
   * @param iUserPassword Server's password for the user name used
   * @return The instance itself. Useful to execute method in chain
   * @throws IOException
   */
  public synchronized OServerAdmin connect(final String iUserName, final String iUserPassword) throws IOException {
    networkAdminOperation(new OStorageRemoteOperation<Void>() {
      @Override
      public Void execute(OChannelBinaryAsynchClient network, OStorageRemoteSession session) throws IOException {
        OStorageRemoteNodeSession nodeSession = storage.getCurrentSession().getOrCreateServerSession(network.getServerURL());
        try {
          storage.beginRequest(network, OChannelBinaryProtocol.REQUEST_CONNECT, session);

          storage.sendClientInfo(network, clientType, false, collectStats);

          String username = iUserName;
          String password = iUserPassword;

          OCredentialInterceptor ci = OSecurityManager.instance().newCredentialInterceptor();

          if (ci != null) {
            ci.intercept(storage.getURL(), iUserName, iUserPassword);
            username = ci.getUsername();
            password = ci.getPassword();
          }

          network.writeString(username);
          network.writeString(password);
        } finally {
          storage.endRequest(network);
        }

        try {
          network.beginResponse(nodeSession.getSessionId(), false);
          int sessionId = network.readInt();
          byte[] sessionToken = network.readBytes();
          if (sessionToken.length == 0) {
            sessionToken = null;
          }
          nodeSession.setSession(sessionId, sessionToken);
        } finally {
          storage.endResponse(network);
        }

        return null;
      }
    }, "Cannot connect to the remote server/database '" + storage.getURL() + "'");
    return this;
  }

  /**
   * Returns the list of databases on the connected remote server.
   *
   * @throws IOException
   */
  public synchronized Map<String, String> listDatabases() throws IOException {

    OBinaryRequest request = new OListDatabasesRequest();
    OBinaryResponse<Map<String, String>> response = new OListDatabasesReponse();

    return networkAdminOperation(request, response, "Cannot retrieve the configuration list");

  }

  /**
   * Returns the server information in form of document.
   *
   * @throws IOException
   */
  public synchronized ODocument getServerInfo() throws IOException {
    OBinaryRequest request = new OGetServerInfoRequest();
    OBinaryResponse<String> response = new OGetServerInfoResponse();
    String result = networkAdminOperation(request, response, "Cannot retrieve server information");
    ODocument res = new ODocument();
    res.fromJSON(result);
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
   * @param iStorageMode local or memory
   * @return The instance itself. Useful to execute method in chain
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
   * @param iStorageMode local or memory
   * @param backupPath path to incremental backup which will be used to create database (optional)
   * @return The instance itself. Useful to execute method in chain
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

      OBinaryRequest request = new OCreateDatabaseRequest(iDatabaseName, iDatabaseName, storageMode, backupPath);
      OBinaryResponse<Void> response = new OCreateDatabaseResponse();

      networkAdminOperation(request, response, "Cannot create the remote storage: " + storage.getName());

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
   * @param storageType Storage type between "plocal" or "memory".
   * @return true if exists, otherwise false
   * @throws IOException
   */
  public synchronized boolean existsDatabase(final String iDatabaseName, final String storageType) throws IOException {
    OBinaryRequest request = new OExistsDatabaseRequest(iDatabaseName, storageType);
    OBinaryResponse<Boolean> response = new OExistsDatabaseResponse();

    return networkAdminOperation(request, response, "Error on checking existence of the remote storage: " + storage.getName());

  }

  /**
   * Checks if a database exists in the remote server.
   *
   * @param storageType Storage type between "plocal" or "memory".
   * @return true if exists, otherwise false
   * @throws IOException
   */
  public synchronized boolean existsDatabase(final String storageType) throws IOException {
    return existsDatabase(storage.getName(), storageType);
  }

  /**
   * Deprecated. Use dropDatabase() instead.
   *
   * @param storageType Storage type between "plocal" or "memory".
   * @return The instance itself. Useful to execute method in chain
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
   * @param storageType Storage type between "plocal" or "memory".
   * @return The instance itself. Useful to execute method in chain
   * @throws IOException
   */
  public synchronized OServerAdmin dropDatabase(final String iDatabaseName, final String storageType) throws IOException {

    OBinaryRequest request = new ODropDatabaseRequest(iDatabaseName, storageType);
    OBinaryResponse<Void> response = new ODropDatabaseResponse();
    networkAdminOperation(request, response, "Cannot delete the remote storage: " + storage.getName());

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
   * @return The instance itself. Useful to execute method in chain
   * @throws IOException
   */
  public synchronized OServerAdmin dropDatabase(final String storageType) throws IOException {
    return dropDatabase(storage.getName(), storageType);
  }

  /**
   * Freezes the database by locking it in exclusive mode.
   *
   * @param storageType Storage type between "plocal" or "memory".
   * @return
   * @throws IOException
   * @see #releaseDatabase(String)
   */
  public synchronized OServerAdmin freezeDatabase(final String storageType) throws IOException {

    OBinaryRequest request = new OFreezeDatabaseRequest(storage.getName(), storageType);
    OBinaryResponse<Void> response = new OFreezeDatabaseResponse();

    networkAdminOperation(request, response, "Cannot freeze the remote storage: " + storage.getName());

    return this;
  }

  /**
   * Releases a frozen database.
   *
   * @param storageType Storage type between "plocal" or "memory".
   * @return
   * @throws IOException
   * @see #freezeDatabase(String)
   */
  public synchronized OServerAdmin releaseDatabase(final String storageType) throws IOException {

    OBinaryRequest request = new OReleaseDatabaseRequest(storage.getName(), storageType);

    OBinaryResponse<Void> response = new OReleaseDatabaseResponse();

    networkAdminOperation(request, response, "Cannot release the remote storage: " + storage.getName());

    return this;
  }

  /**
   * Gets the cluster status.
   *
   * @return the JSON containing the current cluster structure
   */
  public ODocument clusterStatus() {

    OBinaryRequest request = new ODistributedStatusRequest();

    OBinaryResponse<ODocument> responseOperation = new ODistributedStatusResponse();

    ODocument response = storage.networkOperation(request, responseOperation, "Error on executing Cluster status ");

    OLogManager.instance().debug(this, "Cluster status %s", response.toJSON("prettyPrint"));
    return response;
  }

  public synchronized Map<String, String> getGlobalConfigurations() throws IOException {

    OBinaryRequest request = new OGetGlobalConfigurationsRequest();

    OBinaryResponse<Map<String, String>> response = new OGetGlobalConfigurationsResponse();

    return networkAdminOperation(request, response, "Cannot retrieve the configuration list");

  }

  public synchronized String getGlobalConfiguration(final OGlobalConfiguration config) throws IOException {

    OBinaryRequest request = new OGetGlobalConfigurationRequest(config.getKey());

    OBinaryResponse<String> response = new OGetGlobalConfigurationResponse();

    return networkAdminOperation(request, response, "Cannot retrieve the configuration value: " + config.getKey());
  }

  public synchronized OServerAdmin setGlobalConfiguration(final OGlobalConfiguration config, final Object iValue)
      throws IOException {

    OBinaryRequest request = new OSetGlobalConfigurationRequest(config.getKey(), iValue != null ? iValue.toString() : "");

    OBinaryResponse<Void> response = new OSetGlobalConfigurationResponse();

    networkAdminOperation(request, response, "Cannot set the configuration value: " + config.getKey());
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

  protected <T> T networkAdminOperation(final OBinaryRequest request, final OBinaryResponse<T> response,
      final String errorMessage) {
    return networkAdminOperation(new OStorageRemoteOperation<T>() {
      @Override
      public T execute(OChannelBinaryAsynchClient network, OStorageRemoteSession session) throws IOException {
        try {
          network.beginRequest(request.getCommand(), session);
          request.write(network, session, 0);
        } finally {
          network.endRequest();
        }
        final T res;
        try {
          storage.beginResponse(network, session);
          res = response.read(network, session);
        } finally {
          storage.endResponse(network);
        }
        storage.connectionManager.release(network);
        return res;
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
