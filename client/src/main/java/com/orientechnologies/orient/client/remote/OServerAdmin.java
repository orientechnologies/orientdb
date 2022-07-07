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

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.OrientDBRemote;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTxInternal;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;

/** Remote administration class of OrientDB Server instances. */
@Deprecated
public class OServerAdmin {
  protected OStorageRemoteSession session = new OStorageRemoteSession(-1);
  protected String clientType = OStorageRemote.DRIVER_NAME;
  protected boolean collectStats = true;
  private final ORemoteURLs urls;
  private final OrientDBRemote remote;
  private String user;
  private String password;
  private Optional<String> database;

  /**
   * Creates the object passing a remote URL to connect. sessionToken
   *
   * @param iURL URL to connect. It supports only the "remote" storage type.
   * @throws IOException
   */
  @Deprecated
  public OServerAdmin(String iURL) throws IOException {
    String url = iURL;
    if (url.startsWith(OEngineRemote.NAME)) url = url.substring(OEngineRemote.NAME.length() + 1);

    if (!url.contains("/")) url += "/";

    remote = (OrientDBRemote) ODatabaseDocumentTxInternal.getOrCreateRemoteFactory(url);
    urls = new ORemoteURLs(new String[] {}, remote.getContextConfiguration());
    String name = urls.parseServerUrls(url, remote.getContextConfiguration());
    if (name != null && name.length() != 0) {
      this.database = Optional.of(name);
    } else {
      this.database = Optional.empty();
    }
  }

  public OServerAdmin(OrientDBRemote remote, String url) throws IOException {
    this.remote = remote;
    urls = new ORemoteURLs(new String[] {}, remote.getContextConfiguration());
    String name = urls.parseServerUrls(url, remote.getContextConfiguration());
    if (name != null && name.length() != 0) {
      this.database = Optional.of(name);
    } else {
      this.database = Optional.empty();
    }
  }

  /**
   * Creates the object starting from an existent remote storage.
   *
   * @param iStorage
   */
  @Deprecated
  public OServerAdmin(final OStorageRemote iStorage) {
    this.remote = iStorage.context;
    urls = new ORemoteURLs(new String[] {}, remote.getContextConfiguration());
    urls.parseServerUrls(iStorage.getURL(), remote.getContextConfiguration());
    this.database = Optional.ofNullable(iStorage.getName());
  }

  /**
   * Connects to a remote server.
   *
   * @param iUserName Server's user name
   * @param iUserPassword Server's password for the user name used
   * @return The instance itself. Useful to execute method in chain
   * @throws IOException
   */
  @Deprecated
  public synchronized OServerAdmin connect(final String iUserName, final String iUserPassword)
      throws IOException {

    this.user = iUserName;
    this.password = iUserPassword;

    return this;
  }

  private void checkConnected() {
    if (user == null || password == null) {
      throw new OStorageException("OServerAdmin not connect use connect before do an operation");
    }
  }

  /**
   * Returns the list of databases on the connected remote server.
   *
   * @throws IOException
   */
  @Deprecated
  public synchronized Map<String, String> listDatabases() throws IOException {
    checkConnected();
    return remote.getDatabases(user, password);
  }

  /**
   * Returns the server information in form of document.
   *
   * @throws IOException
   */
  @Deprecated
  public synchronized ODocument getServerInfo() throws IOException {
    checkConnected();
    return remote.getServerInfo(user, password);
  }

  public int getSessionId() {
    return session.getSessionId();
  }

  /** Deprecated. Use the {@link #createDatabase(String, String)} instead. */
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
  @Deprecated
  public synchronized OServerAdmin createDatabase(final String iDatabaseType, String iStorageMode)
      throws IOException {
    return createDatabase(getStorageName(), iDatabaseType, iStorageMode);
  }

  public synchronized String getStorageName() {
    return database.get();
  }

  public synchronized OServerAdmin createDatabase(
      final String iDatabaseName, final String iDatabaseType, final String iStorageMode)
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
  public synchronized OServerAdmin createDatabase(
      final String iDatabaseName,
      final String iDatabaseType,
      final String iStorageMode,
      final String backupPath)
      throws IOException {
    checkConnected();
    ODatabaseType storageMode;
    if (iStorageMode == null) storageMode = ODatabaseType.PLOCAL;
    else storageMode = ODatabaseType.valueOf(iStorageMode.toUpperCase());
    OrientDBConfig config =
        OrientDBConfig.builder().addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, true).build();
    if (backupPath != null) {
      remote.restore(iDatabaseName, user, password, storageMode, backupPath, null);
    } else {
      remote.create(iDatabaseName, user, password, storageMode, config);
    }

    return this;
  }

  /**
   * Checks if a database exists in the remote server.
   *
   * @return true if exists, otherwise false
   */
  public synchronized boolean existsDatabase() throws IOException {
    return existsDatabase(database.get(), null);
  }

  /**
   * Checks if a database exists in the remote server.
   *
   * @param iDatabaseName The database name
   * @param storageType Storage type between "plocal" or "memory".
   * @return true if exists, otherwise false
   * @throws IOException
   */
  public synchronized boolean existsDatabase(final String iDatabaseName, final String storageType)
      throws IOException {
    checkConnected();
    return remote.exists(iDatabaseName, user, password);
  }

  /**
   * Checks if a database exists in the remote server.
   *
   * @param storageType Storage type between "plocal" or "memory".
   * @return true if exists, otherwise false
   * @throws IOException
   */
  public synchronized boolean existsDatabase(final String storageType) throws IOException {
    checkConnected();
    return existsDatabase(getStorageName(), storageType);
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
    return dropDatabase(getStorageName(), storageType);
  }

  /**
   * Drops a database from a remote server instance.
   *
   * @param iDatabaseName The database name
   * @param storageType Storage type between "plocal" or "memory".
   * @return The instance itself. Useful to execute method in chain
   * @throws IOException
   */
  public synchronized OServerAdmin dropDatabase(
      final String iDatabaseName, final String storageType) throws IOException {
    checkConnected();
    remote.drop(iDatabaseName, user, password);
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
    return dropDatabase(getStorageName(), storageType);
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
    checkConnected();
    remote.freezeDatabase(getStorageName(), user, password);
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
    checkConnected();
    remote.releaseDatabase(getStorageName(), user, password);
    return this;
  }

  /**
   * Gets the cluster status.
   *
   * @return the JSON containing the current cluster structure
   */
  public ODocument clusterStatus() {
    checkConnected();
    return remote.getClusterStatus(user, password);
  }

  public synchronized Map<String, String> getGlobalConfigurations() throws IOException {
    checkConnected();
    return remote.getGlobalConfigurations(user, password);
  }

  public synchronized String getGlobalConfiguration(final OGlobalConfiguration config)
      throws IOException {
    checkConnected();
    return remote.getGlobalConfiguration(user, password, config);
  }

  public synchronized OServerAdmin setGlobalConfiguration(
      final OGlobalConfiguration config, final Object iValue) throws IOException {
    checkConnected();
    remote.setGlobalConfiguration(user, password, config, iValue.toString());
    return this;
  }

  /** Close the connection if open. */
  public synchronized void close() {}

  public synchronized void close(boolean iForce) {}

  public synchronized String getURL() {
    String url = String.join(";", this.urls.getUrls());
    if (database.isPresent()) {
      url += "/" + database.get();
    }
    return "remote:" + url;
  }

  public boolean isConnected() {
    return user != null && password != null;
  }
}
