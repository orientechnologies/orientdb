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
package com.orientechnologies.orient.console;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.Set;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.console.TTYConsoleReader;
import com.orientechnologies.common.console.annotation.ConsoleCommand;
import com.orientechnologies.common.console.annotation.ConsoleParameter;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.orient.client.remote.OEngineRemote;
import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.client.remote.OStorageRemoteThread;
import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandManager;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.command.script.OCommandExecutorScript;
import com.orientechnologies.orient.core.command.script.OCommandScript;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.config.OStorageEntryConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.ODatabaseRecordAbstract;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.tool.ODatabaseCompare;
import com.orientechnologies.orient.core.db.tool.ODatabaseExport;
import com.orientechnologies.orient.core.db.tool.ODatabaseExportException;
import com.orientechnologies.orient.core.db.tool.ODatabaseImport;
import com.orientechnologies.orient.core.db.tool.ODatabaseImportException;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.intent.OIntentMassiveRead;
import com.orientechnologies.orient.core.iterator.OIdentifiableIterator;
import com.orientechnologies.orient.core.iterator.ORecordIteratorCluster;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.security.OUser;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;
import com.orientechnologies.orient.core.record.impl.ORecordFlat;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializerFactory;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerStringAbstract;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.OStorageProxy;
import com.orientechnologies.orient.core.storage.impl.local.ODataHoleInfo;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocal;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocalAbstract;

public class OConsoleDatabaseApp extends OrientConsole implements OCommandOutputListener, OProgressListener {
  protected ODatabaseDocument   currentDatabase;
  protected String              currentDatabaseName;
  protected ORecordInternal<?>  currentRecord;
  protected List<OIdentifiable> currentResultSet;
  protected OServerAdmin        serverAdmin;
  private int                   lastPercentStep;
  private String                currentDatabaseUserName;
  private String                currentDatabaseUserPassword;

  public OConsoleDatabaseApp(final String[] args) {
    super(args);
  }

  public static void main(final String[] args) {
    int result = 0;

    try {
      boolean tty = false;
      try {
        if (setTerminalToCBreak())
          tty = true;

        Runtime.getRuntime().addShutdownHook(new Thread() {
          @Override
          public void run() {
            try {
              stty("echo");
            } catch (Exception e) {
            }
          }
        });

      } catch (Exception e) {
      }

      final OConsoleDatabaseApp console = new OConsoleDatabaseApp(args);
      if (tty)
        console.setReader(new TTYConsoleReader());

      result = console.run();

    } finally {
      try {
        stty("echo");
      } catch (Exception e) {
      }
    }

    System.exit(result);
  }

  @Override
  protected boolean isCollectingCommands(final String iLine) {
    return iLine.startsWith("js");
  }

  @Override
  protected void onBefore() {
    super.onBefore();

    currentResultSet = new ArrayList<OIdentifiable>();

    OGlobalConfiguration.STORAGE_KEEP_OPEN.setValue(false);

    // DISABLE THE NETWORK AND STORAGE TIMEOUTS
    OGlobalConfiguration.STORAGE_LOCK_TIMEOUT.setValue(0);
    OGlobalConfiguration.NETWORK_LOCK_TIMEOUT.setValue(0);
    OGlobalConfiguration.CLIENT_CHANNEL_MIN_POOL.setValue(1);
    OGlobalConfiguration.CLIENT_CHANNEL_MAX_POOL.setValue(2);

    properties.put("limit", "20");
    properties.put("width", "132");
    properties.put("debug", "false");
    properties.put("maxBinaryDisplay", "160");

    OCommandManager.instance().registerExecutor(OCommandScript.class, OCommandExecutorScript.class);
  }

  @Override
  protected void onAfter() {
    super.onAfter();
    Orient.instance().shutdown();
  }

  @ConsoleCommand(aliases = { "use database" }, description = "Connect to a database or a remote Server instance")
  public void connect(
      @ConsoleParameter(name = "url", description = "The url of the remote server or the database to connect to in the format '<mode>:<path>'") String iURL,
      @ConsoleParameter(name = "user", description = "User name") String iUserName,
      @ConsoleParameter(name = "password", description = "User password", optional = true) String iUserPassword) throws IOException {
    disconnect();

    if (iUserPassword == null) {
      out.print("Enter password: ");
      final BufferedReader br = new BufferedReader(new InputStreamReader(this.in));
      iUserPassword = br.readLine();
      out.println();
    }

    currentDatabaseUserName = iUserName;
    currentDatabaseUserPassword = iUserPassword;

    if (iURL.contains("/")) {
      // OPEN DB
      out.print("Connecting to database [" + iURL + "] with user '" + iUserName + "'...");

      currentDatabase = new ODatabaseDocumentTx(iURL);
      if (currentDatabase == null)
        throw new OException("Database " + iURL + " not found");

      currentDatabase.registerListener(new OConsoleDatabaseListener(this));
      currentDatabase.open(iUserName, iUserPassword);

      currentDatabaseName = currentDatabase.getName();
      if (currentDatabase.getStorage() instanceof OStorageProxy)
        serverAdmin = new OServerAdmin(currentDatabase.getStorage().getURL());
    } else {
      // CONNECT TO REMOTE SERVER
      out.print("Connecting to remote Server instance [" + iURL + "] with user '" + iUserName + "'...");

      serverAdmin = new OServerAdmin(iURL).connect(iUserName, iUserPassword);
      currentDatabase = null;
      currentDatabaseName = null;
    }

    out.println("OK");
  }

  @ConsoleCommand(aliases = { "close database" }, description = "Disconnect from the current database")
  public void disconnect() {
    if (serverAdmin != null) {
      out.print("\nDisconnecting from remote server [" + serverAdmin.getURL() + "]...");
      serverAdmin.close(true);
      serverAdmin = null;
      out.println("OK");
    }

    if (currentDatabase != null) {
      out.print("\nDisconnecting from the database [" + currentDatabaseName + "]...");

      final OStorage stg = Orient.instance().getStorage(currentDatabase.getURL());

      currentDatabase.close();

      // FORCE CLOSING OF STORAGE: THIS CLEAN UP REMOTE CONNECTIONS
      if (stg != null)
        stg.close(true);

      currentDatabase = null;
      currentDatabaseName = null;
      currentRecord = null;

      out.println("OK");
    }
  }

  @ConsoleCommand(description = "Create a new database")
  public void createDatabase(
      @ConsoleParameter(name = "database-url", description = "The url of the database to create in the format '<mode>:<path>'") String iDatabaseURL,
      @ConsoleParameter(name = "user", description = "Server administrator name") String iUserName,
      @ConsoleParameter(name = "password", description = "Server administrator password") String iUserPassword,
      @ConsoleParameter(name = "storage-type", description = "The type of the storage. 'local' and 'plocal' for disk-based databases and 'memory' for in-memory database") String iStorageType,
      @ConsoleParameter(name = "db-type", optional = true, description = "The type of the database used between 'document' and 'graph'. By default is document.") String iDatabaseType)
      throws IOException {

    if (iDatabaseType == null)
      iDatabaseType = "document";

    out.println("Creating database [" + iDatabaseURL + "] using the storage type [" + iStorageType + "]...");

    currentDatabaseUserName = iUserName;
    currentDatabaseUserPassword = iUserPassword;

    if (iDatabaseURL.startsWith(OEngineRemote.NAME)) {
      // REMOTE CONNECTION
      final String dbURL = iDatabaseURL.substring(OEngineRemote.NAME.length() + 1);
      new OServerAdmin(dbURL).connect(iUserName, iUserPassword).createDatabase(iDatabaseType, iStorageType).close();
      connect(iDatabaseURL, OUser.ADMIN, OUser.ADMIN);

    } else {
      // LOCAL CONNECTION
      currentDatabase = Orient.instance().getDatabaseFactory().createDatabase(iDatabaseType, iDatabaseURL);
      currentDatabase.create();
      currentDatabaseName = currentDatabase.getName();
    }

    out.println("Database created successfully.");
    out.println("\nCurrent database is: " + iDatabaseURL);
  }

  @ConsoleCommand(description = "List all the databases available on the connected server")
  public void listDatabases() throws IOException {
    if (serverAdmin != null) {
      final Map<String, String> databases = serverAdmin.listDatabases();
      out.printf("\nFound %d databases:\n", databases.size());
      for (Entry<String, String> database : databases.entrySet()) {
        out.printf("\n* %s (%s)", database.getKey(), database.getValue().substring(0, database.getValue().indexOf(":")));
      }
    } else {
      out.println("Not connected to the Server instance");
    }
    out.println();
  }

  @ConsoleCommand(description = "Reload the database schema")
  public void reloadSchema() throws IOException {
    out.println("reloading database schema...");
    updateDatabaseInfo();
    out.println("\nDone.");
  }

  @ConsoleCommand(description = "Create a new data-segment in the current database.")
  public void createDatasegment(
      @ConsoleParameter(name = "datasegment-name", description = "The name of the data segment to create") final String iName,
      @ConsoleParameter(name = "datasegment-location", description = "The directory where to place the files", optional = true) final String iLocation) {
    checkForDatabase();

    if (iLocation != null)
      out.println("Creating data-segment [" + iName + "] in database " + currentDatabaseName + " in path: " + iLocation + "...");
    else
      out.println("Creating data-segment [" + iName + "] in database directory...");

    currentDatabase.addDataSegment(iName, iLocation);

    updateDatabaseInfo();
  }

  @ConsoleCommand(splitInWords = false, description = "Create a new cluster in the current database. The cluster can be physical or memory")
  public void createCluster(
      @ConsoleParameter(name = "command-text", description = "The command text to execute") String iCommandText) {
    sqlCommand("create", iCommandText, "\nCluster created correctly with id #%d\n", true);
    updateDatabaseInfo();
  }

  @ConsoleCommand(description = "Remove a cluster in the current database. The cluster can be physical or memory")
  public void dropCluster(
      @ConsoleParameter(name = "cluster-name", description = "The name or the id of the cluster to remove") String iClusterName) {
    checkForDatabase();

    out.println("Dropping cluster [" + iClusterName + "] in database " + currentDatabaseName + "...");

    boolean result = currentDatabase.dropCluster(iClusterName, true);

    if (!result) {
      // TRY TO GET AS CLUSTER ID
      try {
        int clusterId = Integer.parseInt(iClusterName);
        if (clusterId > -1) {
          result = currentDatabase.dropCluster(clusterId, true);
        }
      } catch (Exception e) {
      }
    }

    if (result)
      out.println("Cluster correctly removed");
    else
      out.println("Cannot find the cluster to remove");
    updateDatabaseInfo();
  }

  @ConsoleCommand(splitInWords = false, description = "Alters a cluster in the current database. The cluster can be physical or memory")
  public void alterCluster(@ConsoleParameter(name = "command-text", description = "The command text to execute") String iCommandText) {
    sqlCommand("alter", iCommandText, "\nCluster updated successfully\n", false);
    updateDatabaseInfo();
  }

  @ConsoleCommand(description = "Shows the holes in current storage")
  public void showHoles() throws IOException {
    checkForDatabase();

    if (!(currentDatabase.getStorage() instanceof OStorageLocal)) {
      out.println("Error: cannot show holes in databases different by local");
      return;
    }

    final OStorageLocal storage = (OStorageLocal) currentDatabase.getStorage();

    final List<ODataHoleInfo> result = storage.getHolesList();

    out.println("Found " + result.size() + " holes in database " + currentDatabaseName + ":");

    out.println("+----------------------+----------------------+");
    out.println("| Position             | Size (in bytes)      |");
    out.println("+----------------------+----------------------+");

    long size = 0;
    for (ODataHoleInfo ppos : result) {
      out.printf("| %20d | %20d |\n", ppos.dataOffset, ppos.size);
      size += ppos.size;
    }
    out.println("+----------------------+----------------------+");
    out.printf("| %20s | %20s |\n", "Total hole size", OFileUtils.getSizeAsString(size));
    out.println("+----------------------+----------------------+");
  }

  @ConsoleCommand(description = "Begins a transaction. All the changes will remain local")
  public void begin() throws IOException {
    checkForDatabase();

    if (currentDatabase.getTransaction().isActive()) {
      out.println("Error: an active transaction is currently open (id=" + currentDatabase.getTransaction().getId()
          + "). Commit or rollback before starting a new one.");
      return;
    }

    currentDatabase.begin();
    out.println("Transaction " + currentDatabase.getTransaction().getId() + " is running");
  }

  @ConsoleCommand(description = "Commits transaction changes to the database")
  public void commit() throws IOException {
    checkForDatabase();

    if (!currentDatabase.getTransaction().isActive()) {
      out.println("Error: no active transaction is currently open.");
      return;
    }

    final long begin = System.currentTimeMillis();

    final int txId = currentDatabase.getTransaction().getId();
    currentDatabase.commit();

    out.println("Transaction " + txId + " has been committed in " + (System.currentTimeMillis() - begin) + "ms");
  }

  @ConsoleCommand(description = "Rolls back transaction changes to the previous state")
  public void rollback() throws IOException {
    checkForDatabase();

    if (!currentDatabase.getTransaction().isActive()) {
      out.println("Error: no active transaction is running right now.");
      return;
    }

    final long begin = System.currentTimeMillis();

    final int txId = currentDatabase.getTransaction().getId();
    currentDatabase.rollback();
    out.println("Transaction " + txId + " has been rollbacked in " + (System.currentTimeMillis() - begin) + "ms");
  }

  @ConsoleCommand(splitInWords = false, description = "Truncate the class content in the current database")
  public void truncateClass(@ConsoleParameter(name = "text", description = "The name of the class to truncate") String iCommandText) {
    sqlCommand("truncate", iCommandText, "\nTruncated %d record(s) in %f sec(s).\n", true);
  }

  @ConsoleCommand(splitInWords = false, description = "Truncate the cluster content in the current database")
  public void truncateCluster(
      @ConsoleParameter(name = "text", description = "The name of the class to truncate") String iCommandText) {
    sqlCommand("truncate", iCommandText, "\nTruncated %d record(s) in %f sec(s).\n", true);
  }

  @ConsoleCommand(splitInWords = false, description = "Truncate a record deleting it at low level")
  public void truncateRecord(@ConsoleParameter(name = "text", description = "The record(s) to truncate") String iCommandText) {
    sqlCommand("truncate", iCommandText, "\nTruncated %d record(s) in %f sec(s).\n", true);
  }

  @ConsoleCommand(description = "Load a record in memory using passed fetch plan")
  public void loadRecord(
      @ConsoleParameter(name = "record-id", description = "The unique Record Id of the record to load. If you do not have the Record Id, execute a query first") String iRecordId,
      @ConsoleParameter(name = "fetch-plan", description = "The fetch plan to load the record with") String iFetchPlan) {
    loadRecordInternal(iRecordId, iFetchPlan);
  }

  @ConsoleCommand(description = "Load a record in memory and set it as the current")
  public void loadRecord(
      @ConsoleParameter(name = "record-id", description = "The unique Record Id of the record to load. If you do not have the Record Id, execute a query first") String iRecordId) {
    loadRecordInternal(iRecordId, null);
  }

  @ConsoleCommand(description = "Reloads a record using passed fetch plan")
  public void reloadRecord(
      @ConsoleParameter(name = "record-id", description = "The unique Record Id of the record to load. If you do not have the Record Id, execute a query first") String iRecordId,
      @ConsoleParameter(name = "fetch-plan", description = "The fetch plan to load the record with") String iFetchPlan) {
    reloadRecordInternal(iRecordId, iFetchPlan);
  }

  @ConsoleCommand(description = "Reload a record and set it as the current one")
  public void reloadRecord(
      @ConsoleParameter(name = "record-id", description = "The unique Record Id of the record to load. If you do not have the Record Id, execute a query first") String iRecordId) {
    reloadRecordInternal(iRecordId, null);
  }

  @ConsoleCommand(splitInWords = false, description = "Explain how a command is executed profiling it")
  public void explain(@ConsoleParameter(name = "command-text", description = "The command text to execute") String iCommandText) {
    Object result = sqlCommand("explain", iCommandText, "\nProfiled command '%s' in %f sec(s):\n", true);
    if (result != null && result instanceof ODocument) {
      out.printf(((ODocument) result).toJSON());
    }
  }

  @ConsoleCommand(splitInWords = false, description = "Executes a command inside a transaction")
  public void transactional(@ConsoleParameter(name = "command-text", description = "The command to execute") String iCommandText) {
    sqlCommand("transactional", iCommandText, "\nResult: '%s'. Executed in %f sec(s).\n", true);
  }

  @ConsoleCommand(splitInWords = false, description = "Insert a new record into the database")
  public void insert(@ConsoleParameter(name = "command-text", description = "The command text to execute") String iCommandText) {
    sqlCommand("insert", iCommandText, "\nInserted record '%s' in %f sec(s).\n", true);
  }

  @ConsoleCommand(splitInWords = false, description = "Create a new vertex into the database")
  public void createVertex(@ConsoleParameter(name = "command-text", description = "The command text to execute") String iCommandText) {
    sqlCommand("create", iCommandText, "\nCreated vertex '%s' in %f sec(s).\n", true);
  }

  @ConsoleCommand(splitInWords = false, description = "Create a new edge into the database")
  public void createEdge(@ConsoleParameter(name = "command-text", description = "The command text to execute") String iCommandText) {
    sqlCommand("create", iCommandText, "\nCreated edge '%s' in %f sec(s).\n", true);
  }

  @ConsoleCommand(splitInWords = false, description = "Update records in the database")
  public void update(@ConsoleParameter(name = "command-text", description = "The command text to execute") String iCommandText) {
    sqlCommand("update", iCommandText, "\nUpdated %d record(s) in %f sec(s).\n", true);
    updateDatabaseInfo();
    currentDatabase.getLevel1Cache().invalidate();
    currentDatabase.getLevel2Cache().clear();
  }

  @ConsoleCommand(splitInWords = false, description = "Delete records from the database")
  public void delete(@ConsoleParameter(name = "command-text", description = "The command text to execute") String iCommandText) {
    sqlCommand("delete", iCommandText, "\nDelete %d record(s) in %f sec(s).\n", true);
    updateDatabaseInfo();
    currentDatabase.getLevel1Cache().invalidate();
    currentDatabase.getLevel2Cache().clear();
  }

  @ConsoleCommand(splitInWords = false, description = "Grant privileges to a role")
  public void grant(@ConsoleParameter(name = "text", description = "Grant command") String iCommandText) {
    sqlCommand("grant", iCommandText, "\nPrivilege granted to the role: %s\n", true);
  }

  @ConsoleCommand(splitInWords = false, description = "Revoke privileges to a role")
  public void revoke(@ConsoleParameter(name = "text", description = "Revoke command") String iCommandText) {
    sqlCommand("revoke", iCommandText, "\nPrivilege revoked to the role: %s\n", true);
  }

  @ConsoleCommand(splitInWords = false, description = "Create a link from a JOIN")
  public void createLink(@ConsoleParameter(name = "command-text", description = "The command text to execute") String iCommandText) {
    sqlCommand("create", iCommandText, "\nCreated %d link(s) in %f sec(s).\n", true);
  }

  @ConsoleCommand(splitInWords = false, description = "Find all references the target record id @rid")
  public void findReferences(
      @ConsoleParameter(name = "command-text", description = "The command text to execute") String iCommandText) {
    sqlCommand("find", iCommandText, "\nFound %s in %f sec(s).\n", true);
  }

  @ConsoleCommand(splitInWords = false, description = "Alter a database property")
  public void alterDatabase(
      @ConsoleParameter(name = "command-text", description = "The command text to execute") String iCommandText) {
    sqlCommand("alter", iCommandText, "\nDatabase updated successfully\n", false);
    updateDatabaseInfo();
  }

  @ConsoleCommand(description = "Freeze database and flush on the disk")
  public void freezeDatabase(
      @ConsoleParameter(name = "storage-type", description = "Storage type of server database", optional = true) String storageType)
      throws IOException {
    checkForDatabase();

    final String dbName = currentDatabase.getName();

    if (currentDatabase.getURL().startsWith(OEngineRemote.NAME)) {
      if (serverAdmin == null) {
        out.println("\nCannot freeze a remote database without connecting to the server with a valid server's user");
        return;
      }

      if (storageType == null)
        storageType = "plocal";

      new OServerAdmin(currentDatabase.getURL()).connect(currentDatabaseUserName, currentDatabaseUserPassword).freezeDatabase(
          storageType);
    } else {
      // LOCAL CONNECTION
      currentDatabase.freeze();
    }

    out.println("\nDatabase '" + dbName + "' was frozen successfully");
  }

  @ConsoleCommand(description = "Release database after freeze")
  public void releaseDatabase(
      @ConsoleParameter(name = "storage-type", description = "Storage type of server database", optional = true) String storageType)
      throws IOException {
    checkForDatabase();

    final String dbName = currentDatabase.getName();

    if (currentDatabase.getURL().startsWith(OEngineRemote.NAME)) {
      if (serverAdmin == null) {
        out.println("\nCannot release a remote database without connecting to the server with a valid server's user");
        return;
      }

      if (storageType == null)
        storageType = "plocal";

      new OServerAdmin(currentDatabase.getURL()).connect(currentDatabaseUserName, currentDatabaseUserPassword).releaseDatabase(
          storageType);
    } else {
      // LOCAL CONNECTION
      currentDatabase.release();
    }

    out.println("\nDatabase '" + dbName + "' was released successfully");
  }

  @ConsoleCommand(description = "Freeze clusters and flush on the disk")
  public void freezeCluster(
      @ConsoleParameter(name = "cluster-name", description = "The name of the cluster to freeze") String iClusterName,
      @ConsoleParameter(name = "storage-type", description = "Storage type of server database", optional = true) String storageType)
      throws IOException {
    checkForDatabase();

    final int clusterId = currentDatabase.getClusterIdByName(iClusterName);

    if (currentDatabase.getURL().startsWith(OEngineRemote.NAME)) {
      if (serverAdmin == null) {
        out.println("\nCannot freeze a remote database without connecting to the server with a valid server's user");
        return;
      }

      if (storageType == null)
        storageType = "plocal";

      new OServerAdmin(currentDatabase.getURL()).connect(currentDatabaseUserName, currentDatabaseUserPassword).freezeCluster(
          clusterId, storageType);
    } else {
      // LOCAL CONNECTION
      currentDatabase.freezeCluster(clusterId);
    }

    out.println("\nCluster '" + iClusterName + "' was frozen successfully");
  }

  @ConsoleCommand(description = "Release cluster after freeze")
  public void releaseCluster(
      @ConsoleParameter(name = "cluster-name", description = "The name of the cluster to unfreeze") String iClusterName,
      @ConsoleParameter(name = "storage-type", description = "Storage type of server database", optional = true) String storageType)
      throws IOException {
    checkForDatabase();

    final int clusterId = currentDatabase.getClusterIdByName(iClusterName);

    if (currentDatabase.getURL().startsWith(OEngineRemote.NAME)) {
      if (serverAdmin == null) {
        out.println("\nCannot freeze a remote database without connecting to the server with a valid server's user");
        return;
      }

      if (storageType == null)
        storageType = "plocal";

      new OServerAdmin(currentDatabase.getURL()).connect(currentDatabaseUserName, currentDatabaseUserPassword).releaseCluster(
          clusterId, storageType);
    } else {
      // LOCAL CONNECTION
      currentDatabase.releaseCluster(clusterId);
    }

    out.println("\nCluster '" + iClusterName + "' was released successfully");
  }

  @ConsoleCommand(splitInWords = false, description = "Alter a class in the database schema")
  public void alterClass(@ConsoleParameter(name = "command-text", description = "The command text to execute") String iCommandText) {
    sqlCommand("alter", iCommandText, "\nClass updated successfully\n", false);
    updateDatabaseInfo();
  }

  @ConsoleCommand(splitInWords = false, description = "Create a class")
  public void createClass(@ConsoleParameter(name = "command-text", description = "The command text to execute") String iCommandText) {
    sqlCommand("create", iCommandText, "\nClass created successfully. Total classes in database now: %d\n", true);
    updateDatabaseInfo();
  }

  @ConsoleCommand(splitInWords = false, description = "Alter a class property in the database schema")
  public void alterProperty(
      @ConsoleParameter(name = "command-text", description = "The command text to execute") String iCommandText) {
    sqlCommand("alter", iCommandText, "\nProperty updated successfully\n", false);
    updateDatabaseInfo();
  }

  @ConsoleCommand(splitInWords = false, description = "Create a property")
  public void createProperty(
      @ConsoleParameter(name = "command-text", description = "The command text to execute") String iCommandText) {
    sqlCommand("create", iCommandText, "\nProperty created successfully with id=%d\n", true);
    updateDatabaseInfo();
  }

  /***
   * @author Claudio Tesoriero
   * @param iCommandText
   */
  @ConsoleCommand(splitInWords = false, description = "Create a stored function")
  public void createFunction(
      @ConsoleParameter(name = "command-text", description = "The command text to execute") String iCommandText) {
    sqlCommand("create", iCommandText, "\nFunction created successfully with id=%s\n", true);
    updateDatabaseInfo();
  }

  @ConsoleCommand(splitInWords = false, description = "Traverse records and display the results")
  public void traverse(@ConsoleParameter(name = "query-text", description = "The traverse to execute") String iQueryText) {
    final int limit;
    if (iQueryText.contains("limit")) {
      // RESET CONSOLE FLAG
      limit = -1;
    } else {
      limit = Integer.parseInt((String) properties.get("limit"));
    }

    long start = System.currentTimeMillis();
    currentResultSet = currentDatabase.command(new OCommandSQL("traverse " + iQueryText)).execute();

    float elapsedSeconds = getElapsedSecs(start);

    dumpResultSet(limit);

    out.println("\n" + currentResultSet.size() + " item(s) found. Traverse executed in " + elapsedSeconds + " sec(s).");
  }

  @ConsoleCommand(splitInWords = false, description = "Execute a query against the database and display the results")
  public void select(@ConsoleParameter(name = "query-text", description = "The query to execute") String iQueryText) {
    checkForDatabase();

    if (iQueryText == null)
      return;

    iQueryText = iQueryText.trim();

    if (iQueryText.length() == 0 || iQueryText.equalsIgnoreCase("select"))
      return;

    iQueryText = "select " + iQueryText;

    final int limit;
    if (iQueryText.contains("limit")) {
      limit = -1;
    } else {
      limit = Integer.parseInt((String) properties.get("limit"));
    }

    final long start = System.currentTimeMillis();
    currentResultSet = currentDatabase.query(new OSQLSynchQuery<ODocument>(iQueryText, limit).setFetchPlan("*:1"));

    float elapsedSeconds = getElapsedSecs(start);

    dumpResultSet(limit);

    out.println("\n" + currentResultSet.size() + " item(s) found. Query executed in " + elapsedSeconds + " sec(s).");
  }

  @SuppressWarnings("unchecked")
  @ConsoleCommand(splitInWords = false, description = "Execute javascript commands in the console")
  public void js(
      @ConsoleParameter(name = "text", description = "The javascript to execute. Use 'db' to reference to a document database, 'gdb' for a graph database") final String iText) {
    if (iText == null)
      return;

    currentResultSet.clear();

    final OCommandExecutorScript cmd = new OCommandExecutorScript();
    cmd.parse(new OCommandScript("Javascript", iText));

    long start = System.currentTimeMillis();

    final Object result = cmd.execute(null);

    float elapsedSeconds = getElapsedSecs(start);

    if (OMultiValue.isMultiValue(result)) {
      if (result instanceof List<?>)
        currentResultSet = (List<OIdentifiable>) result;
      else if (result instanceof Collection<?>) {
        currentResultSet = new ArrayList<OIdentifiable>();
        currentResultSet.addAll((Collection<? extends OIdentifiable>) result);
      } else if (result.getClass().isArray()) {
        currentResultSet = new ArrayList<OIdentifiable>();
        for (OIdentifiable o : (OIdentifiable[]) result)
          currentResultSet.add(o);
      }
      dumpResultSet(-1);
      out.printf("Client side script executed in %f sec(s). Returned %d records", elapsedSeconds, currentResultSet.size());
    } else
      out.printf("Client side script executed in %f sec(s). Value returned is: %s", elapsedSeconds, result);
  }

  @SuppressWarnings("unchecked")
  @ConsoleCommand(splitInWords = false, description = "Execute javascript commands against a remote server")
  public void jss(
      @ConsoleParameter(name = "text", description = "The javascript to execute. Use 'db' to reference to a document database, 'gdb' for a graph database") final String iText) {
    checkForRemoteServer();

    if (iText == null)
      return;

    currentResultSet.clear();

    long start = System.currentTimeMillis();
    Object result = currentDatabase.command(new OCommandScript("Javascript", iText.toString())).execute();
    float elapsedSeconds = getElapsedSecs(start);

    if (OMultiValue.isMultiValue(result)) {
      if (result instanceof List<?>)
        currentResultSet = (List<OIdentifiable>) result;
      else if (result instanceof Collection<?>) {
        currentResultSet = new ArrayList<OIdentifiable>();
        currentResultSet.addAll((Collection<? extends OIdentifiable>) result);
      } else if (result.getClass().isArray()) {
        currentResultSet = new ArrayList<OIdentifiable>();
        for (OIdentifiable o : (OIdentifiable[]) result)
          currentResultSet.add(o);
      }
      dumpResultSet(-1);
      out.printf("Server side script executed in %f sec(s). Returned %d records", elapsedSeconds, currentResultSet.size());
    } else
      out.printf("Server side script executed in %f sec(s). Value returned is: %s", elapsedSeconds, result);
  }

  @ConsoleCommand(splitInWords = false, description = "Create an index against a property")
  public void createIndex(@ConsoleParameter(name = "command-text", description = "The command text to execute") String iCommandText)
      throws IOException {
    out.println("\nCreating index...");

    sqlCommand("create", iCommandText, "\nCreated index successfully with %d entries in %f sec(s).\n", true);
    updateDatabaseInfo();
    out.println("\nIndex created successfully");
  }

  @ConsoleCommand(description = "Delete the current database")
  public void dropDatabase(
      @ConsoleParameter(name = "storage-type", description = "Storage type of server database", optional = true) String storageType)
      throws IOException {
    checkForDatabase();

    final String dbName = currentDatabase.getName();

    if (currentDatabase.getURL().startsWith(OEngineRemote.NAME)) {
      if (serverAdmin == null) {
        out.println("\nCannot drop a remote database without connecting to the server with a valid server's user");
        return;
      }

      if (storageType == null)
        storageType = "plocal";

      // REMOTE CONNECTION
      final String dbURL = currentDatabase.getURL().substring(OEngineRemote.NAME.length() + 1);
      new OServerAdmin(dbURL).connect(currentDatabaseUserName, currentDatabaseUserPassword).dropDatabase(storageType);
    } else {
      // LOCAL CONNECTION
      currentDatabase.drop();
      currentDatabase = null;
      currentDatabaseName = null;
    }

    out.println("\nDatabase '" + dbName + "' deleted successfully");
  }

  @ConsoleCommand(description = "Delete the specified database")
  public void dropDatabase(
      @ConsoleParameter(name = "database-url", description = "The url of the database to drop in the format '<mode>:<path>'") String iDatabaseURL,
      @ConsoleParameter(name = "user", description = "Server administrator name") String iUserName,
      @ConsoleParameter(name = "password", description = "Server administrator password") String iUserPassword,
      @ConsoleParameter(name = "storage-type", description = "Storage type of server database", optional = true) String storageType)
      throws IOException {

    if (iDatabaseURL.startsWith(OEngineRemote.NAME)) {
      // REMOTE CONNECTION
      final String dbURL = iDatabaseURL.substring(OEngineRemote.NAME.length() + 1);
      serverAdmin = new OServerAdmin(dbURL).connect(iUserName, iUserPassword);
      serverAdmin.dropDatabase(storageType);
      disconnect();
    } else {
      // LOCAL CONNECTION
      currentDatabase = new ODatabaseDocumentTx(iDatabaseURL);
      if (currentDatabase.exists()) {
        currentDatabase.open(iUserName, iUserPassword);
        currentDatabase.drop();
      } else
        out.println("\nCannot drop database '" + iDatabaseURL + "' because was not found");

      currentDatabase = null;
      currentDatabaseName = null;
    }

    out.println("\nDatabase '" + iDatabaseURL + "' deleted successfully");
  }

  @ConsoleCommand(splitInWords = false, description = "Remove an index")
  public void dropIndex(@ConsoleParameter(name = "command-text", description = "The command text to execute") String iCommandText)
      throws IOException {
    out.println("\nRemoving index...");

    sqlCommand("drop", iCommandText, "\nDropped index in %f sec(s).\n", false);
    updateDatabaseInfo();
    out.println("\nIndex removed successfully");
  }

  @ConsoleCommand(splitInWords = false, description = "Rebuild an index if it is automatic")
  public void rebuildIndex(@ConsoleParameter(name = "command-text", description = "The command text to execute") String iCommandText)
      throws IOException {
    out.println("\nRebuilding index(es)...");

    sqlCommand("rebuild", iCommandText, "\nRebuilt index(es). Found %d link(s) in %f sec(s).\n", true);
    updateDatabaseInfo();
    out.println("\nIndex(es) rebuilt successfully");
  }

  @ConsoleCommand(splitInWords = false, description = "Remove a class from the schema")
  public void dropClass(@ConsoleParameter(name = "command-text", description = "The command text to execute") String iCommandText)
      throws IOException {
    sqlCommand("drop", iCommandText, "\nRemoved class in %f sec(s).\n", false);
    updateDatabaseInfo();
  }

  @ConsoleCommand(splitInWords = false, description = "Remove a property from a class")
  public void dropProperty(@ConsoleParameter(name = "command-text", description = "The command text to execute") String iCommandText)
      throws IOException {
    sqlCommand("drop", iCommandText, "\nRemoved class property in %f sec(s).\n", false);
    updateDatabaseInfo();
  }

  @ConsoleCommand(description = "Browse all records of a class")
  public void browseClass(@ConsoleParameter(name = "class-name", description = "The name of the class") final String iClassName) {
    checkForDatabase();

    currentResultSet.clear();

    final int limit = Integer.parseInt((String) properties.get("limit"));

    OIdentifiableIterator<?> it = currentDatabase.browseClass(iClassName);

    browseRecords(limit, it);
  }

  @ConsoleCommand(description = "Browse all records of a cluster")
  public void browseCluster(
      @ConsoleParameter(name = "cluster-name", description = "The name of the cluster") final String iClusterName) {
    checkForDatabase();

    currentResultSet.clear();

    final int limit = Integer.parseInt((String) properties.get("limit"));

    final ORecordIteratorCluster<?> it = currentDatabase.browseCluster(iClusterName);

    browseRecords(limit, it);
  }

  @ConsoleCommand(aliases = { "display" }, description = "Display current record attributes")
  public void displayRecord(
      @ConsoleParameter(name = "number", description = "The number of the record in the most recent result set") final String iRecordNumber) {
    checkForDatabase();

    if (iRecordNumber == null)
      checkCurrentObject();
    else {
      int recNumber = Integer.parseInt(iRecordNumber);
      if (currentResultSet.size() == 0)
        throw new OException("No result set where to find the requested record. Execute a query first.");

      if (currentResultSet.size() <= recNumber)
        throw new OException("The record requested is not part of current result set (0"
            + (currentResultSet.size() > 0 ? "-" + (currentResultSet.size() - 1) : "") + ")");

      currentRecord = currentResultSet.get(recNumber).getRecord();
    }

    dumpRecordDetails();
  }

  @ConsoleCommand(description = "Display a record as raw bytes")
  public void displayRawRecord(@ConsoleParameter(name = "rid", description = "The record id to display") final String iRecordId) {
    checkForDatabase();

    ORecordId rid = new ORecordId(iRecordId);
    final ORawBuffer buffer = currentDatabase.getStorage().readRecord(rid, null, false, null, false).getResult();

    if (buffer == null)
      throw new OException("The record has been deleted");

    String content;
    if (Integer.parseInt(properties.get("maxBinaryDisplay")) < buffer.buffer.length)
      content = new String(Arrays.copyOf(buffer.buffer, Integer.parseInt(properties.get("maxBinaryDisplay"))));
    else
      content = new String(buffer.buffer);

    out.println("Raw record content. The size is " + buffer.buffer.length + " bytes, while settings force to print first "
        + content.length() + " bytes:\n\n" + new String(content));
  }

  @ConsoleCommand(aliases = { "status" }, description = "Display information about the database")
  public void info() {
    if (currentDatabaseName != null) {
      out.println("Current database: " + currentDatabaseName + " (url=" + currentDatabase.getURL() + ")");

      final OStorage stg = currentDatabase.getStorage();

      if (stg instanceof OStorageRemoteThread) {
        final ODocument clusterConfig = ((OStorageRemoteThread) stg).getClusterConfiguration();
        if (clusterConfig != null)
          out.println("\nCluster configuration: " + clusterConfig.toJSON("indent:2"));
        else
          out.println("\nCluster configuration: none");
      } else if (stg instanceof OStorageLocal) {
        final OStorageLocal localStorage = (OStorageLocal) stg;

        long holeSize = localStorage.getHoleSize();

        out.print("\nFragmented at " + (float) (holeSize * 100f / localStorage.getSize()) + "%");
        out.println(" (" + localStorage.getHoles() + " holes, total size of holes: " + OFileUtils.getSizeAsString(holeSize) + ")");
      }

      listProperties();
      listClusters();
      listClasses();
      listIndexes();
    }
  }

  @ConsoleCommand(description = "Display the database properties")
  public void listProperties() {
    if (currentDatabase == null)
      return;

    final OStorage stg = currentDatabase.getStorage();

    if (stg.getConfiguration().properties != null) {
      out.println("\nDB CUSTOM PROPERTIES:\n");
      for (OStorageEntryConfiguration cfg : stg.getConfiguration().properties)
        out.println(String.format("%-30s : %s", cfg.name, cfg.value));
    }
  }

  @ConsoleCommand(aliases = { "desc" }, description = "Display the schema of a class")
  public void infoClass(@ConsoleParameter(name = "class-name", description = "The name of the class") final String iClassName) {
    if (currentDatabaseName == null) {
      out.println("No database selected yet.");
      return;
    }

    final OClass cls = currentDatabase.getMetadata().getSchema().getClass(iClassName);

    if (cls == null) {
      out.println("! Class '" + iClassName + "' does not exist in the database '" + currentDatabaseName + "'");
      return;
    }

    out.println();
    out.println("Class................: " + cls);
    if (cls.getShortName() != null)
      out.println("Alias................: " + cls.getShortName());
    if (cls.getSuperClass() != null)
      out.println("Super class..........: " + cls.getSuperClass());
    out.println("Default cluster......: " + currentDatabase.getClusterNameById(cls.getDefaultClusterId()) + " (id="
        + cls.getDefaultClusterId() + ")");
    out.println("Supported cluster ids: " + Arrays.toString(cls.getClusterIds()));

    if (cls.getBaseClasses().hasNext()) {
      out.print("Base classes.........: ");
      int i = 0;
      for (Iterator<OClass> it = cls.getBaseClasses(); it.hasNext();) {
        if (i > 0)
          out.print(", ");
        out.print(it.next().getName());
        ++i;
      }
      out.println();
    }

    if (cls.properties().size() > 0) {
      out.println("Properties:");
      out.println("-------------------------------+-------------+-------------------------------+-----------+----------+----------+-----------+-----------+");
      out.println(" NAME                          | TYPE        | LINKED TYPE/CLASS             | MANDATORY | READONLY | NOT NULL |    MIN    |    MAX    |");
      out.println("-------------------------------+-------------+-------------------------------+-----------+----------+----------+-----------+-----------+");

      for (final OProperty p : cls.properties()) {
        try {
          out.printf(" %-30s| %-12s| %-30s| %-10s| %-9s| %-9s| %-10s| %-10s|\n", p.getName(), p.getType(),
              p.getLinkedClass() != null ? p.getLinkedClass() : p.getLinkedType(), p.isMandatory(), p.isReadonly(), p.isNotNull(),
              p.getMin() != null ? p.getMin() : "", p.getMax() != null ? p.getMax() : "");
        } catch (Exception e) {
        }
      }
      out.println("-------------------------------+-------------+-------------------------------+-----------+----------+----------+-----------+-----------+");
    }

    final Set<OIndex<?>> indexes = cls.getClassIndexes();
    if (!indexes.isEmpty()) {
      out.println("Indexes (" + indexes.size() + " altogether):");
      out.println("-------------------------------+----------------+");
      out.println(" NAME                          | PROPERTIES     |");
      out.println("-------------------------------+----------------+");
      for (final OIndex<?> index : indexes) {
        final OIndexDefinition indexDefinition = index.getDefinition();
        if (indexDefinition != null) {
          final List<String> fields = indexDefinition.getFields();
          out.printf(" %-30s| %-15s|\n", index.getName(), fields.get(0) + (fields.size() > 1 ? " (+)" : ""));

          for (int i = 1; i < fields.size(); i++) {
            if (i < fields.size() - 1)
              out.printf(" %-30s| %-15s|\n", "", fields.get(i) + " (+)");
            else
              out.printf(" %-30s| %-15s|\n", "", fields.get(i));
          }
        } else {
          out.printf(" %-30s| %-15s|\n", index.getName(), "");
        }
      }
      out.println("-------------------------------+----------------+");
    }
  }

  @ConsoleCommand(description = "Display all indexes", aliases = { "indexes" })
  public void listIndexes() {
    if (currentDatabaseName != null) {
      out.println("\nINDEXES:");
      out.println("----------------------------------------------+------------+-----------------------+----------------+-----------+");
      out.println(" NAME                                         | TYPE       |         CLASS         |     FIELDS     | RECORDS   |");
      out.println("----------------------------------------------+------------+-----------------------+----------------+-----------+");

      int totalIndexes = 0;
      long totalRecords = 0;

      final List<OIndex<?>> indexes = new ArrayList<OIndex<?>>(currentDatabase.getMetadata().getIndexManager().getIndexes());
      Collections.sort(indexes, new Comparator<OIndex<?>>() {
        public int compare(OIndex<?> o1, OIndex<?> o2) {
          return o1.getName().compareToIgnoreCase(o2.getName());
        }
      });

      for (final OIndex<?> index : indexes) {
        try {
          final OIndexDefinition indexDefinition = index.getDefinition();
          if (indexDefinition == null || indexDefinition.getClassName() == null) {
            out.printf(" %-45s| %-10s | %-22s| %-15s|%10d |\n", format(index.getName(), 45), format(index.getType(), 10), "", "",
                index.getSize());
          } else {
            final List<String> fields = indexDefinition.getFields();
            if (fields.size() == 1) {
              out.printf(" %-45s| %-10s | %-22s| %-15s|%10d |\n", format(index.getName(), 45), format(index.getType(), 10),
                  format(indexDefinition.getClassName(), 22), format(fields.get(0), 10), index.getSize());
            } else {
              out.printf(" %-45s| %-10s | %-22s| %-15s|%10d |\n", format(index.getName(), 45), format(index.getType(), 10),
                  format(indexDefinition.getClassName(), 22), format(fields.get(0), 10), index.getSize());
              for (int i = 1; i < fields.size(); i++) {
                out.printf(" %-45s| %-10s | %-22s| %-15s|%10s |\n", "", "", "", fields.get(i), "");
              }
            }
          }

          totalIndexes++;
          totalRecords += index.getSize();
        } catch (Exception e) {
        }
      }
      out.println("----------------------------------------------+------------+-----------------------+----------------+-----------+");
      out.printf(" TOTAL = %-3d                                                                                    %15d |\n",
          totalIndexes, totalRecords);
      out.println("----------------------------------------------------------------------------------------------------------------+\n");
    } else
      out.println("No database selected yet.");
  }

  @ConsoleCommand(description = "Display all the configured clusters", aliases = { "clusters" })
  public void listClusters() {
    if (currentDatabaseName != null) {
      out.println("\nCLUSTERS:");
      out.println("----------------------------------------------+------+---------------------+-----------+--------------+");
      out.println(" NAME                                         |  ID  | TYPE                | RECORDS   | SIZE         |");
      out.println("----------------------------------------------+------+---------------------+-----------+--------------+");

      int clusterId;
      String clusterType = null;
      long totalElements = 0;
      long count;
      long size = 0;
      long totalSize = 0;

      final List<String> clusters = new ArrayList<String>(currentDatabase.getClusterNames());
      Collections.sort(clusters);

      for (String clusterName : clusters) {
        try {
          clusterId = currentDatabase.getClusterIdByName(clusterName);
          clusterType = currentDatabase.getClusterType(clusterName);

          count = currentDatabase.countClusterElements(clusterName);
          totalElements += count;

          if (!(currentDatabase.getStorage() instanceof OStorageProxy)) {
            totalSize += size;
            out.printf(" %-45s|%6d| %-20s|%10d |%13s |\n", format(clusterName, 45), clusterId, clusterType, count,
                OFileUtils.getSizeAsString(size));
          } else {
            out.printf(" %-45s|%6d| %-20s|%10d |%13s |\n", format(clusterName, 45), clusterId, clusterType, count, "Not supported");
          }
        } catch (Exception e) {
        }
      }
      out.println("----------------------------------------------+------+---------------------+-----------+--------------+");
      out.printf(" TOTAL                                                                 %15d |%13s |\n", totalElements,
          OFileUtils.getSizeAsString(totalSize));
      out.println("---------------------------------------------------------------------------------------+--------------+");
    } else
      out.println("No database selected yet.");
  }

  @ConsoleCommand(description = "Display all the configured classes", aliases = { "classes" })
  public void listClasses() {
    if (currentDatabaseName != null) {
      out.println("\nCLASSES:");
      out.println("----------------------------------------------+---------------------+-----------+");
      out.println(" NAME                                         | CLUSTERS            | RECORDS   |");
      out.println("----------------------------------------------+---------------------+-----------+");

      long totalElements = 0;
      long count;

      final List<OClass> classes = new ArrayList<OClass>(currentDatabase.getMetadata().getSchema().getClasses());
      Collections.sort(classes, new Comparator<OClass>() {
        public int compare(OClass o1, OClass o2) {
          return o1.getName().compareToIgnoreCase(o2.getName());
        }
      });

      for (OClass cls : classes) {
        try {
          StringBuilder clusters = new StringBuilder();
          for (int i = 0; i < cls.getClusterIds().length; ++i) {
            if (i > 0)
              clusters.append(", ");
            clusters.append(cls.getClusterIds()[i]);
          }

          count = currentDatabase.countClass(cls.getName());
          totalElements += count;

          out.printf(" %-45s| %-20s|%10d |\n", format(cls.getName(), 45), clusters.toString(), count);
        } catch (Exception e) {
        }
      }
      out.println("----------------------------------------------+---------------------+-----------+");
      out.printf(" TOTAL                                                          %15d |\n", totalElements);
      out.println("--------------------------------------------------------------------------------+");

    } else
      out.println("No database selected yet.");
  }

  @ConsoleCommand(description = "Display all keys in the database dictionary")
  public void dictionaryKeys() {
    checkForDatabase();

    Iterable<Object> keys = currentDatabase.getDictionary().keys();

    int i = 0;
    for (Object k : keys) {
      out.print(String.format("#%d: %s\n", i++, k));
    }

    out.println("Found " + i + " keys:");
  }

  @ConsoleCommand(description = "Loook up a record using the dictionary. If found, set it as the current record")
  public void dictionaryGet(@ConsoleParameter(name = "key", description = "The key to search") final String iKey) {
    checkForDatabase();

    currentRecord = currentDatabase.getDictionary().get(iKey);
    if (currentRecord == null)
      out.println("Entry not found in dictionary.");
    else {
      currentRecord = (ORecordInternal<?>) currentRecord.load();
      displayRecord(null);
    }
  }

  @ConsoleCommand(description = "Insert or modify an entry in the database dictionary. The entry is comprised of key=String, value=record-id")
  public void dictionaryPut(@ConsoleParameter(name = "key", description = "The key to bind") final String iKey,
      @ConsoleParameter(name = "record-id", description = "The record-id of the record to bind to the key") final String iRecordId) {
    checkForDatabase();

    currentRecord = currentDatabase.load(new ORecordId(iRecordId));
    if (currentRecord == null)
      out.println("Error: record with id '" + iRecordId + "' was not found in database");
    else {
      currentDatabase.getDictionary().put(iKey, (ODocument) currentRecord);
      displayRecord(null);
      out.println("The entry " + iKey + "=" + iRecordId + " has been inserted in the database dictionary");
    }
  }

  @ConsoleCommand(description = "Remove the association in the dictionary")
  public void dictionaryRemove(@ConsoleParameter(name = "key", description = "The key to remove") final String iKey) {
    checkForDatabase();

    boolean result = currentDatabase.getDictionary().remove(iKey);
    if (!result)
      out.println("Entry not found in dictionary.");
    else
      out.println("Entry removed from the dictionary.");
  }

  @ConsoleCommand(description = "Copy a database to a remote server")
  public void copyDatabase(
      @ConsoleParameter(name = "db-name", description = "Name of the database to share") final String iDatabaseName,
      @ConsoleParameter(name = "db-user", description = "Database user") final String iDatabaseUserName,
      @ConsoleParameter(name = "db-password", description = "Database password") String iDatabaseUserPassword,
      @ConsoleParameter(name = "server-name", description = "Remote server's name as <address>:<port>") final String iRemoteName,
      @ConsoleParameter(name = "engine-name", description = "Remote server's engine to use between 'local' or 'memory'") final String iRemoteEngine)
      throws IOException {

    try {
      if (serverAdmin == null)
        throw new IllegalStateException("You must be connected to a remote server to share a database");

      out.println("Copying database '" + iDatabaseName + "' to the server '" + iRemoteName + "' via network streaming...");

      serverAdmin.copyDatabase(iDatabaseName, iDatabaseUserName, iDatabaseUserPassword, iRemoteName, iRemoteEngine);

      out.println("Database '" + iDatabaseName + "' has been copied to the server '" + iRemoteName + "'");

    } catch (Exception e) {
      printError(e);
    }
  }

  @ConsoleCommand(description = "Gets the replication journal for a database against a remote server")
  public void replicationGetJournal(
      @ConsoleParameter(name = "db-name", description = "Name of the database") final String iDatabaseName,
      @ConsoleParameter(name = "server-name", description = "Remote server's name as <address>:<port>") final String iRemoteName,
      @ConsoleParameter(name = "limit", description = "Limit as maximum number of records starting from the end", optional = true) final String iLimit)
      throws IOException {

    checkForRemoteServer();

    final long start = System.currentTimeMillis();

    int limit = iLimit == null ? -1 : Integer.parseInt(iLimit);

    try {
      final ODocument response = serverAdmin.getReplicationJournal(iDatabaseName, iRemoteName, limit);
      currentResultSet = response.field("result");
      if (currentResultSet.size() == 0)
        out.println("Replication journal for database '" + iDatabaseName + "' is empty");
      else {
        float elapsedSeconds = getElapsedSecs(start);

        new OTableFormatter(out).hideRID(true).setMaxWidthSize(Integer.parseInt(properties.get("width")))
            .writeRecords(currentResultSet, -1);

        out.println("\n" + currentResultSet.size() + " item(s) found. Query executed in " + elapsedSeconds + " sec(s).");
      }
      out.println();

    } catch (Exception e) {
      printError(e);
    }
  }

  @ConsoleCommand(description = "Resets the replication journal for a database against a remote server")
  public void replicationResetJournal(
      @ConsoleParameter(name = "db-name", description = "Name of the database") final String iDatabaseName,
      @ConsoleParameter(name = "server-name", description = "Remote server's name as <address>:<port>") final String iRemoteName)
      throws IOException {

    checkForRemoteServer();

    try {
      final ODocument response = serverAdmin.resetReplicationJournal(iDatabaseName, iRemoteName);
      out.println("Reset replication journal for database '" + iDatabaseName + "': removed " + response.field("removedEntries")
          + " entries");

    } catch (Exception e) {
      printError(e);
    }
  }

  @ConsoleCommand(description = "Gets the replication conflicts for a database against a remote server")
  public void replicationGetConflicts(@ConsoleParameter(name = "db-name", description = "Name of the database") String iDatabaseName)
      throws IOException {

    checkForRemoteServer();

    try {
      final ODocument response = serverAdmin.getReplicationConflicts(iDatabaseName);
      currentResultSet = response.field("result");

      if (currentResultSet == null || currentResultSet.size() == 0)
        out.println("There are not replication conflicts for database '" + iDatabaseName + "'");
      else {
        new OTableFormatter(out).hideRID(true).setMaxWidthSize(Integer.parseInt(properties.get("width")))
            .writeRecords(currentResultSet, -1);
      }
      out.println();

    } catch (Exception e) {
      printError(e);
    }
  }

  @ConsoleCommand(description = "Displays the status of the cluster nodes")
  public void clusterStatus() throws IOException {

    checkForRemoteServer();
    try {

      out.println("Cluster status:");
      out.println(serverAdmin.clusterStatus().toJSON("attribSameRow,alwaysFetchEmbedded,fetchPlan:*:0"));

    } catch (Exception e) {
      printError(e);
    }
  }

  @ConsoleCommand(description = "Align two databases in different servers")
  public void replicationAlign(
      @ConsoleParameter(name = "db-name", description = "Name of the database") final String iDatabaseName,
      @ConsoleParameter(name = "server-name", description = "Remote server's name as <address>:<port>") final String iRemoteName,
      @ConsoleParameter(name = "options", description = "Alignment options", optional = true) final String iOptions)
      throws IOException {

    try {
      if (serverAdmin == null)
        throw new IllegalStateException("You must be connected to a remote server to align database");

      serverAdmin.replicationAlign(iDatabaseName, iRemoteName, iOptions);

      out.println("Alignment started for database '" + iDatabaseName + "' against the server '" + iRemoteName + "'");

    } catch (Exception e) {
      printError(e);
    }
  }

  @ConsoleCommand(description = "Check database integrity")
  public void checkDatabase(@ConsoleParameter(name = "options", description = "Options: -v", optional = true) final String iOptions)
      throws IOException {
    checkForDatabase();

    if (!(currentDatabase.getStorage() instanceof OStorageLocalAbstract)) {
      out.println("Cannot check integrity of non-local database. Connect to it using local mode.");
      return;
    }

    boolean verbose = iOptions != null && iOptions.indexOf("-v") > -1;

    try {
      ((OStorageLocalAbstract) currentDatabase.getStorage()).check(verbose, this);
    } catch (ODatabaseImportException e) {
      printError(e);
    }
  }

  @ConsoleCommand(description = "Compare two databases")
  public void compareDatabases(
      @ConsoleParameter(name = "db1-url", description = "URL of the first database") final String iDb1URL,
      @ConsoleParameter(name = "db2-url", description = "URL of the second database") final String iDb2URL,
      @ConsoleParameter(name = "user-name", description = "User name", optional = true) final String iUserName,
      @ConsoleParameter(name = "user-password", description = "User password", optional = true) final String iUserPassword,
      @ConsoleParameter(name = "detect-mapping-data", description = "Whether RID mapping data after DB import should be tried to found on the disk.", optional = true) Boolean autoDiscoveringMappingData)
      throws IOException {
    try {
      final ODatabaseCompare compare;
      if (iUserName == null)
        compare = new ODatabaseCompare(iDb1URL, iDb2URL, this);
      else
        compare = new ODatabaseCompare(iDb1URL, iDb1URL, iUserName, iUserPassword, this);

      compare.setAutoDetectExportImportMap(autoDiscoveringMappingData != null ? autoDiscoveringMappingData : true);
      compare.compare();
    } catch (ODatabaseExportException e) {
      printError(e);
    }
  }

  @ConsoleCommand(description = "Import a database into the current one", splitInWords = false)
  public void importDatabase(@ConsoleParameter(name = "options", description = "Import options") final String text)
      throws IOException {
    checkForDatabase();

    out.println("Importing database " + text + "...");

    final List<String> items = OStringSerializerHelper.smartSplit(text, ' ');
    final String fileName = items.size() <= 0 || (items.get(1)).charAt(0) == '-' ? null : items.get(1);
    final String options = fileName != null ? text.substring((items.get(0)).length() + (items.get(1)).length() + 1).trim() : text;

    try {
      ODatabaseImport databaseImport = new ODatabaseImport(currentDatabase, fileName, this);

      databaseImport.setOptions(options).importDatabase();

      databaseImport.close();
    } catch (ODatabaseImportException e) {
      printError(e);
    }
  }

  @ConsoleCommand(description = "Export a database", splitInWords = false)
  public void exportDatabase(@ConsoleParameter(name = "options", description = "Export options") final String iText)
      throws IOException {
    checkForDatabase();

    out.println(new StringBuilder("Exporting current database to: ").append(iText).append("..."));
    final List<String> items = OStringSerializerHelper.smartSplit(iText, ' ');
    final String fileName = items.size() <= 0 || ((String) items.get(1)).charAt(0) == '-' ? null : (String) items.get(1);
    final String options = fileName != null ? iText.substring(
        ((String) items.get(0)).length() + ((String) items.get(1)).length() + 1).trim() : iText;

    try {
      new ODatabaseExport(currentDatabase, fileName, this).setOptions(options).exportDatabase().close();
    } catch (ODatabaseExportException e) {
      printError(e);
    }
  }

  @ConsoleCommand(description = "Export a database schema")
  public void exportSchema(@ConsoleParameter(name = "output-file", description = "Output file path") final String iOutputFilePath)
      throws IOException {
    checkForDatabase();

    out.println("Exporting current database to: " + iOutputFilePath + "...");

    try {
      ODatabaseExport exporter = new ODatabaseExport(currentDatabase, iOutputFilePath, this);
      exporter.setIncludeRecords(false);
      exporter.exportDatabase().close();
    } catch (ODatabaseExportException e) {
      printError(e);
    }
  }

  @ConsoleCommand(description = "Export the current record in the requested format")
  public void exportRecord(@ConsoleParameter(name = "format", description = "Format, such as 'json'") final String iFormat,
      @ConsoleParameter(name = "options", description = "Options", optional = true) final String iOptions) throws IOException {
    checkForDatabase();
    checkCurrentObject();

    final ORecordSerializer serializer = ORecordSerializerFactory.instance().getFormat(iFormat.toLowerCase());

    if (serializer == null) {
      out.println("ERROR: Format '" + iFormat + "' was not found.");
      printSupportedSerializerFormat();
      return;
    } else if (!(serializer instanceof ORecordSerializerStringAbstract)) {
      out.println("ERROR: Format '" + iFormat + "' does not export as text.");
      printSupportedSerializerFormat();
      return;
    }

    try {
      out.println(((ORecordSerializerStringAbstract) serializer).toString(currentRecord, iOptions));
    } catch (ODatabaseExportException e) {
      printError(e);
    }
  }

  @ConsoleCommand(description = "Return all configured properties")
  public void properties() {
    out.println("PROPERTIES:");
    out.println("+---------------------+----------------------+");
    out.printf("| %-30s| %-30s |\n", "NAME", "VALUE");
    out.println("+---------------------+----------------------+");
    for (Entry<String, String> p : properties.entrySet()) {
      out.printf("| %-30s= %-30s |\n", p.getKey(), p.getValue());
    }
    out.println("+---------------------+----------------------+");
  }

  @ConsoleCommand(description = "Return the value of a property")
  public void get(@ConsoleParameter(name = "property-name", description = "Name of the property") final String iPropertyName) {
    Object value = properties.get(iPropertyName);

    out.println();

    if (value == null)
      out.println("Property '" + iPropertyName + "' is not setted");
    else
      out.println(iPropertyName + " = " + value);
  }

  @ConsoleCommand(description = "Change the value of a property")
  public void set(@ConsoleParameter(name = "property-name", description = "Name of the property") final String iPropertyName,
      @ConsoleParameter(name = "property-value", description = "Value to set") final String iPropertyValue) {
    Object prevValue = properties.get(iPropertyName);

    out.println();

    if (iPropertyName.equalsIgnoreCase("limit") && (Integer.parseInt(iPropertyValue) == 0 || Integer.parseInt(iPropertyValue) < -1)) {
      out.println("ERROR: Limit must be > 0 or = -1 (no limit)");
    } else {

      if (prevValue != null)
        out.println("Previous value was: " + prevValue);

      properties.put(iPropertyName, iPropertyValue);

      out.println();
      out.println(iPropertyName + " = " + iPropertyValue);
    }
  }

  @ConsoleCommand(description = "Declare an intent")
  public void declareIntent(
      @ConsoleParameter(name = "Intent name", description = "name of the intent to execute") final String iIntentName) {
    checkForDatabase();

    out.println("Declaring intent '" + iIntentName + "'...");

    if (iIntentName.equalsIgnoreCase("massiveinsert"))
      currentDatabase.declareIntent(new OIntentMassiveInsert());
    else if (iIntentName.equalsIgnoreCase("massiveread"))
      currentDatabase.declareIntent(new OIntentMassiveRead());
    else
      throw new IllegalArgumentException("Intent '" + iIntentName
          + "' not supported. Available ones are: massiveinsert, massiveread");

    out.println("Intent '" + iIntentName + "' setted successfully");
  }

  @ConsoleCommand(description = "Execute a command against the profiler")
  public void profiler(
      @ConsoleParameter(name = "profiler command", description = "command to execute against the profiler") final String iCommandName) {
    if (iCommandName.equalsIgnoreCase("on")) {
      Orient.instance().getProfiler().startRecording();
      out.println("Profiler is ON now, use 'profiler off' to turn off.");
    } else if (iCommandName.equalsIgnoreCase("off")) {
      Orient.instance().getProfiler().stopRecording();
      out.println("Profiler is OFF now, use 'profiler on' to turn on.");
    } else if (iCommandName.equalsIgnoreCase("dump")) {
      out.println(Orient.instance().getProfiler().dump());
    }
  }

  @ConsoleCommand(description = "Return the value of a configuration value")
  public void configGet(@ConsoleParameter(name = "config-name", description = "Name of the configuration") final String iConfigName)
      throws IOException {
    final OGlobalConfiguration config = OGlobalConfiguration.findByKey(iConfigName);
    if (config == null)
      throw new IllegalArgumentException("Configuration variable '" + iConfigName + "' wasn't found");

    final String value;
    if (serverAdmin != null) {
      value = serverAdmin.getGlobalConfiguration(config);
      out.print("\nRemote configuration: ");
    } else {
      value = config.getValueAsString();
      out.print("\nLocal configuration: ");
    }
    out.println(iConfigName + " = " + value);
  }

  @ConsoleCommand(description = "Sleep X milliseconds")
  public void sleep(final String iTime) {
    try {
      Thread.sleep(Long.parseLong(iTime));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  @ConsoleCommand(description = "Change the value of a configuration value")
  public void configSet(
      @ConsoleParameter(name = "config-name", description = "Name of the configuration") final String iConfigName,
      @ConsoleParameter(name = "config-value", description = "Value to set") final String iConfigValue) throws IOException {
    final OGlobalConfiguration config = OGlobalConfiguration.findByKey(iConfigName);
    if (config == null)
      throw new IllegalArgumentException("Configuration variable '" + iConfigName + "' not found");

    if (serverAdmin != null) {
      serverAdmin.setGlobalConfiguration(config, iConfigValue);
      out.println("\nRemote configuration value changed correctly");
    } else {
      config.setValue(iConfigValue);
      out.println("\nLocal configuration value changed correctly");
    }
    out.println();
  }

  @ConsoleCommand(description = "Return all the configuration values")
  public void config() throws IOException {
    if (serverAdmin != null) {
      // REMOTE STORAGE
      final Map<String, String> values = serverAdmin.getGlobalConfigurations();

      out.println("REMOTE SERVER CONFIGURATION:");
      out.println("+------------------------------------+--------------------------------+");
      out.printf("| %-35s| %-30s |\n", "NAME", "VALUE");
      out.println("+------------------------------------+--------------------------------+");
      for (Entry<String, String> p : values.entrySet()) {
        out.printf("| %-35s= %-30s |\n", p.getKey(), p.getValue());
      }
    } else {
      // LOCAL STORAGE
      out.println("LOCAL SERVER CONFIGURATION:");
      out.println("+------------------------------------+--------------------------------+");
      out.printf("| %-35s| %-30s |\n", "NAME", "VALUE");
      out.println("+------------------------------------+--------------------------------+");
      for (OGlobalConfiguration cfg : OGlobalConfiguration.values()) {
        out.printf("| %-35s= %-30s |\n", cfg.getKey(), cfg.getValue());
      }
    }

    out.println("+------------------------------------+--------------------------------+");
  }

  /** Should be used only by console commands */
  public ODatabaseDocument getCurrentDatabase() {
    return currentDatabase;
  }

  /** Should be used only by console commands */
  public String getCurrentDatabaseName() {
    return currentDatabaseName;
  }

  /** Should be used only by console commands */
  public String getCurrentDatabaseUserName() {
    return currentDatabaseUserName;
  }

  /** Should be used only by console commands */
  public String getCurrentDatabaseUserPassword() {
    return currentDatabaseUserPassword;
  }

  /** Should be used only by console commands */
  public ORecordInternal<?> getCurrentRecord() {
    return currentRecord;
  }

  /** Should be used only by console commands */
  public List<OIdentifiable> getCurrentResultSet() {
    return currentResultSet;
  }

  /** Should be used only by console commands */
  public void loadRecordInternal(String iRecordId, String iFetchPlan) {
    checkForDatabase();

    currentRecord = currentDatabase.load(new ORecordId(iRecordId), iFetchPlan);
    displayRecord(null);

    out.println("OK");
  }

  /** Should be used only by console commands */
  public void reloadRecordInternal(String iRecordId, String iFetchPlan) {
    checkForDatabase();

    currentRecord = ((ODatabaseRecordAbstract) currentDatabase.getUnderlying()).executeReadRecord(new ORecordId(iRecordId), null,
        iFetchPlan, true, false);
    displayRecord(null);

    out.println("OK");
  }

  /** Should be used only by console commands */
  public void checkForRemoteServer() {
    if (serverAdmin == null
        && (currentDatabase == null || !(currentDatabase.getStorage() instanceof OStorageRemoteThread) || currentDatabase
            .isClosed()))
      throw new OException("Remote server is not connected. Use 'connect remote:<host>[:<port>][/<database-name>]' to connect");
  }

  /** Should be used only by console commands */
  public void checkForDatabase() {
    if (currentDatabase == null)
      throw new OException("Database not selected. Use 'connect <database-name>' to connect to a database.");
    if (currentDatabase.isClosed())
      throw new ODatabaseException("Database '" + currentDatabaseName + "' is closed");
  }

  /** Should be used only by console commands */
  public void checkCurrentObject() {
    if (currentRecord == null)
      throw new OException("The is no current object selected: create a new one or load it");
  }

  private void dumpRecordDetails() {
    if (currentRecord instanceof ODocument) {
      ODocument rec = (ODocument) currentRecord;
      out.println("--------------------------------------------------");
      out.printf("ODocument - Class: %s   id: %s   v.%s\n", rec.getClassName(), rec.getIdentity().toString(), rec
          .getRecordVersion().toString());
      out.println("--------------------------------------------------");
      Object value;
      for (String fieldName : rec.fieldNames()) {
        value = rec.field(fieldName);
        if (value instanceof byte[])
          value = "byte[" + ((byte[]) value).length + "]";
        out.printf("%20s : %-20s\n", fieldName, value);
      }

    } else if (currentRecord instanceof ORecordFlat) {
      ORecordFlat rec = (ORecordFlat) currentRecord;
      out.println("--------------------------------------------------");
      out.printf("Flat - record id: %s   v.%s\n", rec.getIdentity().toString(), rec.getRecordVersion().toString());
      out.println("--------------------------------------------------");
      out.print(rec.value());

    } else if (currentRecord instanceof ORecordBytes) {
      ORecordBytes rec = (ORecordBytes) currentRecord;
      out.println("--------------------------------------------------");
      out.printf("Bytes - record id: %s   v.%s\n", rec.getIdentity().toString(), rec.getRecordVersion().toString());
      out.println("--------------------------------------------------");

      final byte[] value = rec.toStream();
      final int max = Math.min(Integer.parseInt(properties.get("maxBinaryDisplay")), Array.getLength(value));
      for (int i = 0; i < max; ++i) {
        out.printf("%03d", Array.getByte(value, i));
      }

    } else {
      out.println("--------------------------------------------------");
      out.printf("%s - record id: %s   v.%s\n", currentRecord.getClass().getSimpleName(), currentRecord.getIdentity().toString(),
          currentRecord.getRecordVersion().toString());
    }
    out.println();
  }

  public String ask(final String iText) {
    out.print(iText);
    final Scanner scanner = new Scanner(in);
    final String answer = scanner.nextLine();
    scanner.close();
    return answer;
  }

  public void onMessage(final String iText) {
    out.print(iText);
  }

  private void printSupportedSerializerFormat() {
    out.println("Supported formats are:");

    for (ORecordSerializer s : ORecordSerializerFactory.instance().getFormats()) {
      if (s instanceof ORecordSerializerStringAbstract)
        out.println("- " + s.toString());
    }
  }

  private void browseRecords(final int limit, final OIdentifiableIterator<?> it) {
    final OTableFormatter tableFormatter = new OTableFormatter(out).setMaxWidthSize(Integer.parseInt(properties.get("width")));

    currentResultSet.clear();
    while (it.hasNext() && currentResultSet.size() <= limit)
      currentResultSet.add(it.next());

    tableFormatter.writeRecords(currentResultSet, limit);
  }

  private Object sqlCommand(final String iExpectedCommand, String iReceivedCommand, final String iMessage,
      final boolean iIncludeResult) {
    checkForDatabase();

    if (iReceivedCommand == null)
      return null;

    iReceivedCommand = iExpectedCommand + " " + iReceivedCommand.trim();

    currentResultSet.clear();

    final long start = System.currentTimeMillis();

    final Object result = new OCommandSQL(iReceivedCommand).setProgressListener(this).execute();

    float elapsedSeconds = getElapsedSecs(start);

    if (iIncludeResult)
      out.printf(iMessage, result, elapsedSeconds);
    else
      out.printf(iMessage, elapsedSeconds);

    return result;
  }

  public void onBegin(final Object iTask, final long iTotal) {
    lastPercentStep = 0;

    out.print("[");
    if (interactiveMode) {
      for (int i = 0; i < 10; ++i)
        out.print(' ');
      out.print("]   0%");
    }
  }

  public boolean onProgress(final Object iTask, final long iCounter, final float iPercent) {
    final int completitionBar = (int) iPercent / 10;

    if (((int) (iPercent * 10)) == lastPercentStep)
      return true;

    if (interactiveMode) {
      out.print("\r[");
      for (int i = 0; i < completitionBar; ++i)
        out.print('=');
      for (int i = completitionBar; i < 10; ++i)
        out.print(' ');
      out.printf("] %3.1f%% ", iPercent);
    } else {
      for (int i = lastPercentStep / 100; i < completitionBar; ++i)
        out.print('=');
    }

    lastPercentStep = (int) (iPercent * 10);
    return true;
  }

  @ConsoleCommand(description = "Display the current path")
  public void pwd() {
    out.println("Current path: " + new File("").getAbsolutePath());
  }

  public void onCompletition(final Object iTask, final boolean iSucceed) {
    if (interactiveMode)
      if (iSucceed)
        out.print("\r[==========] 100% Done.");
      else
        out.print(" Error!");
    else
      out.print(iSucceed ? "] Done." : " Error!");
  }

  protected void printApplicationInfo() {
    out.println("OrientDB console v." + OConstants.getVersion() + " " + OConstants.ORIENT_URL);
    out.println("Type 'help' to display all the commands supported.");
  }

  protected static boolean setTerminalToCBreak() throws IOException, InterruptedException {
    // set the console to be character-buffered instead of line-buffered
    int result = stty("-icanon min 1");
    if (result != 0) {
      return false;
    }

    // disable character echoing
    stty("-echo");
    return true;
  }

  protected void dumpResultSet(final int limit) {
    new OTableFormatter(out).setMaxWidthSize(Integer.parseInt(properties.get("width"))).writeRecords(currentResultSet, limit);
  }

  /**
   * Execute the stty command with the specified arguments against the current active terminal.
   */
  protected static int stty(final String args) throws IOException, InterruptedException {
    String cmd = "stty " + args + " < /dev/tty";

    return exec(new String[] { "sh", "-c", cmd });
  }

  protected float getElapsedSecs(final long start) {
    return (float) (System.currentTimeMillis() - start) / 1000;
  }

  /**
   * Execute the specified command and return the output (both stdout and stderr).
   */
  protected static int exec(final String[] cmd) throws IOException, InterruptedException {
    ByteArrayOutputStream bout = new ByteArrayOutputStream();

    Process p = Runtime.getRuntime().exec(cmd);
    int c;
    InputStream in = p.getInputStream();

    while ((c = in.read()) != -1) {
      bout.write(c);
    }

    in = p.getErrorStream();

    while ((c = in.read()) != -1) {
      bout.write(c);
    }

    p.waitFor();

    return p.exitValue();
  }

  protected void printError(final Exception e) {
    if (properties.get("debug") != null && Boolean.parseBoolean(properties.get("debug").toString())) {
      out.println("\n!ERROR:");
      e.printStackTrace();
    } else {
      // SHORT FORM
      out.println("\n!ERROR: " + e.getMessage());

      if (e.getCause() != null) {
        Throwable t = e.getCause();
        while (t != null) {
          out.println("-> " + t.getMessage());
          t = t.getCause();
        }
      }
    }
  }

  protected void updateDatabaseInfo() {
    currentDatabase.getStorage().reload();
    currentDatabase.getMetadata().getSchema().reload();
    currentDatabase.getMetadata().getIndexManager().reload();
  }
}
