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
package com.orientechnologies.orient.console;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
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
import com.orientechnologies.common.io.OIOException;
import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.client.remote.OEngineRemote;
import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.client.remote.OStorageRemoteThread;
import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.OSignalHandler;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.command.script.OCommandExecutorScript;
import com.orientechnologies.orient.core.command.script.OCommandScript;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.config.OStorageEntryConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordLazyMultiValue;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.db.tool.ODatabaseCompare;
import com.orientechnologies.orient.core.db.tool.ODatabaseExport;
import com.orientechnologies.orient.core.db.tool.ODatabaseExportException;
import com.orientechnologies.orient.core.db.tool.ODatabaseImport;
import com.orientechnologies.orient.core.db.tool.ODatabaseImportException;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.id.ORID;
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
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;
import com.orientechnologies.orient.core.record.impl.ORecordFlat;
import com.orientechnologies.orient.core.serialization.OBase64Utils;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializerFactory;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializationDebug;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializationDebugProperty;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerBinaryDebug;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerStringAbstract;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.filter.OSQLPredicate;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OClusterPageDebug;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OLocalPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OPaginatedCluster;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OPaginatedClusterDebug;

import sun.misc.Signal;
import sun.misc.SignalHandler;

public class OConsoleDatabaseApp extends OrientConsole implements OCommandOutputListener, OProgressListener {
  protected static final int    DEFAULT_WIDTH      = 150;
  protected ODatabaseDocumentTx currentDatabase;
  protected String              currentDatabaseName;
  protected ORecord             currentRecord;
  protected int                 currentRecordIdx;
  protected List<OIdentifiable> currentResultSet;
  protected Object              currentResult;
  protected OServerAdmin        serverAdmin;
  private int                   windowSize         = DEFAULT_WIDTH;
  private int                   lastPercentStep;
  private String                currentDatabaseUserName;
  private String                currentDatabaseUserPassword;
  private int                   collectionMaxItems = 10;

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
            restoreTerminal();
          }
        });

      } catch (Exception ignored) {
      }

      new OSignalHandler().installDefaultSignals(new SignalHandler() {
        public void handle(Signal signal) {
          restoreTerminal();
        }
      });

      final OConsoleDatabaseApp console = new OConsoleDatabaseApp(args);
      if (tty)
        console.setReader(new TTYConsoleReader());

      result = console.run();

    } finally {
      restoreTerminal();
    }

    Orient.instance().shutdown();
    System.exit(result);
  }

  protected static void restoreTerminal() {
    try {
      stty("echo");
    } catch (Exception ignored) {
    }
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

  /**
   * Execute the stty command with the specified arguments against the current active terminal.
   */
  protected static int stty(final String args) throws IOException, InterruptedException {
    String cmd = "stty " + args + " < /dev/tty";

    return exec(new String[] { "sh", "-c", cmd });
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

  @ConsoleCommand(aliases = { "use database" }, description = "Connect to a database or a remote Server instance")
  public void connect(
      @ConsoleParameter(name = "url", description = "The url of the remote server or the database to connect to in the format '<mode>:<path>'") String iURL,
      @ConsoleParameter(name = "user", description = "User name") String iUserName,
      @ConsoleParameter(name = "password", description = "User password", optional = true) String iUserPassword)
          throws IOException {
    disconnect();

    if (iUserPassword == null) {
      message("Enter password: ");
      final BufferedReader br = new BufferedReader(new InputStreamReader(this.in));
      iUserPassword = br.readLine();
      message("\n");
    }

    currentDatabaseUserName = iUserName;
    currentDatabaseUserPassword = iUserPassword;

    if (iURL.contains("/")) {
      // OPEN DB
      message("\nConnecting to database [" + iURL + "] with user '" + iUserName + "'...");

      currentDatabase = new ODatabaseDocumentTx(iURL);

      currentDatabase.registerListener(new OConsoleDatabaseListener(this));
      currentDatabase.open(iUserName, iUserPassword);
      currentDatabaseName = currentDatabase.getName();
    } else {
      // CONNECT TO REMOTE SERVER
      message("\nConnecting to remote Server instance [" + iURL + "] with user '" + iUserName + "'...");

      serverAdmin = new OServerAdmin(iURL).connect(iUserName, iUserPassword);
      currentDatabase = null;
      currentDatabaseName = null;
    }

    message("OK");

    dumpDistributedConfiguration(false);
  }

  @ConsoleCommand(aliases = { "close database" }, description = "Disconnect from the current database")
  public void disconnect() {
    if (serverAdmin != null) {
      message("\nDisconnecting from remote server [" + serverAdmin.getURL() + "]...");
      serverAdmin.close(true);
      serverAdmin = null;
      message("\nOK");
    }

    if (currentDatabase != null) {
      message("\nDisconnecting from the database [" + currentDatabaseName + "]...");

      final OStorage stg = Orient.instance().getStorage(currentDatabase.getURL());

      currentDatabase.activateOnCurrentThread();
      if (!currentDatabase.isClosed())
        currentDatabase.close();

      // FORCE CLOSING OF STORAGE: THIS CLEAN UP REMOTE CONNECTIONS
      if (stg != null)
        stg.close(true, false);

      currentDatabase = null;
      currentDatabaseName = null;
      currentRecord = null;

      message("OK");
    }
  }

  @ConsoleCommand(description = "Create a new database")
  public void createDatabase(
      @ConsoleParameter(name = "database-url", description = "The url of the database to create in the format '<mode>:<path>'") String iDatabaseURL,
      @ConsoleParameter(name = "user", optional = true, description = "Server administrator name") String iUserName,
      @ConsoleParameter(name = "password", optional = true, description = "Server administrator password") String iUserPassword,
      @ConsoleParameter(name = "storage-type", optional = true, description = "The type of the storage. 'local' and 'plocal' for disk-based databases and 'memory' for in-memory database") String iStorageType,
      @ConsoleParameter(name = "db-type", optional = true, description = "The type of the database used between 'document' and 'graph'. By default is graph.") String iDatabaseType)
          throws IOException {

    if (iUserName == null)
      iUserName = OUser.ADMIN;
    if (iUserPassword == null)
      iUserPassword = OUser.ADMIN;
    if (iStorageType == null) {
      if (iDatabaseURL.startsWith(OEngineRemote.NAME + ":"))
        throw new IllegalArgumentException("Missing storage type for remote database");

      int pos = iDatabaseURL.indexOf(":");
      if (pos == -1)
        throw new IllegalArgumentException("Invalid URL");
      iStorageType = iDatabaseURL.substring(0, pos);
    }
    if (iDatabaseType == null)
      iDatabaseType = "graph";

    message("\nCreating database [" + iDatabaseURL + "] using the storage type [" + iStorageType + "]...");

    currentDatabaseUserName = iUserName;
    currentDatabaseUserPassword = iUserPassword;

    if (iDatabaseURL.startsWith(OEngineRemote.NAME)) {
      // REMOTE CONNECTION
      final String dbURL = iDatabaseURL.substring(OEngineRemote.NAME.length() + 1);
      new OServerAdmin(dbURL).connect(iUserName, iUserPassword).createDatabase(iDatabaseType, iStorageType).close();
      connect(iDatabaseURL, OUser.ADMIN, OUser.ADMIN);

    } else {
      // LOCAL CONNECTION
      if (iStorageType != null) {
        // CHECK STORAGE TYPE
        if (!iDatabaseURL.toLowerCase().startsWith(iStorageType.toLowerCase()))
          throw new IllegalArgumentException("Storage type '" + iStorageType + "' is different by storage type in URL");
      }
      currentDatabase = Orient.instance().getDatabaseFactory().createDatabase(iDatabaseType, iDatabaseURL);
      currentDatabase.create();
      currentDatabaseName = currentDatabase.getName();
    }

    message("\nDatabase created successfully.");
    message("\n\nCurrent database is: " + iDatabaseURL);
  }

  @ConsoleCommand(description = "List all the databases available on the connected server")
  public void listDatabases() throws IOException {
    if (serverAdmin != null) {
      final Map<String, String> databases = serverAdmin.listDatabases();
      message("\nFound %d databases:\n", databases.size());
      for (Entry<String, String> database : databases.entrySet()) {
        message("\n* %s (%s)", database.getKey(), database.getValue().substring(0, database.getValue().indexOf(":")));
      }
    } else {
      message(
          "\nNot connected to the Server instance. You've to connect to the Server using server's credentials (look at orientdb-*server-config.xml file)");
    }
    out.println();
  }

  @ConsoleCommand(description = "Reload the database schema")
  public void reloadSchema() throws IOException {
    message("\nreloading database schema...");
    updateDatabaseInfo();
    message("\n\nDone.");
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

    message("\nDropping cluster [" + iClusterName + "] in database " + currentDatabaseName + "...");

    boolean result = currentDatabase.dropCluster(iClusterName, true);

    if (!result) {
      // TRY TO GET AS CLUSTER ID
      try {
        int clusterId = Integer.parseInt(iClusterName);
        if (clusterId > -1) {
          result = currentDatabase.dropCluster(clusterId, true);
        }
      } catch (Exception ignored) {
      }
    }

    if (result)
      message("\nCluster correctly removed");
    else
      message("\nCannot find the cluster to remove");
    updateDatabaseInfo();
  }

  @ConsoleCommand(splitInWords = false, description = "Alters a cluster in the current database. The cluster can be physical or memory")
  public void alterCluster(
      @ConsoleParameter(name = "command-text", description = "The command text to execute") String iCommandText) {
    sqlCommand("alter", iCommandText, "\nCluster updated successfully\n", false);
    updateDatabaseInfo();
  }

  @ConsoleCommand(description = "Begins a transaction. All the changes will remain local")
  public void begin() throws IOException {
    checkForDatabase();

    if (currentDatabase.getTransaction().isActive()) {
      message("\nError: an active transaction is currently open (id=" + currentDatabase.getTransaction().getId()
          + "). Commit or rollback before starting a new one.");
      return;
    }

    currentDatabase.begin();
    message("\nTransaction " + currentDatabase.getTransaction().getId() + " is running");
  }

  @ConsoleCommand(description = "Commits transaction changes to the database")
  public void commit() throws IOException {
    checkForDatabase();

    if (!currentDatabase.getTransaction().isActive()) {
      message("\nError: no active transaction is currently open.");
      return;
    }

    final long begin = System.currentTimeMillis();

    final int txId = currentDatabase.getTransaction().getId();
    currentDatabase.commit();

    message("\nTransaction " + txId + " has been committed in " + (System.currentTimeMillis() - begin) + "ms");
  }

  @ConsoleCommand(description = "Rolls back transaction changes to the previous state")
  public void rollback() throws IOException {
    checkForDatabase();

    if (!currentDatabase.getTransaction().isActive()) {
      message("\nError: no active transaction is running right now.");
      return;
    }

    final long begin = System.currentTimeMillis();

    final int txId = currentDatabase.getTransaction().getId();
    currentDatabase.rollback();
    message("\nTransaction " + txId + " has been rollbacked in " + (System.currentTimeMillis() - begin) + "ms");
  }

  @ConsoleCommand(splitInWords = false, description = "Truncate the class content in the current database")
  public void truncateClass(
      @ConsoleParameter(name = "text", description = "The name of the class to truncate") String iCommandText) {
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
      message(((ODocument) result).toJSON());
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
  public void createVertex(
      @ConsoleParameter(name = "command-text", description = "The command text to execute") String iCommandText) {
    sqlCommand("create", iCommandText, "\nCreated vertex '%s' in %f sec(s).\n", true);
  }

  @ConsoleCommand(splitInWords = false, description = "Create a new edge into the database")
  public void createEdge(
      @ConsoleParameter(name = "command-text", description = "The command text to execute") String iCommandText) {
    sqlCommand("create", iCommandText, "\nCreated edge '%s' in %f sec(s).\n", true);
  }

  @ConsoleCommand(splitInWords = false, description = "Update records in the database")
  public void update(@ConsoleParameter(name = "command-text", description = "The command text to execute") String iCommandText) {
    sqlCommand("update", iCommandText, "\nUpdated record(s) '%s' in %f sec(s).\n", true);
    updateDatabaseInfo();
    currentDatabase.getLocalCache().invalidate();
  }

  @ConsoleCommand(splitInWords = false, description = "Move vertices to another position (class/cluster)", priority = 8)
  // EVALUATE THIS BEFORE 'MOVE'
  public void moveVertex(
      @ConsoleParameter(name = "command-text", description = "The command text to execute") String iCommandText) {
    sqlCommand("move", iCommandText, "\nMove vertex command executed with result '%s' in %f sec(s).\n", true);
  }

  @ConsoleCommand(description = "Force calling of JVM Garbage Collection")
  public void gc() {
    System.gc();
  }

  @ConsoleCommand(splitInWords = false, description = "Delete records from the database")
  public void delete(@ConsoleParameter(name = "command-text", description = "The command text to execute") String iCommandText) {
    sqlCommand("delete", iCommandText, "\nDelete record(s) '%s' in %f sec(s).\n", true);
    updateDatabaseInfo();
    currentDatabase.getLocalCache().invalidate();
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
  public void createLink(
      @ConsoleParameter(name = "command-text", description = "The command text to execute") String iCommandText) {
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
      if (storageType == null)
        storageType = "plocal";

      new OServerAdmin(currentDatabase.getURL()).connect(currentDatabaseUserName, currentDatabaseUserPassword)
          .freezeDatabase(storageType);
    } else {
      // LOCAL CONNECTION
      currentDatabase.freeze();
    }

    message("\n\nDatabase '" + dbName + "' was frozen successfully");
  }

  @ConsoleCommand(description = "Release database after freeze")
  public void releaseDatabase(
      @ConsoleParameter(name = "storage-type", description = "Storage type of server database", optional = true) String storageType)
          throws IOException {
    checkForDatabase();

    final String dbName = currentDatabase.getName();

    if (currentDatabase.getURL().startsWith(OEngineRemote.NAME)) {
      if (storageType == null)
        storageType = "plocal";

      new OServerAdmin(currentDatabase.getURL()).connect(currentDatabaseUserName, currentDatabaseUserPassword)
          .releaseDatabase(storageType);
    } else {
      // LOCAL CONNECTION
      currentDatabase.release();
    }

    message("\n\nDatabase '" + dbName + "' was released successfully");
  }

  @ConsoleCommand(description = "Flushes all database content to the disk")
  public void flushDatabase(
      @ConsoleParameter(name = "storage-type", description = "Storage type of server database", optional = true) String storageType)
          throws IOException {
    freezeDatabase(storageType);
    releaseDatabase(storageType);
  }

  @ConsoleCommand(description = "Freeze clusters and flush on the disk")
  public void freezeCluster(
      @ConsoleParameter(name = "cluster-name", description = "The name of the cluster to freeze") String iClusterName,
      @ConsoleParameter(name = "storage-type", description = "Storage type of server database", optional = true) String storageType)
          throws IOException {
    checkForDatabase();

    final int clusterId = currentDatabase.getClusterIdByName(iClusterName);

    if (currentDatabase.getURL().startsWith(OEngineRemote.NAME)) {
      if (storageType == null)
        storageType = "plocal";

      new OServerAdmin(currentDatabase.getURL()).connect(currentDatabaseUserName, currentDatabaseUserPassword)
          .freezeCluster(clusterId, storageType);
    } else {
      // LOCAL CONNECTION
      currentDatabase.freezeCluster(clusterId);
    }

    message("\n\nCluster '" + iClusterName + "' was frozen successfully");
  }

  @ConsoleCommand(description = "Release cluster after freeze")
  public void releaseCluster(
      @ConsoleParameter(name = "cluster-name", description = "The name of the cluster to unfreeze") String iClusterName,
      @ConsoleParameter(name = "storage-type", description = "Storage type of server database", optional = true) String storageType)
          throws IOException {
    checkForDatabase();

    final int clusterId = currentDatabase.getClusterIdByName(iClusterName);

    if (currentDatabase.getURL().startsWith(OEngineRemote.NAME)) {
      if (storageType == null)
        storageType = "plocal";

      new OServerAdmin(currentDatabase.getURL()).connect(currentDatabaseUserName, currentDatabaseUserPassword)
          .releaseCluster(clusterId, storageType);
    } else {
      // LOCAL CONNECTION
      currentDatabase.releaseCluster(clusterId);
    }

    message("\n\nCluster '" + iClusterName + "' was released successfully");
  }

  @ConsoleCommand(description = "Display current record")
  public void current() {
    dumpRecordDetails();
  }

  @ConsoleCommand(description = "Move the current record cursor to the next one in result set")
  public void next() {
    setCurrentRecord(currentRecordIdx + 1);
    dumpRecordDetails();
  }

  @ConsoleCommand(description = "Move the current record cursor to the previous one in result set")
  public void prev() {
    setCurrentRecord(currentRecordIdx - 1);
    dumpRecordDetails();
  }

  @ConsoleCommand(splitInWords = false, description = "Alter a class in the database schema")
  public void alterClass(
      @ConsoleParameter(name = "command-text", description = "The command text to execute") String iCommandText) {
    sqlCommand("alter", iCommandText, "\nClass updated successfully\n", false);
    updateDatabaseInfo();
  }

  @ConsoleCommand(splitInWords = false, description = "Create a class")
  public void createClass(
      @ConsoleParameter(name = "command-text", description = "The command text to execute") String iCommandText) {
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
   * Creates a function.
   *
   * @author Claudio Tesoriero
   * @param iCommandText
   *          the command text to execute
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
      limit = Integer.parseInt(properties.get("limit"));
    }

    long start = System.currentTimeMillis();
    setResultset((List<OIdentifiable>) currentDatabase.command(new OCommandSQL("traverse " + iQueryText)).execute());

    float elapsedSeconds = getElapsedSecs(start);

    dumpResultSet(limit);

    message("\n\n" + currentResultSet.size() + " item(s) found. Traverse executed in " + elapsedSeconds + " sec(s).");
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
      limit = Integer.parseInt(properties.get("limit"));
    }

    final long start = System.currentTimeMillis();
    setResultset((List<OIdentifiable>) currentDatabase.query(new OSQLSynchQuery<ODocument>(iQueryText, limit).setFetchPlan("*:0")));

    float elapsedSeconds = getElapsedSecs(start);

    dumpResultSet(limit);

    message("\n\n" + currentResultSet.size() + " item(s) found. Query executed in " + elapsedSeconds + " sec(s).");
  }

  @ConsoleCommand(splitInWords = false, description = "Move from current record by evaluating a predicate against current record")
  public void move(@ConsoleParameter(name = "text", description = "The sql predicate to evaluate") final String iText) {
    if (iText == null)
      return;

    if (currentRecord == null)
      return;

    final Object result = new OSQLPredicate(iText).evaluate(currentRecord, null, null);

    if (result != null) {
      if (result instanceof OIdentifiable) {
        setResultset(new ArrayList<OIdentifiable>());
        currentRecord = ((OIdentifiable) result).getRecord();
        dumpRecordDetails();
      } else if (result instanceof List<?>) {
        setResultset((List<OIdentifiable>) result);
        dumpResultSet(-1);
      } else if (result instanceof Iterator<?>) {
        final List<OIdentifiable> list = new ArrayList<OIdentifiable>();
        while (((Iterator) result).hasNext())
          list.add(((Iterator<OIdentifiable>) result).next());
        setResultset(list);
        dumpResultSet(-1);
      } else
        setResultset(new ArrayList<OIdentifiable>());
    }
  }

  @ConsoleCommand(splitInWords = false, description = "Evaluate a predicate against current record")
  public void eval(@ConsoleParameter(name = "text", description = "The sql predicate to evaluate") final String iText) {
    if (iText == null)
      return;

    if (currentRecord == null)
      return;

    final Object result = new OSQLPredicate(iText).evaluate(currentRecord, null, null);
    if (result != null)
      out.println("\n" + result);
  }

  @SuppressWarnings("unchecked")
  @ConsoleCommand(splitInWords = false, description = "Execute a script containing multiple commands separated by ; or new line")
  public void script(@ConsoleParameter(name = "text", description = "Commands to execute, one per line") String iText) {
    final String language;
    final int languageEndPos = iText.indexOf(";");
    if (languageEndPos > -1) {
      // EXTRACT THE SCRIPT LANGUAGE
      language = iText.substring(0, languageEndPos);
      iText = iText.substring(languageEndPos + 1);
    } else
      throw new IllegalArgumentException("Missing language in script (sql, js, gremlin, etc.) as first argument");

    executeServerSideScript(language, iText);
  }

  @SuppressWarnings("unchecked")
  @ConsoleCommand(splitInWords = false, description = "Execute javascript commands in the console")
  public void js(
      @ConsoleParameter(name = "text", description = "The javascript to execute. Use 'db' to reference to a document database, 'gdb' for a graph database") final String iText) {
    if (iText == null)
      return;

    resetResultSet();

    final OCommandExecutorScript cmd = new OCommandExecutorScript();
    cmd.parse(new OCommandScript("Javascript", iText));

    long start = System.currentTimeMillis();
    currentResult = cmd.execute(null);
    float elapsedSeconds = getElapsedSecs(start);

    parseResult();

    if (currentResultSet != null) {
      dumpResultSet(-1);
      message("\nClient side script executed in %f sec(s). Returned %d records", elapsedSeconds, currentResultSet.size());
    } else
      message("\nClient side script executed in %f sec(s). Value returned is: %s", elapsedSeconds, currentResult);
  }

  @SuppressWarnings("unchecked")
  @ConsoleCommand(splitInWords = false, description = "Execute javascript commands against a remote server")
  public void jss(
      @ConsoleParameter(name = "text", description = "The javascript to execute. Use 'db' to reference to a document database, 'gdb' for a graph database") final String iText) {
    checkForRemoteServer();

    executeServerSideScript("javascript", iText);

  }

  @ConsoleCommand(splitInWords = false, description = "Create an index against a property")
  public void createIndex(@ConsoleParameter(name = "command-text", description = "The command text to execute") String iCommandText)
      throws IOException {
    message("\n\nCreating index...");

    sqlCommand("create", iCommandText, "\nCreated index successfully with %d entries in %f sec(s).\n", true);
    updateDatabaseInfo();
    message("\n\nIndex created successfully");
  }

  @ConsoleCommand(description = "Delete the current database")
  public void dropDatabase(
      @ConsoleParameter(name = "storage-type", description = "Storage type of server database", optional = true) String storageType)
          throws IOException {
    checkForDatabase();

    final String dbName = currentDatabase.getName();

    if (currentDatabase.getURL().startsWith(OEngineRemote.NAME)) {
      if (serverAdmin == null) {
        message("\n\nCannot drop a remote database without connecting to the server with a valid server's user");
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

    message("\n\nDatabase '" + dbName + "' deleted successfully");
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

      if (serverAdmin != null)
        serverAdmin.close();

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
        message("\n\nCannot drop database '" + iDatabaseURL + "' because was not found");
    }

    currentDatabase = null;
    currentDatabaseName = null;

    message("\n\nDatabase '" + iDatabaseURL + "' deleted successfully");
  }

  @ConsoleCommand(splitInWords = false, description = "Remove an index")
  public void dropIndex(@ConsoleParameter(name = "command-text", description = "The command text to execute") String iCommandText)
      throws IOException {
    message("\n\nRemoving index...");

    sqlCommand("drop", iCommandText, "\nDropped index in %f sec(s).\n", false);
    updateDatabaseInfo();
    message("\n\nIndex removed successfully");
  }

  @ConsoleCommand(splitInWords = false, description = "Rebuild an index if it is automatic")
  public void rebuildIndex(
      @ConsoleParameter(name = "command-text", description = "The command text to execute") String iCommandText)
          throws IOException {
    message("\n\nRebuilding index(es)...");

    sqlCommand("rebuild", iCommandText, "\nRebuilt index(es). Found %d link(s) in %f sec(s).\n", true);
    updateDatabaseInfo();
    message("\n\nIndex(es) rebuilt successfully");
  }

  @ConsoleCommand(splitInWords = false, description = "Remove a class from the schema")
  public void dropClass(@ConsoleParameter(name = "command-text", description = "The command text to execute") String iCommandText)
      throws IOException {
    sqlCommand("drop", iCommandText, "\nRemoved class in %f sec(s).\n", false);
    updateDatabaseInfo();
  }

  @ConsoleCommand(splitInWords = false, description = "Remove a property from a class")
  public void dropProperty(
      @ConsoleParameter(name = "command-text", description = "The command text to execute") String iCommandText)
          throws IOException {
    sqlCommand("drop", iCommandText, "\nRemoved class property in %f sec(s).\n", false);
    updateDatabaseInfo();
  }

  @ConsoleCommand(description = "Browse all records of a class")
  public void browseClass(@ConsoleParameter(name = "class-name", description = "The name of the class") final String iClassName) {
    checkForDatabase();

    resetResultSet();

    final int limit = Integer.parseInt(properties.get("limit"));

    OIdentifiableIterator<?> it = currentDatabase.browseClass(iClassName);

    browseRecords(limit, it);
  }

  @ConsoleCommand(description = "Browse all records of a cluster")
  public void browseCluster(
      @ConsoleParameter(name = "cluster-name", description = "The name of the cluster") final String iClusterName) {
    checkForDatabase();

    resetResultSet();

    final int limit = Integer.parseInt(properties.get("limit"));

    final ORecordIteratorCluster<?> it = currentDatabase.browseCluster(iClusterName);

    browseRecords(limit, it);
  }

  @ConsoleCommand(aliases = { "display" }, description = "Display current record attributes")
  public void displayRecord(
      @ConsoleParameter(name = "number", description = "The number of the record in the most recent result set") final String iRecordNumber) {
    checkForDatabase();

    if (iRecordNumber == null || currentResultSet == null)
      checkCurrentObject();
    else {
      int recNumber = Integer.parseInt(iRecordNumber);
      if (currentResultSet.size() == 0)
        throw new OException("No result set where to find the requested record. Execute a query first.");

      if (currentResultSet.size() <= recNumber)
        throw new OException("The record requested is not part of current result set (0"
            + (currentResultSet.size() > 0 ? "-" + (currentResultSet.size() - 1) : "") + ")");

      setCurrentRecord(recNumber);
    }

    dumpRecordDetails();
  }

  @ConsoleCommand(description = "Display a record as raw bytes")
  public void displayRawRecord(@ConsoleParameter(name = "rid", description = "The record id to display") final String iRecordId)
      throws IOException {
    checkForDatabase();

    ORecordId rid;
    if (iRecordId.indexOf(':') > -1)
      rid = new ORecordId(iRecordId);
    else {
      OIdentifiable rec = setCurrentRecord(Integer.parseInt(iRecordId));
      if (rec != null)
        rid = (ORecordId) rec.getIdentity();
      else
        return;
    }

    ORawBuffer record;
    ORecordId id = new ORecordId(rid);
    if (!(currentDatabase.getStorage() instanceof OLocalPaginatedStorage)) {
      record = currentDatabase.getStorage().readRecord(rid, null, false, null).getResult();
      if (record != null) {
        String content;
        if (Integer.parseInt(properties.get("maxBinaryDisplay")) < record.buffer.length)
          content = new String(Arrays.copyOf(record.buffer, Integer.parseInt(properties.get("maxBinaryDisplay"))));
        else
          content = new String(record.buffer);
        out.println("\nRaw record content. The size is " + record.buffer.length + " bytes, while settings force to print first "
            + content.length() + " bytes:\n\n" + content);
      }
    } else {
      OLocalPaginatedStorage storage = (OLocalPaginatedStorage) currentDatabase.getStorage();
      OPaginatedCluster cluster = (OPaginatedCluster) storage.getClusterById(id.getClusterId());
      if (cluster == null) {
        message("\n cluster with id %i does not exist", id.getClusterId());
        return;
      }
      OPaginatedClusterDebug debugInfo = cluster.readDebug(id.clusterPosition);
      message("\n\nLOW LEVEL CLUSTER INFO");
      message("\n cluster fieldId: %d", debugInfo.fileId);
      message("\n cluster name: %s", cluster.getName());

      message("\n in cluster position: %d", debugInfo.clusterPosition);
      message("\n empty: %b", debugInfo.empty);
      message("\n contentSize: %d", debugInfo.contentSize);
      message("\n n-pages: %d", debugInfo.pages.size());
      message(
          "\n\n +----------PAGE_ID---------------+------IN_PAGE_POSITION----------+---------IN_PAGE_SIZE-----------+----PAGE_CONTENT---->> ");
      for (OClusterPageDebug page : debugInfo.pages) {
        message("\n |%30d ", page.pageIndex);
        message(" |%30d ", page.inPagePosition);
        message(" |%30d ", page.inPageSize);
        message(" |%s", OBase64Utils.encodeBytes(page.content));
      }
      record = cluster.readRecord(id.clusterPosition);
    }
    if (record == null)
      throw new OException("The record has been deleted");
    byte[] buff = record.getBuffer();
    ORecordSerializerBinaryDebug debugger = new ORecordSerializerBinaryDebug();
    ORecordSerializationDebug deserializeDebug = debugger.deserializeDebug(buff, currentDatabase);
    message("\n\nRECORD CONTENT INFO");
    message("\n class name: %s", deserializeDebug.className);
    message("\n fail on Reading: %b", deserializeDebug.readingFailure);
    message("\n fail position: %d", deserializeDebug.failPosition);
    if (deserializeDebug.readingException != null) {
      StringWriter writer = new StringWriter();
      deserializeDebug.readingException.printStackTrace(new PrintWriter(writer));
      message("\n Exception On Reading: %s", writer.getBuffer().toString());
    }

    message("\n number of properties : %d", deserializeDebug.properties.size());
    message("\n\n PROPERTIES");
    for (ORecordSerializationDebugProperty prop : deserializeDebug.properties) {
      message("\n  property name: %s", prop.name);
      message("\n  property type: %s", prop.type.name());
      message("\n  property globlaId: %d", prop.globalId);
      message("\n  fail on reading: %b", prop.faildToRead);
      if (prop.faildToRead) {
        message("\n  failed on reading position: %b", prop.failPosition);
        StringWriter writer = new StringWriter();
        prop.readingException.printStackTrace(new PrintWriter(writer));
        message("\n  Exception on reading: %s", writer.getBuffer().toString());
      } else {
        if (prop.value instanceof ORidBag) {
          message("\n  property value: ORidBug ");
          ((ORidBag) prop.value).debugPrint(System.out);
        } else
          message("\n  property value: %s", prop.value != null ? prop.value.toString() : "null");
      }
      message("\n");
    }

  }

  @ConsoleCommand(aliases = { "status" }, description = "Display information about the database")
  public void info() {
    if (currentDatabaseName != null) {
      message("\nCurrent database: " + currentDatabaseName + " (url=" + currentDatabase.getURL() + ")");

      final OStorage stg = currentDatabase.getStorage();

      if (stg instanceof OStorageRemoteThread) {
        dumpDistributedConfiguration(true);
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

    final OStorageConfiguration dbCfg = stg.getConfiguration();

    message("\n\nDATABASE PROPERTIES");

    if (dbCfg.getProperties() != null) {
      message("\n--------------------------------+----------------------------------------------------+");
      message("\n NAME                           | VALUE                                              |");
      message("\n--------------------------------+----------------------------------------------------+");
      message("\n %-30s | %-50s |", "Name", format(dbCfg.name, 50));
      message("\n %-30s | %-50s |", "Version", format("" + dbCfg.version, 50));
      message("\n %-30s | %-50s |", "Conflict Strategy", format(dbCfg.getConflictStrategy(), 50));
      message("\n %-30s | %-50s |", "Date format", format(dbCfg.dateFormat, 50));
      message("\n %-30s | %-50s |", "Datetime format", format(dbCfg.dateTimeFormat, 50));
      message("\n %-30s | %-50s |", "Timezone", format(dbCfg.getTimeZone().getID(), 50));
      message("\n %-30s | %-50s |", "Locale Country", format(dbCfg.getLocaleCountry(), 50));
      message("\n %-30s | %-50s |", "Locale Language", format(dbCfg.getLocaleLanguage(), 50));
      message("\n %-30s | %-50s |", "Charset", format(dbCfg.getCharset(), 50));
      message("\n %-30s | %-50s |", "Schema RID", format(dbCfg.schemaRecordId, 50));
      message("\n %-30s | %-50s |", "Index Manager RID", format(dbCfg.indexMgrRecordId, 50));
      message("\n %-30s | %-50s |", "Dictionary RID", format(dbCfg.dictionaryRecordId, 50));
      message("\n--------------------------------+----------------------------------------------------+");

      if (!dbCfg.getProperties().isEmpty()) {
        message("\n\nDATABASE CUSTOM PROPERTIES:");
        message("\n +-------------------------------+--------------------------------------------------+");
        message("\n | NAME                          | VALUE                                            |");
        message("\n +-------------------------------+--------------------------------------------------+");
        for (OStorageEntryConfiguration cfg : dbCfg.getProperties())
          message("\n | %-29s | %-49s|", cfg.name, format(cfg.value, 49));
        message("\n +-------------------------------+--------------------------------------------------+");
      }
    }
  }

  @ConsoleCommand(aliases = { "desc" }, description = "Display the schema of a class")
  public void infoClass(@ConsoleParameter(name = "class-name", description = "The name of the class") final String iClassName) {
    if (currentDatabaseName == null) {
      message("\nNo database selected yet.");
      return;
    }

    final OClass cls = currentDatabase.getMetadata().getImmutableSchemaSnapshot().getClass(iClassName);

    if (cls == null) {
      message("\n! Class '" + iClassName + "' does not exist in the database '" + currentDatabaseName + "'");
      return;
    }

    message("\nClass................: " + cls);
    if (cls.getShortName() != null)
      message("\nAlias................: " + cls.getShortName());
    if (cls.hasSuperClasses())
      message("\nSuper classes........: " + Arrays.toString(cls.getSuperClassesNames().toArray()));
    message("\nDefault cluster......: " + currentDatabase.getClusterNameById(cls.getDefaultClusterId()) + " (id="
        + cls.getDefaultClusterId() + ")");
    message("\nSupported cluster ids: " + Arrays.toString(cls.getClusterIds()));
    message("\nCluster selection....: " + cls.getClusterSelection().getName());
    message("\nOversize.............: " + cls.getClassOverSize());

    if (!cls.getSubclasses().isEmpty()) {
      message("\nSubclasses.........: ");
      int i = 0;
      for (OClass c : cls.getSubclasses()) {
        if (i > 0)
          message(", ");
        message(c.getName());
        ++i;
      }
      out.println();
    }

    if (cls.properties().size() > 0) {
      message("\nPROPERTIES");
      message(
          "\n-------------------------------+-------------+-------------------------------+-----------+----------+----------+-----------+-----------+----------+");
      message(
          "\n NAME                          | TYPE        | LINKED TYPE/CLASS             | MANDATORY | READONLY | NOT NULL |    MIN    |    MAX    | COLLATE  |");
      message(
          "\n-------------------------------+-------------+-------------------------------+-----------+----------+----------+-----------+-----------+----------+");

      for (final OProperty p : cls.properties()) {
        try {
          message("\n %-30s| %-12s| %-30s| %-10s| %-9s| %-9s| %-10s| %-10s| %-9s|", p.getName(), p.getType(),
              p.getLinkedClass() != null ? p.getLinkedClass() : p.getLinkedType(), p.isMandatory(), p.isReadonly(), p.isNotNull(),
              p.getMin() != null ? p.getMin() : "", p.getMax() != null ? p.getMax() : "",
              p.getCollate() != null ? p.getCollate().getName() : "");
        } catch (Exception ignored) {
        }
      }
      message(
          "\n-------------------------------+-------------+-------------------------------+-----------+----------+----------+-----------+-----------+----------+");
    }

    final Set<OIndex<?>> indexes = cls.getClassIndexes();
    if (!indexes.isEmpty()) {
      message("\n\nINDEXES (" + indexes.size() + " altogether)");
      message("\n-------------------------------+----------------+");
      message("\n NAME                          | PROPERTIES     |");
      message("\n-------------------------------+----------------+");
      for (final OIndex<?> index : indexes) {
        final OIndexDefinition indexDefinition = index.getDefinition();
        if (indexDefinition != null) {
          final List<String> fields = indexDefinition.getFields();
          message("\n %-30s| %-15s|", index.getName(), fields.get(0) + (fields.size() > 1 ? " (+)" : ""));

          for (int i = 1; i < fields.size(); i++) {
            if (i < fields.size() - 1)
              message("\n %-30s| %-15s|", "", fields.get(i) + " (+)");
            else
              message("\n %-30s| %-15s|", "", fields.get(i));
          }
        } else {
          message("\n %-30s| %-15s|", index.getName(), "");
        }
      }
      message("\n-------------------------------+----------------+");
    }
  }

  @ConsoleCommand(description = "Display all indexes", aliases = { "indexes" })
  public void listIndexes() {
    if (currentDatabaseName != null) {
      message("\n\nINDEXES");
      message(
          "\n----------------------------------------------+------------+-----------------------+----------------+------------+");
      message(
          "\n NAME                                         | TYPE       |         CLASS         |     FIELDS     | RECORDS    |");
      message(
          "\n----------------------------------------------+------------+-----------------------+----------------+------------+");

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
          final long size = index.getKeySize();
          if (indexDefinition == null || indexDefinition.getClassName() == null) {
            message("\n %-45s| %-10s | %-22s| %-15s|%11d |", format(index.getName(), 45), format(index.getType(), 10), "", "",
                size);
          } else {
            final List<String> fields = indexDefinition.getFields();
            if (fields.size() == 1) {
              message("\n %-45s| %-10s | %-22s| %-15s|%11d |", format(index.getName(), 45), format(index.getType(), 10),
                  format(indexDefinition.getClassName(), 22), format(fields.get(0), 10), size);
            } else {
              message("\n %-45s| %-10s | %-22s| %-15s|%11d |", format(index.getName(), 45), format(index.getType(), 10),
                  format(indexDefinition.getClassName(), 22), format(fields.get(0), 10), size);
              for (int i = 1; i < fields.size(); i++) {
                message("\n %-45s| %-10s | %-22s| %-15s|%11s |", "", "", "", fields.get(i), "");
              }
            }
          }

          totalIndexes++;
          totalRecords += size;
        } catch (Exception ignored) {
        }
      }
      message(
          "\n----------------------------------------------+------------+-----------------------+----------------+------------+");
      message("\n TOTAL = %-3d                                                                                     %15d |",
          totalIndexes, totalRecords);
      message(
          "\n-----------------------------------------------------------------------------------------------------------------+");
    } else
      message("\nNo database selected yet.");
  }

  @ConsoleCommand(description = "Display all the configured clusters", aliases = { "clusters" })
  public void listClusters() {
    if (currentDatabaseName != null) {
      message("\n\nCLUSTERS");
      message("\n----------------------------------------------+-------+-------------------+----------------+");
      message("\n NAME                                         | ID    | CONFLICT STRATEGY | RECORDS        |");
      message("\n----------------------------------------------+-------+-------------------+----------------+");

      int clusterId;
      String clusterType;
      long totalElements = 0;
      long count;

      final List<String> clusters = new ArrayList<String>(currentDatabase.getClusterNames());
      Collections.sort(clusters);

      for (String clusterName : clusters) {
        try {
          clusterId = currentDatabase.getClusterIdByName(clusterName);
          final OCluster cluster = currentDatabase.getStorage().getClusterById(clusterId);

          final String conflictStrategy = cluster.getRecordConflictStrategy() != null
              ? cluster.getRecordConflictStrategy().getName() : "";

          count = currentDatabase.countClusterElements(clusterName);
          totalElements += count;

          message("\n %-45s| %5d | %-17s |%15d |", format(clusterName, 45), clusterId, format(conflictStrategy, 15), count);
        } catch (Exception e) {
          if (e instanceof OIOException)
            break;
        }
      }
      message("\n----------------------------------------------+-------+-------------------+----------------+");
      message("\n TOTAL = %-3d                                                              |%15d |", clusters.size(),
          totalElements);
      message("\n------------------------------------------------------+-------------------+----------------+");
    } else
      message("\nNo database selected yet.");
  }

  @ConsoleCommand(description = "Display all the configured classes", aliases = { "classes" })
  public void listClasses() {
    if (currentDatabaseName != null) {
      message("\n\nCLASSES");
      message(
          "\n----------------------------------------------+------------------------------------+------------+----------------+");
      message(
          "\n NAME                                         | SUPERCLASS                         | CLUSTERS   | RECORDS        |");
      message(
          "\n----------------------------------------------+------------------------------------+------------+----------------+");

      long totalElements = 0;
      long count;

      final List<OClass> classes = new ArrayList<OClass>(currentDatabase.getMetadata().getImmutableSchemaSnapshot().getClasses());
      Collections.sort(classes, new Comparator<OClass>() {
        public int compare(OClass o1, OClass o2) {
          return o1.getName().compareToIgnoreCase(o2.getName());
        }
      });

      for (OClass cls : classes) {
        try {
          final StringBuilder clusters = new StringBuilder(1024);
          if (cls.isAbstract())
            clusters.append("-");
          else
            for (int i = 0; i < cls.getClusterIds().length; ++i) {
              if (i > 0)
                clusters.append(",");
              clusters.append(cls.getClusterIds()[i]);
            }

          count = currentDatabase.countClass(cls.getName(), false);
          totalElements += count;

          final String superClasses = cls.hasSuperClasses() ? Arrays.toString(cls.getSuperClassesNames().toArray()) : "";

          message("\n %-45s| %-35s| %-11s|%15d |", format(cls.getName(), 45), format(superClasses, 35), clusters.toString(), count);
        } catch (Exception ignored) {
        }
      }
      message(
          "\n----------------------------------------------+------------------------------------+------------+----------------+");
      message("\n TOTAL = %-3d                                                                                     %15d |",
          classes.size(), totalElements);
      message(
          "\n----------------------------------------------+------------------------------------+------------+----------------+");

    } else
      message("\nNo database selected yet.");
  }

  @ConsoleCommand(description = "Loook up a record using the dictionary. If found, set it as the current record")
  public void dictionaryGet(@ConsoleParameter(name = "key", description = "The key to search") final String iKey) {
    checkForDatabase();

    currentRecord = currentDatabase.getDictionary().get(iKey);
    if (currentRecord == null)
      message("\nEntry not found in dictionary.");
    else {
      currentRecord = (ORecord) currentRecord.load();
      displayRecord(null);
    }
  }

  @ConsoleCommand(description = "Insert or modify an entry in the database dictionary. The entry is comprised of key=String, value=record-id")
  public void dictionaryPut(@ConsoleParameter(name = "key", description = "The key to bind") final String iKey,
      @ConsoleParameter(name = "record-id", description = "The record-id of the record to bind to the key") final String iRecordId) {
    checkForDatabase();

    currentRecord = currentDatabase.load(new ORecordId(iRecordId));
    if (currentRecord == null)
      message("\nError: record with id '" + iRecordId + "' was not found in database");
    else {
      currentDatabase.getDictionary().put(iKey, currentRecord);
      displayRecord(null);
      message("\nThe entry " + iKey + "=" + iRecordId + " has been inserted in the database dictionary");
    }
  }

  @ConsoleCommand(description = "Remove the association in the dictionary")
  public void dictionaryRemove(@ConsoleParameter(name = "key", description = "The key to remove") final String iKey) {
    checkForDatabase();

    boolean result = currentDatabase.getDictionary().remove(iKey);
    if (!result)
      message("\nEntry not found in dictionary.");
    else
      message("\nEntry removed from the dictionary.");
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

      message("\nCopying database '" + iDatabaseName + "' to the server '" + iRemoteName + "' via network streaming...");

      serverAdmin.copyDatabase(iDatabaseName, iDatabaseUserName, iDatabaseUserPassword, iRemoteName, iRemoteEngine);

      message("\nDatabase '" + iDatabaseName + "' has been copied to the server '" + iRemoteName + "'");

    } catch (Exception e) {
      printError(e);
    }
  }

  @ConsoleCommand(description = "Displays the status of the cluster nodes")
  public void clusterStatus() throws IOException {
    if (serverAdmin == null)
      throw new IllegalStateException("You must be connected to a remote server to get the cluster status");

    checkForRemoteServer();
    try {

      message("\nCluster status:");
      out.println(serverAdmin.clusterStatus().toJSON("attribSameRow,alwaysFetchEmbedded,fetchPlan:*:0"));

    } catch (Exception e) {
      printError(e);
    }
  }

  @ConsoleCommand(description = "Check database integrity")
  public void checkDatabase(@ConsoleParameter(name = "options", description = "Options: -v", optional = true) final String iOptions)
      throws IOException {
    checkForDatabase();

    if (!(currentDatabase.getStorage() instanceof OAbstractPaginatedStorage)) {
      message("\nCannot check integrity of non-local database. Connect to it using local mode.");
      return;
    }

    boolean verbose = iOptions != null && iOptions.contains("-v");

    try {
      ((OAbstractPaginatedStorage) currentDatabase.getStorage()).check(verbose, this);
    } catch (ODatabaseImportException e) {
      printError(e);
    }
  }

  @ConsoleCommand(description = "Repair database structure")
  public void repairDatabase(
      @ConsoleParameter(name = "options", description = "Options: -v", optional = true) final String iOptions) throws IOException {
    checkForDatabase();

    message("\nRepairing database...");

    boolean verbose = iOptions != null && iOptions.contains("-v");

    long fixedLinks = 0l;
    long modifiedDocuments = 0l;
    long errors = 0l;

    try {

      message("\n- Fixing dirty links...");
      for (String clusterName : currentDatabase.getClusterNames()) {
        for (ORecord rec : currentDatabase.browseCluster(clusterName)) {
          try {
            if (rec instanceof ODocument) {
              boolean changed = false;

              final ODocument doc = (ODocument) rec;
              for (String fieldName : doc.fieldNames()) {
                final Object fieldValue = doc.rawField(fieldName);

                if (fieldValue instanceof OIdentifiable) {
                  if (fixLink(fieldValue)) {
                    doc.field(fieldName, (OIdentifiable) null);
                    fixedLinks++;
                    changed = true;
                    if (verbose)
                      message("\n--- reset link " + ((OIdentifiable) fieldValue).getIdentity() + " in field '" + fieldName
                          + "' (rid=" + doc.getIdentity() + ")");
                  }
                } else if (fieldValue instanceof Iterable<?>) {
                  if (fieldValue instanceof ORecordLazyMultiValue)
                    ((ORecordLazyMultiValue) fieldValue).setAutoConvertToRecord(false);

                  final Iterator<Object> it = ((Iterable) fieldValue).iterator();
                  for (int i = 0; it.hasNext(); ++i) {
                    final Object v = it.next();
                    if (fixLink(v)) {
                      it.remove();
                      fixedLinks++;
                      changed = true;
                      if (verbose)
                        message("\n--- reset link " + ((OIdentifiable) v).getIdentity() + " as item " + i
                            + " in collection of field '" + fieldName + "' (rid=" + doc.getIdentity() + ")");
                    }
                  }
                }
              }

              if (changed) {
                modifiedDocuments++;
                doc.save();

                if (verbose)
                  message("\n-- updated document " + doc.getIdentity());
              }
            }
          } catch (Exception e) {
            errors++;
          }
        }
      }
      if (verbose)
        message("\n");

      message("Done! Fixed links: " + fixedLinks + ", modified documents: " + modifiedDocuments);

      message("\nRepair database complete (" + errors + " errors)");
    } catch (Exception e) {
      printError(e);
    }
  }

  @ConsoleCommand(description = "Compare two databases")
  public void compareDatabases(@ConsoleParameter(name = "db1-url", description = "URL of the first database") final String iDb1URL,
      @ConsoleParameter(name = "db2-url", description = "URL of the second database") final String iDb2URL,
      @ConsoleParameter(name = "user-name", description = "User name", optional = true) final String iUserName,
      @ConsoleParameter(name = "user-password", description = "User password", optional = true) final String iUserPassword,
      @ConsoleParameter(name = "detect-mapping-data", description = "Whether RID mapping data after DB import should be tried to found on the disk.", optional = true) String autoDiscoveringMappingData)
          throws IOException {
    try {
      final ODatabaseCompare compare;
      if (iUserName == null)
        compare = new ODatabaseCompare(iDb1URL, iDb2URL, this);
      else
        compare = new ODatabaseCompare(iDb1URL, iDb2URL, iUserName, iUserPassword, this);

      compare.setAutoDetectExportImportMap(autoDiscoveringMappingData != null ? Boolean.valueOf(autoDiscoveringMappingData) : true);
      compare.setCompareIndexMetadata(true);
      compare.compare();
    } catch (ODatabaseExportException e) {
      printError(e);
    }
  }

  @ConsoleCommand(description = "Import a database into the current one", splitInWords = false)
  public void importDatabase(@ConsoleParameter(name = "options", description = "Import options") final String text)
      throws IOException {
    checkForDatabase();

    message("\nImporting database " + text + "...");

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

  @ConsoleCommand(description = "Backup a database", splitInWords = false)
  public void backupDatabase(@ConsoleParameter(name = "options", description = "Backup options") final String iText)
      throws IOException {
    checkForDatabase();

    final List<String> items = OStringSerializerHelper.smartSplit(iText, ' ');

    if (items.size() < 2)
      try {
        syntaxError("backupDatabase", getClass().getMethod("backupDatabase", String.class));
        return;
      } catch (NoSuchMethodException ignored) {
      }

    out.println(new StringBuilder("Backuping current database to: ").append(iText).append("..."));

    final String fileName = items.size() <= 0 || items.get(1).charAt(0) == '-' ? null : items.get(1);

    int bufferSize = Integer.parseInt(properties.get("backupBufferSize"));
    int compressionLevel = Integer.parseInt(properties.get("backupCompressionLevel"));

    for (int i = 2; i < items.size(); ++i) {
      final String item = items.get(i);
      final int sep = item.indexOf('=');
      if (sep == -1) {
        OLogManager.instance().warn(this, "Unrecognized parameter %s, skipped", item);
        continue;
      }

      final String parName = item.substring(1, sep);
      final String parValue = item.substring(sep + 1);

      if (parName.equalsIgnoreCase("bufferSize"))
        bufferSize = Integer.parseInt(parValue);
      else if (parName.equalsIgnoreCase("compressionLevel"))
        compressionLevel = Integer.parseInt(parValue);
    }

    final long startTime = System.currentTimeMillis();
    try {
      final FileOutputStream fos = new FileOutputStream(fileName);
      try {
        currentDatabase.backup(fos, null, null, this, compressionLevel, bufferSize);

        message("\nBackup executed in %.2f seconds", ((float) (System.currentTimeMillis() - startTime) / 1000));

      } finally {
        fos.flush();
        fos.close();
      }
    } catch (ODatabaseExportException e) {
      printError(e);
    }
  }

  @ConsoleCommand(description = "Restore a database into the current one", splitInWords = false)
  public void restoreDatabase(@ConsoleParameter(name = "options", description = "Restore options") final String text)
      throws IOException {
    checkForDatabase();

    message("\nRestoring database %s...", text);

    final List<String> items = OStringSerializerHelper.smartSplit(text, ' ');

    if (items.size() < 2)
      try {
        syntaxError("restoreDatabase", getClass().getMethod("restoreDatabase", String.class));
        return;
      } catch (NoSuchMethodException e) {
      }

    final String fileName = items.size() <= 0 || (items.get(1)).charAt(0) == '-' ? null : items.get(1);
    // final String options = fileName != null ? text.substring((items.get(0)).length() + (items.get(1)).length() + 1).trim() :
    // text;

    final long startTime = System.currentTimeMillis();
    try {
      final FileInputStream f = new FileInputStream(fileName);
      try {
        currentDatabase.restore(f, null, null, this);
      } finally {
        f.close();
      }

      message("\nDatabase restored in %.2f seconds", ((float) (System.currentTimeMillis() - startTime) / 1000));

    } catch (ODatabaseImportException e) {
      printError(e);
    }
  }

  @ConsoleCommand(description = "Export a database", splitInWords = false)
  public void exportDatabase(@ConsoleParameter(name = "options", description = "Export options") final String iText)
      throws IOException {
    checkForDatabase();

    out.println(new StringBuilder("Exporting current database to: ").append(iText).append(" in GZipped JSON format ..."));
    final List<String> items = OStringSerializerHelper.smartSplit(iText, ' ');
    final String fileName = items.size() <= 1 || items.get(1).charAt(0) == '-' ? null : items.get(1);
    final String options = fileName != null ? iText.substring(items.get(0).length() + items.get(1).length() + 1).trim() : iText;

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

    message("\nExporting current database to: " + iOutputFilePath + "...");

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
      @ConsoleParameter(name = "options", description = "Options", optional = true) String iOptions) throws IOException {
    checkForDatabase();
    checkCurrentObject();

    final ORecordSerializer serializer = ORecordSerializerFactory.instance().getFormat(iFormat.toLowerCase());

    if (serializer == null) {
      message("\nERROR: Format '" + iFormat + "' was not found.");
      printSupportedSerializerFormat();
      return;
    } else if (!(serializer instanceof ORecordSerializerStringAbstract)) {
      message("\nERROR: Format '" + iFormat + "' does not export as text.");
      printSupportedSerializerFormat();
      return;
    }

    if (iOptions == null || iOptions.length() <= 0) {
      iOptions = "rid,version,class,type,keepTypes,alwaysFetchEmbedded,fetchPlan:*:0,prettyPrint";
    }

    try {
      out.println(((ORecordSerializerStringAbstract) serializer).toString(currentRecord, iOptions));
    } catch (ODatabaseExportException e) {
      printError(e);
    }
  }

  @ConsoleCommand(description = "Return all configured properties")
  public void properties() {
    message("\nPROPERTIES:");
    message("\n+-------------------------------+--------------------------------+");
    message("\n| %-30s| %-30s |", "NAME", "VALUE");
    message("\n+-------------------------------+--------------------------------+");
    for (Entry<String, String> p : properties.entrySet()) {
      message("\n| %-30s| %-30s |", p.getKey(), p.getValue());
    }
    message("\n+-------------------------------+--------------------------------+");
  }

  @ConsoleCommand(description = "Return the value of a property")
  public void get(@ConsoleParameter(name = "property-name", description = "Name of the property") final String iPropertyName) {
    Object value = properties.get(iPropertyName);

    out.println();

    if (value == null)
      message("\nProperty '" + iPropertyName + "' is not setted");
    else
      out.println(iPropertyName + " = " + value);
  }

  @ConsoleCommand(description = "Change the value of a property")
  public void set(@ConsoleParameter(name = "property-name", description = "Name of the property") final String iPropertyName,
      @ConsoleParameter(name = "property-value", description = "Value to set") final String iPropertyValue) {
    Object prevValue = properties.get(iPropertyName);

    out.println();

    if (iPropertyName.equalsIgnoreCase("limit")
        && (Integer.parseInt(iPropertyValue) == 0 || Integer.parseInt(iPropertyValue) < -1)) {
      message("\nERROR: Limit must be > 0 or = -1 (no limit)");
    } else {

      if (prevValue != null)
        message("\nPrevious value was: " + prevValue);

      properties.put(iPropertyName, iPropertyValue);

      out.println();
      out.println(iPropertyName + " = " + iPropertyValue);
    }
  }

  @ConsoleCommand(description = "Declare an intent")
  public void declareIntent(
      @ConsoleParameter(name = "Intent name", description = "name of the intent to execute") final String iIntentName) {
    checkForDatabase();

    message("\nDeclaring intent '" + iIntentName + "'...");

    if (iIntentName.equalsIgnoreCase("massiveinsert"))
      currentDatabase.declareIntent(new OIntentMassiveInsert());
    else if (iIntentName.equalsIgnoreCase("massiveread"))
      currentDatabase.declareIntent(new OIntentMassiveRead());
    else
      throw new IllegalArgumentException(
          "Intent '" + iIntentName + "' not supported. Available ones are: massiveinsert, massiveread");

    message("\nIntent '" + iIntentName + "' setted successfully");
  }

  @ConsoleCommand(description = "Execute a command against the profiler")
  public void profiler(
      @ConsoleParameter(name = "profiler command", description = "command to execute against the profiler") final String iCommandName) {
    if (iCommandName.equalsIgnoreCase("on")) {
      Orient.instance().getProfiler().startRecording();
      message("\nProfiler is ON now, use 'profiler off' to turn off.");
    } else if (iCommandName.equalsIgnoreCase("off")) {
      Orient.instance().getProfiler().stopRecording();
      message("\nProfiler is OFF now, use 'profiler on' to turn on.");
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
      message("\nRemote configuration: ");
    } else {
      value = config.getValueAsString();
      message("\nLocal configuration: ");
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
  public void configSet(@ConsoleParameter(name = "config-name", description = "Name of the configuration") final String iConfigName,
      @ConsoleParameter(name = "config-value", description = "Value to set") final String iConfigValue) throws IOException {
    final OGlobalConfiguration config = OGlobalConfiguration.findByKey(iConfigName);
    if (config == null)
      throw new IllegalArgumentException("Configuration variable '" + iConfigName + "' not found");

    if (serverAdmin != null) {
      serverAdmin.setGlobalConfiguration(config, iConfigValue);
      message("\n\nRemote configuration value changed correctly");
    } else {
      config.setValue(iConfigValue);
      message("\n\nLocal configuration value changed correctly");
    }
    out.println();
  }

  @ConsoleCommand(description = "Return all the configuration values")
  public void config() throws IOException {
    if (serverAdmin != null) {
      // REMOTE STORAGE
      final Map<String, String> values = serverAdmin.getGlobalConfigurations();

      message("\nREMOTE SERVER CONFIGURATION:");
      message("\n+------------------------------------+--------------------------------+");
      message("\n| %-35s| %-30s |", "NAME", "VALUE");
      message("\n+------------------------------------+--------------------------------+");
      for (Entry<String, String> p : values.entrySet()) {
        message("\n| %-35s= %-30s |", p.getKey(), p.getValue());
      }
    } else {
      // LOCAL STORAGE
      message("\nLOCAL SERVER CONFIGURATION:");
      message("\n+------------------------------------+--------------------------------+");
      message("\n| %-35s| %-30s |", "NAME", "VALUE");
      message("\n+------------------------------------+--------------------------------+");
      for (OGlobalConfiguration cfg : OGlobalConfiguration.values()) {
        message("\n| %-35s= %-30s |", cfg.getKey(), cfg.getValue());
      }
    }

    message("\n+------------------------------------+--------------------------------+");
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
  public ORecord getCurrentRecord() {
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

    message("\nOK");
  }

  /** Should be used only by console commands */
  public void reloadRecordInternal(String iRecordId, String iFetchPlan) {
    checkForDatabase();

    currentRecord = currentDatabase.executeReadRecord(new ORecordId(iRecordId), null, null, iFetchPlan, true, false, false,
        OStorage.LOCKING_STRATEGY.NONE, new ODatabaseDocumentTx.SimpleRecordReader());
    displayRecord(null);

    message("\nOK");
  }

  /** Should be used only by console commands */
  public void checkForRemoteServer() {
    if (serverAdmin == null && (currentDatabase == null || !(currentDatabase.getStorage() instanceof OStorageRemoteThread)
        || currentDatabase.isClosed()))
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

  public String ask(final String iText) {
    out.print(iText);
    final Scanner scanner = new Scanner(in);
    final String answer = scanner.nextLine();
    scanner.close();
    return answer;
  }

  public void onMessage(final String iText) {
    message(iText);
  }

  @Override
  public void onBegin(final Object iTask, final long iTotal, Object metadata) {
    lastPercentStep = 0;

    message("[");
    if (interactiveMode) {
      for (int i = 0; i < 10; ++i)
        message(" ");
      message("]   0%");
    }
  }

  public boolean onProgress(final Object iTask, final long iCounter, final float iPercent) {
    final int completitionBar = (int) iPercent / 10;

    if (((int) (iPercent * 10)) == lastPercentStep)
      return true;

    final StringBuilder buffer = new StringBuilder(64);

    if (interactiveMode) {
      buffer.append("\r[");
      for (int i = 0; i < completitionBar; ++i)
        buffer.append('=');
      for (int i = completitionBar; i < 10; ++i)
        buffer.append(' ');
      message("] %3.1f%% ", iPercent);
    } else {
      for (int i = lastPercentStep / 100; i < completitionBar; ++i)
        buffer.append('=');
    }

    message(buffer.toString());

    lastPercentStep = (int) (iPercent * 10);
    return true;
  }

  @ConsoleCommand(description = "Display the current path")
  public void pwd() {
    message("\nCurrent path: " + new File("").getAbsolutePath());
  }

  public void onCompletition(final Object iTask, final boolean iSucceed) {
    if (interactiveMode)
      if (iSucceed)
        message("\r[==========] 100% Done.");
      else
        message(" Error!");
    else
      message(iSucceed ? "] Done." : " Error!");
  }

  /**
   * Checks if the link must be fixed.
   * 
   * @param fieldValue
   *          Field containing the OIdentifiable (RID or Record)
   * @return true to fix it, otherwise false
   */
  protected boolean fixLink(final Object fieldValue) {
    if (fieldValue instanceof OIdentifiable) {
      final ORID id = ((OIdentifiable) fieldValue).getIdentity();

      if (id.isValid())
        if (id.isPersistent()) {
          final ORecord connected = ((OIdentifiable) fieldValue).getRecord();
          if (connected == null)
            return true;
        } else
          return true;
    }
    return false;
  }

  protected void dumpDistributedConfiguration(final boolean iForce) {
    if (currentDatabase == null)
      return;

    final OStorage stg = currentDatabase.getStorage();
    if (stg instanceof OStorageRemoteThread) {
      final ODocument distributedCfg = ((OStorageRemoteThread) stg).getClusterConfiguration();
      if (distributedCfg != null && !distributedCfg.isEmpty()) {
        message("\n\nDISTRIBUTED CONFIGURATION:\n" + distributedCfg.toJSON("prettyPrint"));
      } else if (iForce)
        message("\n\nDISTRIBUTED CONFIGURATION: none (OrientDB is running in standalone mode)");
    }
  }

  @Override
  protected boolean isCollectingCommands(final String iLine) {
    return iLine.startsWith("js") || iLine.startsWith("script");
  }

  @Override
  protected void onBefore() {
    super.onBefore();

    setResultset(new ArrayList<OIdentifiable>());

    // DISABLE THE NETWORK AND STORAGE TIMEOUTS
    OGlobalConfiguration.STORAGE_LOCK_TIMEOUT.setValue(0);
    OGlobalConfiguration.NETWORK_LOCK_TIMEOUT.setValue(0);
    OGlobalConfiguration.CLIENT_CHANNEL_MIN_POOL.setValue(1);
    OGlobalConfiguration.CLIENT_CHANNEL_MAX_POOL.setValue(2);

    properties.put("limit", "20");
    properties.put("width", "150");
    properties.put("debug", "false");
    properties.put("collectionMaxItems", "10");
    properties.put("maxBinaryDisplay", "150");
    properties.put("verbose", "2");
    properties.put("ignoreErrors", "false");
    properties.put("backupCompressionLevel", "9"); // 9 = MAX
    properties.put("backupBufferSize", "1048576"); // 1MB
  }

  protected OIdentifiable setCurrentRecord(final int iIndex) {
    currentRecordIdx = iIndex;
    if (iIndex < currentResultSet.size())
      currentRecord = (ORecord) currentResultSet.get(iIndex);
    else
      currentRecord = null;
    return currentRecord;
  }

  protected void printApplicationInfo() {
    message("\nOrientDB console v." + OConstants.getVersion() + " " + OConstants.ORIENT_URL);
    message("\nType 'help' to display all the supported commands.");
  }

  protected void dumpResultSet(final int limit) {
    new OTableFormatter(this).setMaxWidthSize(getWindowSize()).writeRecords(currentResultSet, limit);
  }

  protected float getElapsedSecs(final long start) {
    return (float) (System.currentTimeMillis() - start) / 1000;
  }

  protected void printError(final Exception e) {
    if (properties.get("debug") != null && Boolean.parseBoolean(properties.get("debug"))) {
      message("\n\n!ERROR:");
      e.printStackTrace(err);
    } else {
      // SHORT FORM
      message("\n\n!ERROR: " + e.getMessage());

      if (e.getCause() != null) {
        Throwable t = e.getCause();
        while (t != null) {
          message("\n-> " + t.getMessage());
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

  @Override
  protected String getContext() {
    if (currentDatabase != null && currentDatabaseName != null) {
      final StringBuilder buffer = new StringBuilder(64);
      buffer.append(" {db=");
      buffer.append(currentDatabaseName);
      if (currentDatabase.getTransaction().isActive()) {
        buffer.append(" tx=[");
        buffer.append(currentDatabase.getTransaction().getEntryCount());
        buffer.append(" entries]");
      }

      buffer.append("}");
      return buffer.toString();
    } else if (serverAdmin != null)
      return " {server=" + serverAdmin.getURL() + "}";
    return "";
  }

  @Override
  protected String getPrompt() {
    return String.format("orientdb%s> ", getContext());
  }

  protected void parseResult() {
    setResultset(null);

    if (currentResult instanceof Map<?, ?>)
      return;

    final Object first = OMultiValue.getFirstValue(currentResult);

    if (first instanceof OIdentifiable) {
      if (currentResult instanceof List<?>)
        currentResultSet = (List<OIdentifiable>) currentResult;
      else if (currentResult instanceof Collection<?>) {
        currentResultSet = new ArrayList<OIdentifiable>();
        currentResultSet.addAll((Collection<? extends OIdentifiable>) currentResult);
      } else if (currentResult.getClass().isArray()) {
        currentResultSet = new ArrayList<OIdentifiable>();
        Collections.addAll(currentResultSet, (OIdentifiable[]) currentResult);
      }

      setResultset(currentResultSet);
    }
  }

  protected void setResultset(final List<OIdentifiable> iResultset) {
    currentResultSet = iResultset;
    currentRecordIdx = 0;
    currentRecord = iResultset == null || iResultset.isEmpty() ? null : (ORecord) iResultset.get(0).getRecord();
  }

  protected void resetResultSet() {
    currentResultSet = null;
    currentRecord = null;
  }

  protected void executeServerSideScript(final String iLanguage, final String iText) {
    if (iText == null)
      return;

    resetResultSet();

    long start = System.currentTimeMillis();
    currentResult = currentDatabase.command(new OCommandScript(iLanguage, iText)).execute();
    float elapsedSeconds = getElapsedSecs(start);

    parseResult();
    if (currentResultSet != null) {
      dumpResultSet(-1);
      message("\nServer side script executed in %f sec(s). Returned %d records", elapsedSeconds, currentResultSet.size());
    } else {
      String lineFeed = currentResult instanceof Map<?, ?> ? "\n" : "";
      message("\nServer side script executed in %f sec(s). Value returned is: %s%s", elapsedSeconds, lineFeed, currentResult);
    }
  }

  protected Map<String, List<String>> parseOptions(final String iOptions) {
    final Map<String, List<String>> options = new HashMap<String, List<String>>();
    if (iOptions != null) {
      final List<String> opts = OStringSerializerHelper.smartSplit(iOptions, ' ');
      for (String o : opts) {
        final int sep = o.indexOf('=');
        if (sep == -1) {
          OLogManager.instance().warn(this, "Unrecognized option %s, skipped", o);
          continue;
        }

        final String option = o.substring(0, sep);
        final List<String> items = OStringSerializerHelper.smartSplit(o.substring(sep + 1), ' ');

        options.put(o, items);
      }
    }
    return options;
  }

  protected int getWindowSize() {
    if (properties.containsKey("width"))
      return Integer.parseInt(properties.get("width"));
    return windowSize;
  }

  protected int getCollectionMaxItems() {
    if (properties.containsKey("collectionMaxItems"))
      return Integer.parseInt(properties.get("collectionMaxItems"));
    return collectionMaxItems;
  }

  private void dumpRecordDetails() {
    if (currentRecord == null)
      return;
    else if (currentRecord instanceof ODocument) {
      ODocument rec = (ODocument) currentRecord;
      message("\n+-------------------------------------------------------------------------------------------------+");
      message("\n| Document - @class: %-37s @rid: %-15s @version: %-6s |", rec.getClassName(), rec.getIdentity().toString(),
          rec.getRecordVersion().toString());
      message("\n+-------------------------------------------------------------------------------------------------+");
      message("\n| %24s | %-68s |", "Name", "Value");
      message("\n+-------------------------------------------------------------------------------------------------+");
      Object value;
      for (String fieldName : rec.fieldNames()) {
        value = rec.field(fieldName);
        if (value instanceof byte[])
          value = "byte[" + ((byte[]) value).length + "]";
        else if (value instanceof Iterator<?>) {
          final List<Object> coll = new ArrayList<Object>();
          while (((Iterator<?>) value).hasNext())
            coll.add(((Iterator<?>) value).next());
          value = coll;
        } else if (OMultiValue.isMultiValue(value)) {
          final int size = OMultiValue.getSize(value);
          if (size < getCollectionMaxItems()) {
            final StringBuilder buffer = new StringBuilder(50);
            for (Object o : OMultiValue.getMultiValueIterable(value)) {
              if (buffer.length() > 0)
                buffer.append(',');
              buffer.append(o);
            }
            value = "[" + buffer.toString() + "]";
          }
        }

        message("\n| %24s | %-68s |", fieldName, value);
      }

    } else if (currentRecord instanceof ORecordFlat) {
      ORecordFlat rec = (ORecordFlat) currentRecord;
      message("\n+-------------------------------------------------------------------------------------------------+");
      message("\n| Flat     - @rid: %s @version: %s", rec.getIdentity().toString(), rec.getRecordVersion().toString());
      message("\n+-------------------------------------------------------------------------------------------------+");
      message(rec.value());

    } else if (currentRecord instanceof ORecordBytes) {
      ORecordBytes rec = (ORecordBytes) currentRecord;
      message("\n+-------------------------------------------------------------------------------------------------+");
      message("\n| Bytes    - @rid: %s @version: %s", rec.getIdentity().toString(), rec.getRecordVersion().toString());
      message("\n+-------------------------------------------------------------------------------------------------+");

      final byte[] value = rec.toStream();
      final int max = Math.min(Integer.parseInt(properties.get("maxBinaryDisplay")), Array.getLength(value));
      for (int i = 0; i < max; ++i) {
        message("%03d", Array.getByte(value, i));
      }

    } else {
      message("\n+-------------------------------------------------------------------------------------------------+");
      message("\n| %s - record id: %s   v.%s", currentRecord.getClass().getSimpleName(), currentRecord.getIdentity().toString(),
          currentRecord.getRecordVersion().toString());
    }
    message("\n+-------------------------------------------------------------------------------------------------+");
    out.println();
  }

  private void printSupportedSerializerFormat() {
    message("\nSupported formats are:");

    for (ORecordSerializer s : ORecordSerializerFactory.instance().getFormats()) {
      if (s instanceof ORecordSerializerStringAbstract)
        message("\n- " + s.toString());
    }
  }

  private void browseRecords(final int limit, final OIdentifiableIterator<?> it) {
    final OTableFormatter tableFormatter = new OTableFormatter(this).setMaxWidthSize(getWindowSize());

    setResultset(new ArrayList<OIdentifiable>());
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

    resetResultSet();

    final long start = System.currentTimeMillis();

    final Object result = new OCommandSQL(iReceivedCommand).setProgressListener(this).execute();

    float elapsedSeconds = getElapsedSecs(start);

    if (iIncludeResult)
      message(iMessage, result, elapsedSeconds);
    else
      message(iMessage, elapsedSeconds);

    return result;
  }
}
