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
package com.orientechnologies.orient.console;

import static com.orientechnologies.orient.core.config.OGlobalConfiguration.WARNING_DEFAULT_USERS;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.console.OConsoleProperties;
import com.orientechnologies.common.console.TTYConsoleReader;
import com.orientechnologies.common.console.annotation.ConsoleCommand;
import com.orientechnologies.common.console.annotation.ConsoleParameter;
import com.orientechnologies.common.exception.OSystemException;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.io.OIOException;
import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.client.remote.ODatabaseImportRemote;
import com.orientechnologies.orient.client.remote.OEngineRemote;
import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.client.remote.OStorageRemote;
import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.OSignalHandler;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.command.script.OCommandExecutorScript;
import com.orientechnologies.orient.core.command.script.OCommandScript;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.config.OStorageEntryConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.OrientDBConfigBuilder;
import com.orientechnologies.orient.core.db.OrientDBInternal;
import com.orientechnologies.orient.core.db.OrientDBRemote;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentRemote;
import com.orientechnologies.orient.core.db.document.SimpleRecordReader;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.tool.OBonsaiTreeRepair;
import com.orientechnologies.orient.core.db.tool.ODatabaseCompare;
import com.orientechnologies.orient.core.db.tool.ODatabaseExport;
import com.orientechnologies.orient.core.db.tool.ODatabaseExportException;
import com.orientechnologies.orient.core.db.tool.ODatabaseImport;
import com.orientechnologies.orient.core.db.tool.ODatabaseImportException;
import com.orientechnologies.orient.core.db.tool.ODatabaseRepair;
import com.orientechnologies.orient.core.db.tool.OGraphRepair;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.ORetryQueryException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.intent.OIntentMassiveRead;
import com.orientechnologies.orient.core.iterator.OIdentifiableIterator;
import com.orientechnologies.orient.core.iterator.ORecordIteratorCluster;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.metadata.security.OUser;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.OBlob;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.security.OSecurityManager;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializerFactory;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerStringAbstract;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.sql.filter.OSQLPredicate;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.util.OURLConnection;
import com.orientechnologies.orient.core.util.OURLHelper;
import com.orientechnologies.orient.server.config.OServerConfigurationManager;
import com.orientechnologies.orient.server.config.OServerUserConfiguration;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Array;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class OConsoleDatabaseApp extends OrientConsole
    implements OCommandOutputListener, OProgressListener {
  protected ODatabaseDocumentInternal currentDatabase;
  protected String currentDatabaseName;
  protected ORecord currentRecord;
  protected int currentRecordIdx;
  protected List<OIdentifiable> currentResultSet;
  protected Object currentResult;
  protected OURLConnection urlConnection;
  protected OrientDB orientDB;
  private int lastPercentStep;
  private String currentDatabaseUserName;
  private String currentDatabaseUserPassword;
  private int maxMultiValueEntries = 10;

  public OConsoleDatabaseApp(final String[] args) {
    super(args);
  }

  public static void main(final String[] args) {
    int result = 0;

    final boolean interactiveMode = isInteractiveMode(args);
    try {
      final OConsoleDatabaseApp console = new OConsoleDatabaseApp(args);
      boolean tty = false;
      try {
        if (setTerminalToCBreak(interactiveMode)) tty = true;

        Runtime.getRuntime().addShutdownHook(new Thread(() -> restoreTerminal(interactiveMode)));

      } catch (Exception ignored) {
      }

      new OSignalHandler().installDefaultSignals(signal -> restoreTerminal(interactiveMode));

      if (tty) console.setReader(new TTYConsoleReader(console.historyEnabled()));

      result = console.run();

    } finally {
      restoreTerminal(interactiveMode);
    }

    Orient.instance().shutdown();
    System.exit(result);
  }

  protected static void restoreTerminal(final boolean interactiveMode) {
    try {
      stty("echo", interactiveMode);
    } catch (Exception ignored) {
    }
  }

  protected static boolean setTerminalToCBreak(final boolean interactiveMode)
      throws IOException, InterruptedException {
    // set the console to be character-buffered instead of line-buffered
    int result = stty("-icanon min 1", interactiveMode);
    if (result != 0) {
      return false;
    }

    // disable character echoing
    stty("-echo", interactiveMode);
    return true;
  }

  /** Execute the stty command with the specified arguments against the current active terminal. */
  protected static int stty(final String args, final boolean interactiveMode)
      throws IOException, InterruptedException {
    if (!interactiveMode) {
      return -1;
    }

    final String cmd = "stty " + args + " < /dev/tty";

    final Process p = Runtime.getRuntime().exec(new String[] {"sh", "-c", cmd});
    p.waitFor(10, TimeUnit.SECONDS);

    return p.exitValue();
  }

  private void checkDefaultPassword(String database, String user, String password) {
    if ((("admin".equals(user) && "admin".equals(password))
            || ("reader".equals(user) && "reader".equals(password))
            || ("writer".equals(user) && "writer".equals(password)))
        && WARNING_DEFAULT_USERS.getValueAsBoolean()) {
      message(
          String.format(
              "IMPORTANT! Using default password is unsafe, please change password for user '%s' on database '%s'",
              user, database));
    }
  }

  @ConsoleCommand(
      aliases = {"use database"},
      description = "Connect to a database or a remote Server instance",
      onlineHelp = "Console-Command-Connect")
  public void connect(
      @ConsoleParameter(
              name = "url",
              description =
                  "The url of the remote server or the database to connect to in the format '<mode>:<path>'")
          String iURL,
      @ConsoleParameter(name = "user", description = "User name") String iUserName,
      @ConsoleParameter(name = "password", description = "User password", optional = true)
          String iUserPassword)
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
    urlConnection = OURLHelper.parseNew(iURL);
    if (urlConnection.getDbName() != null && !"".equals(urlConnection.getDbName())) {
      checkDefaultPassword(
          urlConnection.getDbName(), currentDatabaseUserName, currentDatabaseUserPassword);
    }
    orientDB =
        new OrientDB(
            urlConnection.getType() + ":" + urlConnection.getPath(),
            iUserName,
            iUserPassword,
            OrientDBConfig.defaultConfig());

    if (!"".equals(urlConnection.getDbName())) {
      // OPEN DB
      message("\nConnecting to database [" + iURL + "] with user '" + iUserName + "'...");
      currentDatabase =
          (ODatabaseDocumentInternal)
              orientDB.open(urlConnection.getDbName(), iUserName, iUserPassword);
      currentDatabaseName = currentDatabase.getName();
    }

    message("OK");

    final ODocument distribCfg = getDistributedConfiguration();
    if (distribCfg != null) listServers();
  }

  @ConsoleCommand(
      aliases = {"close database"},
      description = "Disconnect from the current database",
      onlineHelp = "Console-Command-Disconnect")
  public void disconnect() {
    if (currentDatabase != null) {
      message("\nDisconnecting from the database [" + currentDatabaseName + "]...");

      currentDatabase.activateOnCurrentThread();
      if (!currentDatabase.isClosed()) currentDatabase.close();

      currentDatabase = null;
      currentDatabaseName = null;
      currentRecord = null;

      message("OK");
      out.println();
    }
    urlConnection = null;
    if (orientDB != null) {
      orientDB.close();
    }
  }

  @ConsoleCommand(
      description =
          "Create a new database. For encrypted database or portion of database, set the variable 'storage.encryptionKey' with the key to use",
      onlineHelp = "Console-Command-Create-Database")
  public void createDatabase(
      @ConsoleParameter(
              name = "database-url",
              description = "The url of the database to create in the format '<mode>:<path>'")
          String databaseURL,
      @ConsoleParameter(name = "user", optional = true, description = "Server administrator name")
          String userName,
      @ConsoleParameter(
              name = "password",
              optional = true,
              description = "Server administrator password")
          String userPassword,
      @ConsoleParameter(
              name = "storage-type",
              optional = true,
              description =
                  "The type of the storage: 'plocal' for disk-based databases and 'memory' for in-memory database")
          String storageType,
      @ConsoleParameter(
              name = "db-type",
              optional = true,
              description =
                  "The type of the database used between 'document' and 'graph'. By default is graph.")
          String databaseType,
      @ConsoleParameter(
              name = "[options]",
              optional = true,
              description = "Additional options, example: -encryption=aes -compression=nothing")
          final String options)
      throws IOException {

    disconnect();

    if (userName == null) userName = OUser.ADMIN;
    if (userPassword == null) userPassword = OUser.ADMIN;

    currentDatabaseUserName = userName;
    currentDatabaseUserPassword = userPassword;
    final Map<String, String> omap = parseCommandOptions(options);

    urlConnection = OURLHelper.parseNew(databaseURL);
    OrientDBConfigBuilder config = OrientDBConfig.builder();

    for (Map.Entry<String, String> oentry : omap.entrySet()) {
      if ("-encryption".equalsIgnoreCase(oentry.getKey()))
        config.addConfig(OGlobalConfiguration.STORAGE_ENCRYPTION_METHOD, oentry.getValue());
      else if ("-compression".equalsIgnoreCase(oentry.getKey()))
        config.addConfig(OGlobalConfiguration.STORAGE_COMPRESSION_METHOD, oentry.getValue());
    }

    ODatabaseType type;
    if (storageType != null) {
      type = ODatabaseType.valueOf(storageType.toUpperCase());
    } else {
      type = urlConnection.getDbType().orElse(ODatabaseType.PLOCAL);
    }

    message("\nCreating database [" + databaseURL + "] using the storage type [" + type + "]...");
    String conn = urlConnection.getType() + ":" + urlConnection.getPath();
    if (orientDB != null) {
      OrientDBInternal contectSession = OrientDBInternal.extract(orientDB);
      String user = OrientDBInternal.extractUser(orientDB);
      if (!contectSession.getConnectionUrl().equals(conn)
          || user == null
          || !user.equals(userName)) {
        orientDB =
            new OrientDB(
                conn, currentDatabaseUserName, currentDatabaseUserPassword, config.build());
      }
    } else {
      orientDB =
          new OrientDB(conn, currentDatabaseUserName, currentDatabaseUserPassword, config.build());
    }

    final String backupPath = omap.remove("-restore");

    if (backupPath != null) {
      OrientDBInternal internal = OrientDBInternal.extract(orientDB);
      internal.restore(
          urlConnection.getDbName(),
          currentDatabaseUserName,
          currentDatabaseUserPassword,
          type,
          backupPath,
          config.build());
    } else {
      OrientDBInternal internal = OrientDBInternal.extract(orientDB);
      if (internal.isEmbedded()) {
        orientDB.execute(
            "create database ? " + type + " users (? identified by ? role admin) ",
            urlConnection.getDbName(),
            currentDatabaseUserName,
            currentDatabaseUserPassword);
      } else {
        orientDB.create(urlConnection.getDbName(), type);
      }
    }
    currentDatabase =
        (ODatabaseDocumentInternal)
            orientDB.open(urlConnection.getDbName(), userName, userPassword);
    currentDatabaseName = currentDatabase.getName();

    message("\nDatabase created successfully.");
    message("\n\nCurrent database is: " + databaseURL);
  }

  protected Map<String, String> parseCommandOptions(
      @ConsoleParameter(
              name = "[options]",
              optional = true,
              description = "Additional options, example: -encryption=aes -compression=nothing")
          String options) {
    final Map<String, String> omap = new HashMap<String, String>();
    if (options != null) {
      final List<String> kvOptions = OStringSerializerHelper.smartSplit(options, ',', false);
      for (String option : kvOptions) {
        final String[] values = option.split("=");
        if (values.length == 2) omap.put(values[0], values[1]);
        else omap.put(values[0], null);
      }
    }
    return omap;
  }

  @ConsoleCommand(
      description = "List all the databases available on the connected server",
      onlineHelp = "Console-Command-List-Databases")
  public void listDatabases() throws IOException {
    if (orientDB != null) {
      final List<String> databases = orientDB.list();
      message("\nFound %d databases:\n", databases.size());
      for (String database : databases) {
        message("\n* %s ", database);
      }
    } else {
      message(
          "\nNot connected to the Server instance. You've to connect to the Server using server's credentials (look at orientdb-*server-config.xml file)");
    }
    out.println();
  }

  @ConsoleCommand(
      description = "List all the active connections to the server",
      onlineHelp = "Console-Command-List-Connections")
  public void listConnections() throws IOException {
    checkForRemoteServer();
    OrientDBRemote remote = (OrientDBRemote) OrientDBInternal.extract(orientDB);
    final ODocument serverInfo =
        remote.getServerInfo(currentDatabaseUserName, currentDatabaseUserPassword);

    final List<OIdentifiable> resultSet = new ArrayList<OIdentifiable>();

    final List<Map<String, Object>> connections = serverInfo.field("connections");
    for (Map<String, Object> conn : connections) {
      final ODocument row = new ODocument();

      String commandDetail = (String) conn.get("commandInfo");

      if (commandDetail != null && ((String) conn.get("commandDetail")).length() > 1)
        commandDetail += " (" + conn.get("commandDetail") + ")";

      row.fields(
          "ID",
          conn.get("connectionId"),
          "REMOTE_ADDRESS",
          conn.get("remoteAddress"),
          "PROTOC",
          conn.get("protocol"),
          "LAST_OPERATION_ON",
          conn.get("lastCommandOn"),
          "DATABASE",
          conn.get("db"),
          "USER",
          conn.get("user"),
          "COMMAND",
          commandDetail,
          "TOT_REQS",
          conn.get("totalRequests"));
      resultSet.add(row);
    }

    Collections.sort(
        resultSet,
        new Comparator<OIdentifiable>() {
          @Override
          public int compare(final OIdentifiable o1, final OIdentifiable o2) {
            final String o1s = ((ODocument) o1).field("LAST_OPERATION_ON");
            final String o2s = ((ODocument) o2).field("LAST_OPERATION_ON");
            return o2s.compareTo(o1s);
          }
        });

    final OTableFormatter formatter = new OTableFormatter(this);
    formatter.setMaxWidthSize(getConsoleWidth());
    formatter.setMaxMultiValueEntries(getMaxMultiValueEntries());

    formatter.writeRecords(resultSet, -1);

    out.println();
  }

  @ConsoleCommand(description = "Reload the database schema")
  public void reloadSchema() throws IOException {
    message("\nreloading database schema...");
    updateDatabaseInfo();
    message("\n\nDone.");
  }

  @ConsoleCommand(
      splitInWords = false,
      description =
          "Create a new cluster in the current database. The cluster can be physical or memory")
  public void createCluster(
      @ConsoleParameter(name = "command-text", description = "The command text to execute")
          String iCommandText) {
    sqlCommand("create", iCommandText, "\nCluster created correctly in %.2f seconds\n", false);
    updateDatabaseInfo();
  }

  @ConsoleCommand(
      description =
          "Remove a cluster in the current database. The cluster can be physical or memory")
  public void dropCluster(
      @ConsoleParameter(
              name = "cluster-name",
              description = "The name or the id of the cluster to remove")
          String iClusterName) {
    checkForDatabase();

    message("\nDropping cluster [" + iClusterName + "] in database " + currentDatabaseName + "...");

    boolean result = currentDatabase.dropCluster(iClusterName);

    if (!result) {
      // TRY TO GET AS CLUSTER ID
      try {
        int clusterId = Integer.parseInt(iClusterName);
        if (clusterId > -1) {
          result = currentDatabase.dropCluster(clusterId);
        }
      } catch (Exception ignored) {
      }
    }

    if (result) message("\nCluster correctly removed");
    else message("\nCannot find the cluster to remove");
    updateDatabaseInfo();
  }

  @ConsoleCommand(
      splitInWords = false,
      description =
          "Alters a cluster in the current database. The cluster can be physical or memory")
  public void alterCluster(
      @ConsoleParameter(name = "command-text", description = "The command text to execute")
          String iCommandText) {
    sqlCommand("alter", iCommandText, "\nCluster updated successfully.\n", false);
    updateDatabaseInfo();
  }

  @ConsoleCommand(description = "Begins a transaction. All the changes will remain local")
  public void begin() throws IOException {
    checkForDatabase();

    if (currentDatabase.getTransaction().isActive()) {
      message(
          "\nError: an active transaction is currently open (id="
              + currentDatabase.getTransaction().getId()
              + "). Commit or rollback before starting a new one.");
      return;
    }

    if (currentDatabase.isRemote()) {
      message(
          "\nWARNING - Transactions are not supported from console in remote, please use an sql script: \neg.\n\nscript sql\nbegin;\n<your commands here>\ncommit;\nend\n\n");
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

    message(
        "\nTransaction "
            + txId
            + " has been committed in "
            + (System.currentTimeMillis() - begin)
            + "ms");
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
    message(
        "\nTransaction "
            + txId
            + " has been rollbacked in "
            + (System.currentTimeMillis() - begin)
            + "ms");
  }

  @ConsoleCommand(
      splitInWords = false,
      description = "Truncate the class content in the current database")
  public void truncateClass(
      @ConsoleParameter(name = "text", description = "The name of the class to truncate")
          String iCommandText) {
    sqlCommand("truncate", iCommandText, "\nClass truncated.\n", false);
  }

  @ConsoleCommand(
      splitInWords = false,
      description = "Truncate the cluster content in the current database")
  public void truncateCluster(
      @ConsoleParameter(name = "text", description = "The name of the class to truncate")
          String iCommandText) {
    sqlCommand("truncate", iCommandText, "\nTruncated %d record(s) in %f sec(s).\n", true);
  }

  @ConsoleCommand(splitInWords = false, description = "Truncate a record deleting it at low level")
  public void truncateRecord(
      @ConsoleParameter(name = "text", description = "The record(s) to truncate")
          String iCommandText) {
    sqlCommand("truncate", iCommandText, "\nTruncated %d record(s) in %f sec(s).\n", true);
  }

  @ConsoleCommand(description = "Load a record in memory using passed fetch plan")
  public void loadRecord(
      @ConsoleParameter(
              name = "record-id",
              description =
                  "The unique Record Id of the record to load. If you do not have the Record Id, execute a query first")
          String iRecordId,
      @ConsoleParameter(name = "fetch-plan", description = "The fetch plan to load the record with")
          String iFetchPlan) {
    loadRecordInternal(iRecordId, iFetchPlan);
  }

  @ConsoleCommand(description = "Load a record in memory and set it as the current")
  public void loadRecord(
      @ConsoleParameter(
              name = "record-id",
              description =
                  "The unique Record Id of the record to load. If you do not have the Record Id, execute a query first")
          String iRecordId) {
    loadRecordInternal(iRecordId, null);
  }

  @ConsoleCommand(description = "Reloads a record using passed fetch plan")
  public void reloadRecord(
      @ConsoleParameter(
              name = "record-id",
              description =
                  "The unique Record Id of the record to load. If you do not have the Record Id, execute a query first")
          String iRecordId,
      @ConsoleParameter(name = "fetch-plan", description = "The fetch plan to load the record with")
          String iFetchPlan) {
    reloadRecordInternal(iRecordId, iFetchPlan);
  }

  @ConsoleCommand(
      description = "Reload a record and set it as the current one",
      onlineHelp = "Console-Command-Reload-Record")
  public void reloadRecord(
      @ConsoleParameter(
              name = "record-id",
              description =
                  "The unique Record Id of the record to load. If you do not have the Record Id, execute a query first")
          String iRecordId) {
    reloadRecordInternal(iRecordId, null);
  }

  @ConsoleCommand(
      splitInWords = false,
      description = "Explain how a command is executed profiling it",
      onlineHelp = "SQL-Explain")
  public void explain(
      @ConsoleParameter(name = "command-text", description = "The command text to execute")
          String iCommandText) {
    Object result = sqlCommand("explain", iCommandText, "\n", false);
    if (result != null && result instanceof ODocument) {
      message(((ODocument) result).getProperty("executionPlanAsString"));
    } else if (result != null
        && result instanceof List
        && ((List) result).size() == 1
        && ((List) result).get(0) instanceof OResult) {
      message(((OResult) (((List) result).get(0))).getProperty("executionPlanAsString"));
    } else if (result != null
        && result instanceof List
        && ((List) result).size() == 1
        && ((List) result).get(0) instanceof ODocument) {
      message(((ODocument) (((List) result).get(0))).getProperty("executionPlanAsString"));
    }
  }

  @ConsoleCommand(splitInWords = false, description = "Executes a command inside a transaction")
  public void transactional(
      @ConsoleParameter(name = "command-text", description = "The command to execute")
          String iCommandText) {
    sqlCommand("transactional", iCommandText, "\nResult: '%s'. Executed in %f sec(s).\n", true);
  }

  @ConsoleCommand(
      splitInWords = false,
      description = "Insert a new record into the database",
      onlineHelp = "SQL-Insert")
  public void insert(
      @ConsoleParameter(name = "command-text", description = "The command text to execute")
          String iCommandText) {
    sqlCommand("insert", iCommandText, "\nInserted record '%s' in %f sec(s).\n", true);
  }

  @ConsoleCommand(
      splitInWords = false,
      description = "Create a new vertex into the database",
      onlineHelp = "SQL-Create-Vertex")
  public void createVertex(
      @ConsoleParameter(name = "command-text", description = "The command text to execute")
          String iCommandText) {
    sqlCommand("create", iCommandText, "\nCreated vertex '%s' in %f sec(s).\n", true);
  }

  @ConsoleCommand(
      splitInWords = false,
      description = "Create a new edge into the database",
      onlineHelp = "SQL-Create-Edge")
  public void createEdge(
      @ConsoleParameter(name = "command-text", description = "The command text to execute")
          String iCommandText) {

    String command = "create " + iCommandText;
    resetResultSet();
    final long start = System.currentTimeMillis();

    OResultSet rs = currentDatabase.command(command);
    final List<OIdentifiable> result =
        rs.stream().map(x -> x.toElement()).collect(Collectors.toList());
    rs.close();
    float elapsedSeconds = getElapsedSecs(start);

    setResultset((List<OIdentifiable>) result);

    int displayLimit = Integer.parseInt(properties.get(OConsoleProperties.LIMIT));

    dumpResultSet(displayLimit);

    message(
        "\nCreated '%s' edges in %f sec(s).\n",
        ((List<OIdentifiable>) result).size(), elapsedSeconds);
  }

  @ConsoleCommand(description = "Switches on storage profiling for upcoming set of commands")
  public void profileStorageOn() {
    sqlCommand("profile", " storage on", "\nProfiling of storage is switched on.\n", false);
  }

  @ConsoleCommand(
      description =
          "Switches off storage profiling for issued set of commands and "
              + "returns reslut of profiling.")
  public void profileStorageOff() {
    final Collection<ODocument> result =
        (Collection<ODocument>)
            sqlCommand(
                "profile", " storage off", "\nProfiling of storage is switched off\n", false);

    final String profilingWasNotSwitchedOn =
        "Can not retrieve results of profiling, probably profiling was not switched on";

    if (result == null) {
      message(profilingWasNotSwitchedOn);
      return;
    }

    final Iterator<ODocument> profilerIterator = result.iterator();

    if (profilerIterator.hasNext()) {
      final ODocument profilerDocument = profilerIterator.next();
      if (profilerDocument == null) message(profilingWasNotSwitchedOn);
      else message("Profiling result is : \n%s\n", profilerDocument.toJSON("prettyPrint"));
    } else {
      message(profilingWasNotSwitchedOn);
    }
  }

  @ConsoleCommand(
      splitInWords = false,
      description = "Update records in the database",
      onlineHelp = "SQL-Update")
  public void update(
      @ConsoleParameter(name = "command-text", description = "The command text to execute")
          String iCommandText) {
    sqlCommand("update", iCommandText, "\nUpdated record(s) '%s' in %f sec(s).\n", true);
    updateDatabaseInfo();
    currentDatabase.getLocalCache().invalidate();
  }

  @ConsoleCommand(
      splitInWords = false,
      description = "High Availability commands",
      onlineHelp = "SQL-HA")
  public void ha(
      @ConsoleParameter(name = "command-text", description = "The command text to execute")
          String iCommandText) {
    sqlCommand("ha", iCommandText, "\nExecuted '%s' in %f sec(s).\n", true);
  }

  @ConsoleCommand(
      splitInWords = false,
      description = "Move vertices to another position (class/cluster)",
      priority = 8,
      onlineHelp = "SQL-Move-Vertex")
  // EVALUATE THIS BEFORE 'MOVE'
  public void moveVertex(
      @ConsoleParameter(name = "command-text", description = "The command text to execute")
          String iCommandText) {
    sqlCommand(
        "move",
        iCommandText,
        "\nMove vertex command executed with result '%s' in %f sec(s).\n",
        true);
  }

  @ConsoleCommand(
      splitInWords = false,
      description = "Optimizes the current database",
      onlineHelp = "SQL-Optimize-Database")
  public void optimizeDatabase(
      @ConsoleParameter(name = "command-text", description = "The command text to execute")
          String iCommandText) {
    sqlCommand("optimize", iCommandText, "\nDatabase optimized '%s' in %f sec(s).\n", true);
  }

  @ConsoleCommand(description = "Force calling of JVM Garbage Collection")
  public void gc() {
    System.gc();
  }

  @ConsoleCommand(
      splitInWords = false,
      description = "Delete records from the database",
      onlineHelp = "SQL-Delete")
  public void delete(
      @ConsoleParameter(name = "command-text", description = "The command text to execute")
          String iCommandText) {
    sqlCommand("delete", iCommandText, "\nDelete record(s) '%s' in %f sec(s).\n", true);
    updateDatabaseInfo();
    currentDatabase.getLocalCache().invalidate();
  }

  @ConsoleCommand(
      splitInWords = false,
      description = "Grant privileges to a role",
      onlineHelp = "SQL-Grant")
  public void grant(
      @ConsoleParameter(name = "text", description = "Grant command") String iCommandText) {
    sqlCommand("grant", iCommandText, "\nPrivilege granted to the role: %s.\n", true);
  }

  @ConsoleCommand(
      splitInWords = false,
      description = "Revoke privileges to a role",
      onlineHelp = "SQL-Revoke")
  public void revoke(
      @ConsoleParameter(name = "text", description = "Revoke command") String iCommandText) {
    sqlCommand("revoke", iCommandText, "\nPrivilege revoked to the role: %s.\n", true);
  }

  @ConsoleCommand(
      splitInWords = false,
      description = "Create a link from a JOIN",
      onlineHelp = "SQL-Create-Link")
  public void createLink(
      @ConsoleParameter(name = "command-text", description = "The command text to execute")
          String iCommandText) {
    sqlCommand("create", iCommandText, "\nCreated %d link(s) in %f sec(s).\n", true);
  }

  @ConsoleCommand(
      splitInWords = false,
      description = "Find all references the target record id @rid",
      onlineHelp = "SQL-Find-References")
  public void findReferences(
      @ConsoleParameter(name = "command-text", description = "The command text to execute")
          String iCommandText) {
    sqlCommand("find", iCommandText, "\nFound %s in %f sec(s).\n", true);
  }

  @ConsoleCommand(
      splitInWords = false,
      description = "Alter a database property",
      onlineHelp = "SQL-Alter-Database")
  public void alterDatabase(
      @ConsoleParameter(name = "command-text", description = "The command text to execute")
          String iCommandText) {
    sqlCommand("alter", iCommandText, "\nDatabase updated successfully.\n", false);
    updateDatabaseInfo();
  }

  @ConsoleCommand(
      description = "Freeze database and flush on the disk",
      onlineHelp = "Console-Command-Freeze-Database")
  public void freezeDatabase(
      @ConsoleParameter(
              name = "storage-type",
              description = "Storage type of server database",
              optional = true)
          String storageType)
      throws IOException {
    checkForDatabase();

    final String dbName = currentDatabase.getName();

    if (currentDatabase.getURL().startsWith(OEngineRemote.NAME)) {
      if (storageType == null) storageType = "plocal";

      new OServerAdmin(currentDatabase.getURL())
          .connect(currentDatabaseUserName, currentDatabaseUserPassword)
          .freezeDatabase(storageType);
    } else {
      // LOCAL CONNECTION
      currentDatabase.freeze();
    }

    message("\n\nDatabase '" + dbName + "' was frozen successfully");
  }

  @ConsoleCommand(
      description = "Release database after freeze",
      onlineHelp = "Console-Command-Release-Db")
  public void releaseDatabase(
      @ConsoleParameter(
              name = "storage-type",
              description = "Storage type of server database",
              optional = true)
          String storageType)
      throws IOException {
    checkForDatabase();

    final String dbName = currentDatabase.getName();

    if (currentDatabase.getURL().startsWith(OEngineRemote.NAME)) {
      if (storageType == null) storageType = "plocal";

      new OServerAdmin(currentDatabase.getURL())
          .connect(currentDatabaseUserName, currentDatabaseUserPassword)
          .releaseDatabase(storageType);
    } else {
      // LOCAL CONNECTION
      currentDatabase.release();
    }

    message("\n\nDatabase '" + dbName + "' was released successfully");
  }

  @ConsoleCommand(description = "Flushes all database content to the disk")
  public void flushDatabase(
      @ConsoleParameter(
              name = "storage-type",
              description = "Storage type of server database",
              optional = true)
          String storageType)
      throws IOException {
    freezeDatabase(storageType);
    releaseDatabase(storageType);
  }

  @ConsoleCommand(description = "Display current record")
  public void current() {
    dumpRecordDetails();
  }

  @ConsoleCommand(description = "Move the current record stream to the next one in result set")
  public void next() {
    setCurrentRecord(currentRecordIdx + 1);
    dumpRecordDetails();
  }

  @ConsoleCommand(description = "Move the current record stream to the previous one in result set")
  public void prev() {
    setCurrentRecord(currentRecordIdx - 1);
    dumpRecordDetails();
  }

  @ConsoleCommand(splitInWords = false, description = "Alter a class in the database schema")
  public void alterClass(
      @ConsoleParameter(name = "command-text", description = "The command text to execute")
          String iCommandText) {
    sqlCommand("alter", iCommandText, "\nClass updated successfully.\n", false);
    updateDatabaseInfo();
  }

  @ConsoleCommand(
      splitInWords = false,
      description = "Create a class",
      onlineHelp = "SQL-Create-Class")
  public void createClass(
      @ConsoleParameter(name = "command-text", description = "The command text to execute")
          String iCommandText) {
    sqlCommand("create", iCommandText, "\nClass created successfully.\n", true);
    updateDatabaseInfo();
  }

  @ConsoleCommand(splitInWords = false, description = "Create a sequence in the database")
  public void createSequence(
      @ConsoleParameter(name = "command-text", description = "The command text to execute")
          String iCommandText) {
    sqlCommand("create", iCommandText, "\nSequence created successfully.\n", true);
    updateDatabaseInfo();
  }

  @ConsoleCommand(splitInWords = false, description = "Alter an existent sequence in the database")
  public void alterSequence(
      @ConsoleParameter(name = "command-text", description = "The command text to execute")
          String iCommandText) {
    sqlCommand("alter", iCommandText, "\nSequence altered successfully.\n", true);
    updateDatabaseInfo();
  }

  @ConsoleCommand(splitInWords = false, description = "Remove a sequence from the database")
  public void dropSequence(
      @ConsoleParameter(name = "command-text", description = "The command text to execute")
          String iCommandText) {
    sqlCommand("drop", iCommandText, "Sequence removed successfully.\n", false);
    updateDatabaseInfo();
  }

  @ConsoleCommand(
      splitInWords = false,
      description = "Create a user",
      onlineHelp = "SQL-Create-User")
  public void createUser(
      @ConsoleParameter(name = "command-text", description = "The command text to execute")
          String iCommandText) {
    sqlCommand("create", iCommandText, "\nUser created successfully.\n", false);
    updateDatabaseInfo();
  }

  @ConsoleCommand(splitInWords = false, description = "Drop a user", onlineHelp = "SQL-Drop-User")
  public void dropUser(
      @ConsoleParameter(name = "command-text", description = "The command text to execute")
          String iCommandText) {
    sqlCommand("drop", iCommandText, "\nUser dropped successfully.\n", false);
    updateDatabaseInfo();
  }

  @ConsoleCommand(
      splitInWords = false,
      description = "Alter a class property in the database schema",
      onlineHelp = "SQL-Alter-Property")
  public void alterProperty(
      @ConsoleParameter(name = "command-text", description = "The command text to execute")
          String iCommandText) {
    sqlCommand("alter", iCommandText, "\nProperty updated successfully.\n", false);
    updateDatabaseInfo();
  }

  @ConsoleCommand(
      splitInWords = false,
      description = "Create a property",
      onlineHelp = "SQL-Create-Property")
  public void createProperty(
      @ConsoleParameter(name = "command-text", description = "The command text to execute")
          String iCommandText) {
    sqlCommand("create", iCommandText, "\nProperty created successfully.\n", true);
    updateDatabaseInfo();
  }

  /**
   * * Creates a function.
   *
   * @param iCommandText the command text to execute
   * @author Claudio Tesoriero
   */
  @ConsoleCommand(
      splitInWords = false,
      description = "Create a stored function",
      onlineHelp = "SQL-Create-Function")
  public void createFunction(
      @ConsoleParameter(name = "command-text", description = "The command text to execute")
          String iCommandText) {
    sqlCommand("create", iCommandText, "\nFunction created successfully with id=%s.\n", true);
    updateDatabaseInfo();
  }

  @ConsoleCommand(
      splitInWords = false,
      description = "Traverse records and display the results",
      onlineHelp = "SQL-Traverse")
  public void traverse(
      @ConsoleParameter(name = "query-text", description = "The traverse to execute")
          String iQueryText) {
    final int limit;
    if (iQueryText.toLowerCase(Locale.ENGLISH).contains(" limit ")) {
      // RESET CONSOLE FLAG
      limit = -1;
    } else {
      limit = Integer.parseInt(properties.get(OConsoleProperties.LIMIT));
    }

    long start = System.currentTimeMillis();
    OResultSet rs = currentDatabase.command("traverse " + iQueryText);
    setResultset(rs.stream().map(x -> x.toElement()).collect(Collectors.toList()));
    rs.close();

    float elapsedSeconds = getElapsedSecs(start);

    dumpResultSet(limit);

    message(
        "\n\n"
            + currentResultSet.size()
            + " item(s) found. Traverse executed in "
            + elapsedSeconds
            + " sec(s).");
  }

  @ConsoleCommand(
      splitInWords = false,
      description = "Execute a query against the database and display the results",
      onlineHelp = "SQL-Query")
  public void select(
      @ConsoleParameter(name = "query-text", description = "The query to execute")
          String iQueryText) {
    checkForDatabase();

    if (iQueryText == null) return;

    iQueryText = iQueryText.trim();

    if (iQueryText.length() == 0 || iQueryText.equalsIgnoreCase("select")) return;

    iQueryText = "select " + iQueryText;

    final int queryLimit;
    final int displayLimit;
    if (iQueryText.toLowerCase(Locale.ENGLISH).contains(" limit ")) {
      queryLimit = -1;
      displayLimit = -1;
    } else {
      // USE LIMIT + 1 TO DISCOVER IF MORE ITEMS ARE PRESENT
      displayLimit = Integer.parseInt(properties.get(OConsoleProperties.LIMIT));
      if (displayLimit > 0) {
        queryLimit = displayLimit + 1;
      } else {
        queryLimit = -1;
      }
    }

    final long start = System.currentTimeMillis();
    List<OIdentifiable> result = new ArrayList<>();
    try (OResultSet rs = currentDatabase.query(iQueryText)) {
      int count = 0;
      while (rs.hasNext() && (queryLimit < 0 || count < queryLimit)) {
        OResult item = rs.next();
        if (item.isBlob()) {
          result.add(item.getBlob().get());
        } else {
          result.add(item.toElement());
        }
      }
    }
    setResultset(result);

    float elapsedSeconds = getElapsedSecs(start);

    dumpResultSet(displayLimit);

    long tot =
        displayLimit > -1
            ? Math.min(currentResultSet.size(), displayLimit)
            : currentResultSet.size();
    message("\n\n" + tot + " item(s) found. Query executed in " + elapsedSeconds + " sec(s).");
  }

  @ConsoleCommand(
      splitInWords = false,
      description = "Execute a MATCH query against the database and display the results",
      onlineHelp = "SQL-Match")
  public void match(
      @ConsoleParameter(name = "query-text", description = "The query to execute")
          String iQueryText) {
    checkForDatabase();

    if (iQueryText == null) return;

    iQueryText = iQueryText.trim();

    if (iQueryText.length() == 0 || iQueryText.equalsIgnoreCase("match")) return;

    iQueryText = "match " + iQueryText;

    final int queryLimit;
    final int displayLimit;
    if (iQueryText.toLowerCase(Locale.ENGLISH).contains(" limit ")) {
      queryLimit = -1;
      displayLimit = -1;
    } else {
      // USE LIMIT + 1 TO DISCOVER IF MORE ITEMS ARE PRESENT
      displayLimit = Integer.parseInt(properties.get(OConsoleProperties.LIMIT));
      queryLimit = displayLimit + 1;
    }

    final long start = System.currentTimeMillis();
    List<OIdentifiable> result = new ArrayList<>();
    OResultSet rs = currentDatabase.query(iQueryText);
    int count = 0;
    while (rs.hasNext() && (queryLimit < 0 || count < queryLimit)) {
      result.add(rs.next().toElement());
    }
    rs.close();
    setResultset(result);

    float elapsedSeconds = getElapsedSecs(start);

    dumpResultSet(displayLimit);

    long tot =
        displayLimit > -1
            ? Math.min(currentResultSet.size(), displayLimit)
            : currentResultSet.size();
    message("\n\n" + tot + " item(s) found. Query executed in " + elapsedSeconds + " sec(s).");
  }

  @ConsoleCommand(
      splitInWords = false,
      description = "Move from current record by evaluating a predicate against current record")
  public void move(
      @ConsoleParameter(name = "text", description = "The sql predicate to evaluate")
          final String iText) {
    if (iText == null) return;

    if (currentRecord == null) return;

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
        while (((Iterator) result).hasNext()) list.add(((Iterator<OIdentifiable>) result).next());
        setResultset(list);
        dumpResultSet(-1);
      } else setResultset(new ArrayList<OIdentifiable>());
    }
  }

  @ConsoleCommand(splitInWords = false, description = "Evaluate a predicate against current record")
  public void eval(
      @ConsoleParameter(name = "text", description = "The sql predicate to evaluate")
          final String iText) {
    if (iText == null) return;

    if (currentRecord == null) return;

    final Object result = new OSQLPredicate(iText).evaluate(currentRecord, null, null);
    if (result != null) out.println("\n" + result);
  }

  @SuppressWarnings("unchecked")
  @ConsoleCommand(
      splitInWords = false,
      description = "Execute a script containing multiple commands separated by ; or new line")
  public void script(
      @ConsoleParameter(name = "text", description = "Commands to execute, one per line")
          String iText) {
    final String language;
    final int languageEndPos = iText.indexOf(";");
    String[] splitted = iText.split(" ")[0].split(";")[0].split("\n")[0].split("\t");
    language = splitted[0];
    iText = iText.substring(language.length() + 1);
    if (iText.trim().length() == 0) {
      throw new IllegalArgumentException(
          "Missing language in script (sql, js, gremlin, etc.) as first argument");
    }

    executeServerSideScript(language, iText);
  }

  @SuppressWarnings("unchecked")
  @ConsoleCommand(splitInWords = false, description = "Execute javascript commands in the console")
  public void js(
      @ConsoleParameter(
              name = "text",
              description =
                  "The javascript to execute. Use 'db' to reference to a document database, 'gdb' for a graph database")
          final String iText) {
    if (iText == null) return;

    resetResultSet();

    long start = System.currentTimeMillis();
    while (true) {
      try {
        final OCommandExecutorScript cmd = new OCommandExecutorScript();
        cmd.parse(new OCommandScript("Javascript", iText));

        currentResult = cmd.execute(null);
        break;
      } catch (ORetryQueryException e) {
        continue;
      }
    }
    float elapsedSeconds = getElapsedSecs(start);

    parseResult();

    if (currentResultSet != null) {
      dumpResultSet(-1);
      message(
          "\nClient side script executed in %f sec(s). Returned %d records",
          elapsedSeconds, currentResultSet.size());
    } else
      message(
          "\nClient side script executed in %f sec(s). Value returned is: %s",
          elapsedSeconds, currentResult);
  }

  @SuppressWarnings("unchecked")
  @ConsoleCommand(
      splitInWords = false,
      description = "Execute javascript commands against a remote server")
  public void jss(
      @ConsoleParameter(
              name = "text",
              description =
                  "The javascript to execute. Use 'db' to reference to a document database, 'gdb' for a graph database")
          final String iText) {
    checkForRemoteServer();

    executeServerSideScript("javascript", iText);
  }

  @SuppressWarnings("unchecked")
  @ConsoleCommand(
      description =
          "Set a server user. If the user already exists, the password and permissions are updated. For more information look at http://orientdb.com/docs/last/Security.html#orientdb-server-security",
      onlineHelp = "Console-Command-Set-Server-User")
  public void setServerUser(
      @ConsoleParameter(name = "user-name", description = "User name") String iServerUserName,
      @ConsoleParameter(name = "user-password", description = "User password")
          String iServerUserPasswd,
      @ConsoleParameter(
              name = "user-permissions",
              description =
                  "Permissions, look at http://orientdb.com/docs/last/Security.html#servers-resources")
          String iPermissions) {

    if (iServerUserName == null || iServerUserName.length() == 0)
      throw new IllegalArgumentException("User name null or empty");

    if (iPermissions == null || iPermissions.length() == 0)
      throw new IllegalArgumentException("User permissions null or empty");

    final File serverCfgFile = new File("../config/orientdb-server-config.xml");
    if (!serverCfgFile.exists())
      throw new OConfigurationException("Cannot access to file " + serverCfgFile);

    try {
      final OServerConfigurationManager serverCfg = new OServerConfigurationManager(serverCfgFile);

      final String defAlgo =
          OGlobalConfiguration.SECURITY_USER_PASSWORD_DEFAULT_ALGORITHM.getValueAsString();

      final String hashedPassword = OSecurityManager.createHash(iServerUserPasswd, defAlgo, true);

      serverCfg.setUser(iServerUserName, hashedPassword, iPermissions);
      serverCfg.saveConfiguration();

      message("\nServer user '%s' set correctly", iServerUserName);

    } catch (Exception e) {
      error("\nError on loading %s file: %s", serverCfgFile, e.toString());
    }
  }

  @SuppressWarnings("unchecked")
  @ConsoleCommand(
      description =
          "Drop a server user. For more information look at http://orientdb.com/docs/last/Security.html#orientdb-server-security",
      onlineHelp = "Console-Command-Drop-Server-User")
  public void dropServerUser(
      @ConsoleParameter(name = "user-name", description = "User name") String iServerUserName) {

    if (iServerUserName == null || iServerUserName.length() == 0)
      throw new IllegalArgumentException("User name null or empty");

    final File serverCfgFile = new File("../config/orientdb-server-config.xml");
    if (!serverCfgFile.exists())
      throw new OConfigurationException("Cannot access to file " + serverCfgFile);

    try {
      final OServerConfigurationManager serverCfg = new OServerConfigurationManager(serverCfgFile);

      if (!serverCfg.existsUser(iServerUserName)) {
        error("\nServer user '%s' not found in configuration", iServerUserName);
        return;
      }

      serverCfg.dropUser(iServerUserName);
      serverCfg.saveConfiguration();

      message("\nServer user '%s' dropped correctly", iServerUserName);

    } catch (Exception e) {
      error("\nError on loading %s file: %s", serverCfgFile, e.toString());
    }
  }

  @SuppressWarnings("unchecked")
  @ConsoleCommand(
      description =
          "Display all the server user names. For more information look at http://orientdb.com/docs/last/Security.html#orientdb-server-security",
      onlineHelp = "Console-Command-List-Server-User")
  public void listServerUsers() {

    final File serverCfgFile = new File("../config/orientdb-server-config.xml");
    if (!serverCfgFile.exists())
      throw new OConfigurationException("Cannot access to file " + serverCfgFile);

    try {
      final OServerConfigurationManager serverCfg = new OServerConfigurationManager(serverCfgFile);

      message("\nSERVER USERS\n");
      final Set<OServerUserConfiguration> users = serverCfg.getUsers();
      if (users.isEmpty()) message("\nNo users found");
      else
        for (OServerUserConfiguration u : users) {
          message("\n- '%s', permissions: %s", u.name, u.resources);
        }

    } catch (Exception e) {
      error("\nError on loading %s file: %s", serverCfgFile, e.toString());
    }
  }

  @ConsoleCommand(
      splitInWords = false,
      description = "Create an index against a property",
      onlineHelp = "SQL-Create-Index")
  public void createIndex(
      @ConsoleParameter(name = "command-text", description = "The command text to execute")
          String iCommandText)
      throws IOException {
    message("\n\nCreating index...");

    sqlCommand("create", iCommandText, "\nCreated index successfully in %f sec(s).\n", false);
    updateDatabaseInfo();
    message("\n\nIndex created successfully");
  }

  @ConsoleCommand(
      description = "Delete the current database",
      onlineHelp = "Console-Command-Drop-Database")
  public void dropDatabase(
      @ConsoleParameter(
              name = "storage-type",
              description = "Storage type of server database",
              optional = true)
          String storageType)
      throws IOException {
    checkForDatabase();

    final String dbName = currentDatabase.getName();
    currentDatabase.close();
    if (storageType != null
        && !"plocal".equalsIgnoreCase(storageType)
        && !"local".equalsIgnoreCase(storageType)
        && !"memory".equalsIgnoreCase(storageType)) {
      message("\n\nInvalid storage type for db: '" + storageType + "'");
      return;
    }
    orientDB.drop(dbName);
    currentDatabase = null;
    currentDatabaseName = null;
    message("\n\nDatabase '" + dbName + "' deleted successfully");
  }

  @ConsoleCommand(
      description = "Delete the specified database",
      onlineHelp = "Console-Command-Drop-Database")
  public void dropDatabase(
      @ConsoleParameter(
              name = "database-url",
              description = "The url of the database to drop in the format '<mode>:<path>'")
          String iDatabaseURL,
      @ConsoleParameter(name = "user", description = "Server administrator name") String iUserName,
      @ConsoleParameter(name = "password", description = "Server administrator password")
          String iUserPassword,
      @ConsoleParameter(
              name = "storage-type",
              description = "Storage type of server database",
              optional = true)
          String storageType)
      throws IOException {

    connect(iDatabaseURL, iUserName, iUserPassword);
    dropDatabase(null);
  }

  @ConsoleCommand(
      splitInWords = false,
      description = "Remove an index",
      onlineHelp = "SQL-Drop-Index")
  public void dropIndex(
      @ConsoleParameter(name = "command-text", description = "The command text to execute")
          String iCommandText)
      throws IOException {
    message("\n\nRemoving index...");

    sqlCommand("drop", iCommandText, "\nDropped index in %f sec(s).\n", false);
    updateDatabaseInfo();
    message("\n\nIndex removed successfully");
  }

  @ConsoleCommand(
      splitInWords = false,
      description = "Rebuild an index if it is automatic",
      onlineHelp = "SQL-Rebuild-Index")
  public void rebuildIndex(
      @ConsoleParameter(name = "command-text", description = "The command text to execute")
          String iCommandText)
      throws IOException {
    message("\n\nRebuilding index(es)...");

    sqlCommand(
        "rebuild", iCommandText, "\nRebuilt index(es). Found %d link(s) in %f sec(s).\n", true);
    updateDatabaseInfo();
    message("\n\nIndex(es) rebuilt successfully");
  }

  @ConsoleCommand(
      splitInWords = false,
      description = "Remove a class from the schema",
      onlineHelp = "SQL-Drop-Class")
  public void dropClass(
      @ConsoleParameter(name = "command-text", description = "The command text to execute")
          String iCommandText)
      throws IOException {
    sqlCommand("drop", iCommandText, "\nRemoved class in %f sec(s).\n", false);
    updateDatabaseInfo();
  }

  @ConsoleCommand(
      splitInWords = false,
      description = "Remove a property from a class",
      onlineHelp = "SQL-Drop-Property")
  public void dropProperty(
      @ConsoleParameter(name = "command-text", description = "The command text to execute")
          String iCommandText)
      throws IOException {
    sqlCommand("drop", iCommandText, "\nRemoved class property in %f sec(s).\n", false);
    updateDatabaseInfo();
  }

  @ConsoleCommand(
      description = "Browse all records of a class",
      onlineHelp = "Console-Command-Browse-Class")
  public void browseClass(
      @ConsoleParameter(name = "class-name", description = "The name of the class")
          final String iClassName) {
    checkForDatabase();

    resetResultSet();

    final OIdentifiableIterator<?> it = currentDatabase.browseClass(iClassName);

    browseRecords(it);
  }

  @ConsoleCommand(
      description = "Browse all records of a cluster",
      onlineHelp = "Console-Command-Browse-Cluster")
  public void browseCluster(
      @ConsoleParameter(name = "cluster-name", description = "The name of the cluster")
          final String iClusterName) {
    checkForDatabase();

    resetResultSet();

    final ORecordIteratorCluster<?> it = currentDatabase.browseCluster(iClusterName);

    browseRecords(it);
  }

  @ConsoleCommand(
      aliases = {"display"},
      description = "Display current record attributes",
      onlineHelp = "Console-Command-Display-Record")
  public void displayRecord(
      @ConsoleParameter(
              name = "number",
              description = "The number of the record in the most recent result set")
          final String iRecordNumber) {
    checkForDatabase();

    if (iRecordNumber == null || currentResultSet == null) checkCurrentObject();
    else {
      int recNumber = Integer.parseInt(iRecordNumber);
      if (currentResultSet.size() == 0)
        throw new OSystemException(
            "No result set where to find the requested record. Execute a query first.");

      if (currentResultSet.size() <= recNumber) {
        String resultSize = currentResultSet.size() > 0 ? "-" + (currentResultSet.size() - 1) : "";
        throw new OSystemException(
            "The record requested is not part of current result set (0" + resultSize + ")");
      }

      setCurrentRecord(recNumber);
    }

    dumpRecordDetails();
  }

  @ConsoleCommand(
      aliases = {"status"},
      description = "Display information about the database",
      onlineHelp = "Console-Command-Info")
  public void info() {
    if (currentDatabaseName != null) {
      message(
          "\nCurrent database: " + currentDatabaseName + " (url=" + currentDatabase.getURL() + ")");

      currentDatabase.getMetadata().reload();

      if (currentDatabase.isRemote()) {
        listServers();
      }

      listProperties();
      listClusters(null);
      listClasses();
      listIndexes();
    }
  }

  @ConsoleCommand(description = "Display the database properties")
  public void listProperties() {
    if (currentDatabase == null) return;

    final OStorageConfiguration dbCfg = currentDatabase.getStorageInfo().getConfiguration();

    message("\n\nDATABASE PROPERTIES");

    if (dbCfg.getProperties() != null) {
      final List<ODocument> resultSet = new ArrayList<ODocument>();

      if (dbCfg.getName() != null)
        resultSet.add(new ODocument().field("NAME", "Name").field("VALUE", dbCfg.getName()));

      resultSet.add(new ODocument().field("NAME", "Version").field("VALUE", dbCfg.getVersion()));
      resultSet.add(
          new ODocument()
              .field("NAME", "Conflict-Strategy")
              .field("VALUE", dbCfg.getConflictStrategy()));
      resultSet.add(
          new ODocument().field("NAME", "Date-Format").field("VALUE", dbCfg.getDateFormat()));
      resultSet.add(
          new ODocument()
              .field("NAME", "Datetime-Format")
              .field("VALUE", dbCfg.getDateTimeFormat()));
      resultSet.add(
          new ODocument().field("NAME", "Timezone").field("VALUE", dbCfg.getTimeZone().getID()));
      resultSet.add(
          new ODocument().field("NAME", "Locale-Country").field("VALUE", dbCfg.getLocaleCountry()));
      resultSet.add(
          new ODocument()
              .field("NAME", "Locale-Language")
              .field("VALUE", dbCfg.getLocaleLanguage()));
      resultSet.add(new ODocument().field("NAME", "Charset").field("VALUE", dbCfg.getCharset()));
      resultSet.add(
          new ODocument()
              .field("NAME", "Schema-RID")
              .field("VALUE", dbCfg.getSchemaRecordId(), OType.LINK));
      resultSet.add(
          new ODocument()
              .field("NAME", "Index-Manager-RID")
              .field("VALUE", dbCfg.getIndexMgrRecordId(), OType.LINK));

      final OTableFormatter formatter = new OTableFormatter(this);
      formatter.setMaxWidthSize(getConsoleWidth());
      formatter.setMaxMultiValueEntries(getMaxMultiValueEntries());

      formatter.writeRecords(resultSet, -1);

      message("\n");

      if (!dbCfg.getProperties().isEmpty()) {
        message("\n\nDATABASE CUSTOM PROPERTIES:");

        final List<ODocument> dbResultSet = new ArrayList<ODocument>();

        for (OStorageEntryConfiguration cfg : dbCfg.getProperties())
          dbResultSet.add(new ODocument().field("NAME", cfg.name).field("VALUE", cfg.value));

        final OTableFormatter dbFormatter = new OTableFormatter(this);
        formatter.setMaxWidthSize(getConsoleWidth());
        formatter.setMaxMultiValueEntries(getMaxMultiValueEntries());

        dbFormatter.writeRecords(dbResultSet, -1);
      }
    }
  }

  @ConsoleCommand(
      aliases = {"desc"},
      description = "Display a class in the schema",
      onlineHelp = "Console-Command-Info-Class")
  public void infoClass(
      @ConsoleParameter(name = "class-name", description = "The name of the class")
          final String iClassName) {
    checkForDatabase();

    currentDatabase.getMetadata().reload();

    final OClass cls =
        currentDatabase.getMetadata().getImmutableSchemaSnapshot().getClass(iClassName);

    if (cls == null) {
      message(
          "\n! Class '"
              + iClassName
              + "' does not exist in the database '"
              + currentDatabaseName
              + "'");
      return;
    }

    message("\nCLASS '" + cls.getName() + "'\n");

    final long count = currentDatabase.countClass(cls.getName(), false);
    message("\nRecords..............: " + count);

    if (cls.getShortName() != null) message("\nAlias................: " + cls.getShortName());
    if (cls.hasSuperClasses())
      message("\nSuper classes........: " + Arrays.toString(cls.getSuperClassesNames().toArray()));

    message(
        "\nDefault cluster......: "
            + currentDatabase.getClusterNameById(cls.getDefaultClusterId())
            + " (id="
            + cls.getDefaultClusterId()
            + ")");

    final StringBuilder clusters = new StringBuilder();
    for (int clId : cls.getClusterIds()) {
      if (clusters.length() > 0) clusters.append(", ");

      clusters.append(currentDatabase.getClusterNameById(clId));
      clusters.append("(");
      clusters.append(clId);
      clusters.append(")");
    }

    message("\nSupported clusters...: " + clusters.toString());
    message("\nCluster selection....: " + cls.getClusterSelection().getName());
    message("\nOversize.............: " + cls.getClassOverSize());

    if (!cls.getSubclasses().isEmpty()) {
      message("\nSubclasses.........: ");
      int i = 0;
      for (OClass c : cls.getSubclasses()) {
        if (i > 0) message(", ");
        message(c.getName());
        ++i;
      }
      out.println();
    }

    if (cls.properties().size() > 0) {
      message("\n\nPROPERTIES");

      final List<ODocument> resultSet = new ArrayList<ODocument>();

      for (final OProperty p : cls.properties()) {
        try {
          final ODocument row = new ODocument();
          resultSet.add(row);

          row.field("NAME", p.getName());
          row.field("TYPE", (Object) p.getType());
          row.field(
              "LINKED-TYPE/CLASS",
              p.getLinkedClass() != null ? p.getLinkedClass() : p.getLinkedType());
          row.field("MANDATORY", p.isMandatory());
          row.field("READONLY", p.isReadonly());
          row.field("NOT-NULL", p.isNotNull());
          row.field("MIN", p.getMin() != null ? p.getMin() : "");
          row.field("MAX", p.getMax() != null ? p.getMax() : "");
          row.field("COLLATE", p.getCollate() != null ? p.getCollate().getName() : "");
          row.field("DEFAULT", p.getDefaultValue() != null ? p.getDefaultValue() : "");

        } catch (Exception ignored) {
        }
      }

      final OTableFormatter formatter = new OTableFormatter(this);
      formatter.setMaxWidthSize(getConsoleWidth());
      formatter.setMaxMultiValueEntries(getMaxMultiValueEntries());

      formatter.writeRecords(resultSet, -1);
    }

    final Set<OIndex> indexes = cls.getClassIndexes();
    if (!indexes.isEmpty()) {
      message("\n\nINDEXES (" + indexes.size() + " altogether)");

      final List<ODocument> resultSet = new ArrayList<ODocument>();

      for (final OIndex index : indexes) {
        final ODocument row = new ODocument();
        resultSet.add(row);

        row.field("NAME", index.getName());

        final OIndexDefinition indexDefinition = index.getDefinition();
        if (indexDefinition != null) {
          final List<String> fields = indexDefinition.getFields();
          row.field("PROPERTIES", fields);
        }
      }

      final OTableFormatter formatter = new OTableFormatter(this);
      formatter.setMaxWidthSize(getConsoleWidth());
      formatter.setMaxMultiValueEntries(getMaxMultiValueEntries());

      formatter.writeRecords(resultSet, -1);
    }

    if (cls.getCustomKeys().size() > 0) {
      message("\n\nCUSTOM ATTRIBUTES");

      final List<ODocument> resultSet = new ArrayList<ODocument>();

      for (final String k : cls.getCustomKeys()) {
        try {
          final ODocument row = new ODocument();
          resultSet.add(row);

          row.field("NAME", k);
          row.field("VALUE", cls.getCustom(k));

        } catch (Exception ignored) {
          // IGNORED
        }
      }

      final OTableFormatter formatter = new OTableFormatter(this);
      formatter.setMaxWidthSize(getConsoleWidth());
      formatter.setMaxMultiValueEntries(getMaxMultiValueEntries());

      formatter.writeRecords(resultSet, -1);
    }
  }

  @ConsoleCommand(
      description = "Display a class property",
      onlineHelp = "Console-Command-Info-Property")
  public void infoProperty(
      @ConsoleParameter(
              name = "property-name",
              description = "The name of the property as <class>.<property>")
          final String iPropertyName) {
    checkForDatabase();

    if (iPropertyName.indexOf('.') == -1)
      throw new OSystemException("Property name is in the format <class>.<property>");

    final String[] parts = iPropertyName.split("\\.");

    final OClass cls =
        currentDatabase.getMetadata().getImmutableSchemaSnapshot().getClass(parts[0]);

    if (cls == null) {
      message(
          "\n! Class '"
              + parts[0]
              + "' does not exist in the database '"
              + currentDatabaseName
              + "'");
      return;
    }

    final OProperty prop = cls.getProperty(parts[1]);

    if (prop == null) {
      message("\n! Property '" + parts[1] + "' does not exist in class '" + parts[0] + "'");
      return;
    }

    message("\nPROPERTY '" + prop.getFullName() + "'\n");
    message("\nType.................: " + prop.getType());
    message("\nMandatory............: " + prop.isMandatory());
    message("\nNot null.............: " + prop.isNotNull());
    message("\nRead only............: " + prop.isReadonly());
    message("\nDefault value........: " + prop.getDefaultValue());
    message("\nMinimum value........: " + prop.getMin());
    message("\nMaximum value........: " + prop.getMax());
    message("\nREGEXP...............: " + prop.getRegexp());
    message("\nCollate..............: " + prop.getCollate());
    message("\nLinked class.........: " + prop.getLinkedClass());
    message("\nLinked type..........: " + prop.getLinkedType());

    if (prop.getCustomKeys().size() > 0) {
      message("\n\nCUSTOM ATTRIBUTES");

      final List<ODocument> resultSet = new ArrayList<ODocument>();

      for (final String k : prop.getCustomKeys()) {
        try {
          final ODocument row = new ODocument();
          resultSet.add(row);

          row.field("NAME", k);
          row.field("VALUE", prop.getCustom(k));

        } catch (Exception ignored) {
        }
      }

      final OTableFormatter formatter = new OTableFormatter(this);
      formatter.setMaxWidthSize(getConsoleWidth());
      formatter.setMaxMultiValueEntries(getMaxMultiValueEntries());

      formatter.writeRecords(resultSet, -1);
    }

    final Collection<OIndex> indexes = prop.getAllIndexes();
    if (!indexes.isEmpty()) {
      message("\n\nINDEXES (" + indexes.size() + " altogether)");

      final List<ODocument> resultSet = new ArrayList<ODocument>();

      for (final OIndex index : indexes) {
        final ODocument row = new ODocument();
        resultSet.add(row);

        row.field("NAME", index.getName());

        final OIndexDefinition indexDefinition = index.getDefinition();
        if (indexDefinition != null) {
          final List<String> fields = indexDefinition.getFields();
          row.field("PROPERTIES", fields);
        }
      }
      final OTableFormatter formatter = new OTableFormatter(this);
      formatter.setMaxWidthSize(getConsoleWidth());
      formatter.setMaxMultiValueEntries(getMaxMultiValueEntries());

      formatter.writeRecords(resultSet, -1);
    }
  }

  @ConsoleCommand(
      description = "Display all indexes",
      aliases = {"indexes"},
      onlineHelp = "Console-Command-List-Indexes")
  public void listIndexes() {
    if (currentDatabaseName != null) {
      message("\n\nINDEXES");

      final List<ODocument> resultSet = new ArrayList<>();

      int totalIndexes = 0;
      long totalRecords = 0;

      final List<OIndex> indexes =
          new ArrayList<OIndex>(
              currentDatabase.getMetadata().getIndexManagerInternal().getIndexes(currentDatabase));
      indexes.sort((o1, o2) -> o1.getName().compareToIgnoreCase(o2.getName()));

      long totalIndexedRecords = 0;

      for (final OIndex index : indexes) {
        final ODocument row = new ODocument();
        resultSet.add(row);

        final long indexSize = index.getSize(); // getInternal doesn't work in remote...
        totalIndexedRecords += indexSize;

        row.field("NAME", index.getName());
        row.field("TYPE", index.getType());
        row.field("RECORDS", indexSize);

        try {
          final OIndexDefinition indexDefinition = index.getDefinition();
          final long size = index.getInternal().size();
          if (indexDefinition != null) {
            row.field("CLASS", indexDefinition.getClassName());
            row.field("COLLATE", indexDefinition.getCollate().getName());

            final List<String> fields = indexDefinition.getFields();
            final StringBuilder buffer = new StringBuilder();
            for (int i = 0; i < fields.size(); ++i) {
              if (buffer.length() > 0) buffer.append(",");

              buffer.append(fields.get(i));
              buffer.append("(");
              buffer.append(indexDefinition.getTypes()[i]);
              buffer.append(")");
            }

            row.field("FIELDS", buffer.toString());
          }

          totalIndexes++;
          totalRecords += size;
        } catch (Exception ignored) {
        }
      }

      final OTableFormatter formatter = new OTableFormatter(this);
      formatter.setMaxWidthSize(getConsoleWidth());
      formatter.setMaxMultiValueEntries(getMaxMultiValueEntries());

      formatter.setColumnAlignment("RECORDS", OTableFormatter.ALIGNMENT.RIGHT);

      final ODocument footer = new ODocument();
      footer.field("NAME", "TOTAL");
      footer.field("RECORDS", totalIndexedRecords);
      formatter.setFooter(footer);

      formatter.writeRecords(resultSet, -1);

    } else message("\nNo database selected yet.");
  }

  @ConsoleCommand(
      description = "Display all the configured clusters",
      aliases = {"clusters"},
      onlineHelp = "Console-Command-List-Clusters")
  public void listClusters(
      @ConsoleParameter(
              name = "[options]",
              optional = true,
              description = "Additional options, example: -v=verbose")
          final String options) {
    final Map<String, String> commandOptions = parseCommandOptions(options);

    if (currentDatabaseName != null) {
      message("\n\nCLUSTERS (collections)");

      final List<ODocument> resultSet = new ArrayList<ODocument>();

      int clusterId;
      long totalElements = 0;
      long totalSpaceUsed = 0;
      long totalTombstones = 0;
      long count;

      final List<String> clusters = new ArrayList<String>(currentDatabase.getClusterNames());
      Collections.sort(clusters);

      ODocument dClusters = null;
      final ODocument dCfg = getDistributedConfiguration();
      if (dCfg != null) {
        final ODocument dDatabaseCfg = dCfg.field("database");
        if (dDatabaseCfg != null) {
          dClusters = dDatabaseCfg.field("clusters");
        }
      }

      final boolean isRemote = currentDatabase.isRemote();

      for (String clusterName : clusters) {
        try {
          final ODocument row = new ODocument();
          resultSet.add(row);

          clusterId = currentDatabase.getClusterIdByName(clusterName);

          final String conflictStrategy =
              Optional.ofNullable(currentDatabase.getClusterRecordConflictStrategy(clusterId))
                  .orElse("");

          count = currentDatabase.countClusterElements(clusterName);
          totalElements += count;

          final OClass cls =
              currentDatabase
                  .getMetadata()
                  .getImmutableSchemaSnapshot()
                  .getClassByClusterId(clusterId);
          final String className = Optional.ofNullable(cls).map(OClass::getName).orElse(null);

          row.field("NAME", clusterName);
          row.field("ID", clusterId);
          row.field("CLASS", className);
          if (!currentDatabase.isRemote()) {
            row.field("CONFLICT-STRATEGY", conflictStrategy);
          }
          row.field("COUNT", count);

          if (dClusters != null) {
            ODocument dClusterCfg = dClusters.field(clusterName);
            if (dClusterCfg == null) dClusterCfg = dClusters.field("*");

            if (dClusterCfg != null) {
              final List<String> servers = new ArrayList<String>(dClusterCfg.field("servers"));
              final boolean newNode = servers.remove("<NEW_NODE>");
              if (!servers.isEmpty()) {
                row.field("OWNER_SERVER", servers.get(0));

                if (servers.size() > 1) {
                  servers.remove(0);

                  row.field("OTHER_SERVERS", servers);
                }
              }
              row.field("AUTO_DEPLOY_NEW_NODE", newNode);
            }
          }

        } catch (Exception e) {
          if (e instanceof OIOException) break;
        }
      }

      final OTableFormatter formatter = new OTableFormatter(this);
      formatter.setMaxWidthSize(getConsoleWidth());
      formatter.setMaxMultiValueEntries(getMaxMultiValueEntries());

      formatter.setColumnAlignment("ID", OTableFormatter.ALIGNMENT.RIGHT);
      formatter.setColumnAlignment("COUNT", OTableFormatter.ALIGNMENT.RIGHT);
      formatter.setColumnAlignment("OWNER_SERVER", OTableFormatter.ALIGNMENT.CENTER);
      formatter.setColumnAlignment("OTHER_SERVERS", OTableFormatter.ALIGNMENT.CENTER);
      formatter.setColumnAlignment("AUTO_DEPLOY_NEW_NODE", OTableFormatter.ALIGNMENT.CENTER);
      if (!isRemote) {
        formatter.setColumnAlignment("SPACE-USED", OTableFormatter.ALIGNMENT.RIGHT);
        if (commandOptions.containsKey("-v")) {
          formatter.setColumnAlignment("TOMBSTONES", OTableFormatter.ALIGNMENT.RIGHT);
        }
      }

      final ODocument footer = new ODocument();
      footer.field("NAME", "TOTAL");
      footer.field("COUNT", totalElements);
      if (!isRemote) {
        footer.field("SPACE-USED", OFileUtils.getSizeAsString(totalSpaceUsed));
        if (commandOptions.containsKey("-v")) {
          footer.field("TOMBSTONES", totalTombstones);
        }
      }
      formatter.setFooter(footer);

      formatter.writeRecords(resultSet, -1);

      message("\n");

    } else message("\nNo database selected yet.");
  }

  @ConsoleCommand(
      description = "Display all the configured classes",
      aliases = {"classes"},
      onlineHelp = "Console-Command-List-Classes")
  public void listClasses() {
    if (currentDatabaseName != null) {
      message("\n\nCLASSES");

      final List<ODocument> resultSet = new ArrayList<ODocument>();

      long totalElements = 0;
      long count;

      currentDatabase.getMetadata().reload();
      final List<OClass> classes =
          new ArrayList<OClass>(
              currentDatabase.getMetadata().getImmutableSchemaSnapshot().getClasses());
      Collections.sort(
          classes,
          new Comparator<OClass>() {
            public int compare(OClass o1, OClass o2) {
              return o1.getName().compareToIgnoreCase(o2.getName());
            }
          });

      for (OClass cls : classes) {
        try {
          final ODocument row = new ODocument();
          resultSet.add(row);

          final StringBuilder clusters = new StringBuilder(1024);
          if (cls.isAbstract()) clusters.append("-");
          else {
            int[] clusterIds = cls.getClusterIds();
            for (int i = 0; i < clusterIds.length; ++i) {
              if (i > 0) clusters.append(",");

              clusters.append(currentDatabase.getClusterNameById(clusterIds[i]));
              clusters.append("(");
              clusters.append(clusterIds[i]);
              clusters.append(")");
            }
          }

          count = currentDatabase.countClass(cls.getName(), false);
          totalElements += count;

          final String superClasses =
              cls.hasSuperClasses() ? Arrays.toString(cls.getSuperClassesNames().toArray()) : "";

          row.field("NAME", cls.getName());
          row.field("SUPER-CLASSES", superClasses);
          row.field("CLUSTERS", clusters);
          row.field("COUNT", count);

        } catch (Exception ignored) {
          // IGNORED
        }
      }

      final OTableFormatter formatter = new OTableFormatter(this);

      formatter.setMaxWidthSize(getConsoleWidth());
      formatter.setMaxMultiValueEntries(getMaxMultiValueEntries());

      formatter.setColumnAlignment("COUNT", OTableFormatter.ALIGNMENT.RIGHT);

      final ODocument footer = new ODocument();
      footer.field("NAME", "TOTAL");
      footer.field("COUNT", totalElements);

      formatter.setFooter(footer);

      formatter.writeRecords(resultSet, -1);

      message("\n");

    } else message("\nNo database selected yet.");
  }

  @ConsoleCommand(
      description = "Display all the connected servers that manage current database",
      onlineHelp = "Console-Command-List-Servers")
  public void listServers() {

    final ODocument distribCfg = getDistributedConfiguration();
    if (distribCfg == null) {
      message("\n\nDistributed configuration is not active, cannot retrieve server list");
      return;
    }

    final List<OIdentifiable> servers = new ArrayList<OIdentifiable>();

    final Collection<ODocument> members = distribCfg.field("members");

    if (members != null) {
      message("\n\nCONFIGURED SERVERS");

      for (ODocument m : members) {
        final ODocument server = new ODocument();

        server.field("Name", m.<Object>field("name"));
        server.field("Status", m.<Object>field("status"));
        server.field("Connections", m.<Object>field("connections"));
        server.field("StartedOn", m.<Object>field("startedOn"));

        final Collection<Map> listeners = m.field("listeners");
        if (listeners != null) {
          for (Map l : listeners) {
            final String protocol = (String) l.get("protocol");
            if (protocol.equals("ONetworkProtocolBinary")) {
              server.field("Binary", l.get("listen"));
            } else if (protocol.equals("ONetworkProtocolHttpDb")) {
              server.field("HTTP", l.get("listen"));
            }
          }
        }

        final long usedMem = m.field("usedMemory");
        final long freeMem = m.field("freeMemory");
        final long maxMem = m.field("maxMemory");

        server.field(
            "UsedMemory",
            String.format(
                "%s (%.2f%%)",
                OFileUtils.getSizeAsString(usedMem), ((float) usedMem / (float) maxMem) * 100));
        server.field(
            "FreeMemory",
            String.format(
                "%s (%.2f%%)",
                OFileUtils.getSizeAsString(freeMem), ((float) freeMem / (float) maxMem) * 100));
        server.field("MaxMemory", OFileUtils.getSizeAsString(maxMem));

        servers.add(server);
      }
    }
    currentResultSet = servers;
    new OTableFormatter(this).setMaxWidthSize(getConsoleWidth()).writeRecords(servers, -1);
  }

  @ConsoleCommand(
      description =
          "Loook up a record using the dictionary. If found, set it as the current record",
      onlineHelp = "Console-Command-Dictionary-Get")
  public void dictionaryGet(
      @ConsoleParameter(name = "key", description = "The key to search") final String iKey) {
    checkForDatabase();

    currentRecord = currentDatabase.getDictionary().get(iKey);
    if (currentRecord == null) message("\nEntry not found in dictionary.");
    else {
      currentRecord = (ORecord) currentRecord.load();
      displayRecord(null);
    }
  }

  @ConsoleCommand(
      description =
          "Insert or modify an entry in the database dictionary. The entry is comprised of key=String, value=record-id",
      onlineHelp = "Console-Command-Dictionary-Put")
  public void dictionaryPut(
      @ConsoleParameter(name = "key", description = "The key to bind") final String iKey,
      @ConsoleParameter(
              name = "record-id",
              description = "The record-id of the record to bind to the key")
          final String iRecordId) {
    checkForDatabase();

    currentRecord = currentDatabase.load(new ORecordId(iRecordId));
    if (currentRecord == null)
      message("\nError: record with id '" + iRecordId + "' was not found in database");
    else {
      currentDatabase.getDictionary().put(iKey, currentRecord);
      displayRecord(null);
      message(
          "\nThe entry "
              + iKey
              + "="
              + iRecordId
              + " has been inserted in the database dictionary");
    }
  }

  @ConsoleCommand(
      description = "Remove the association in the dictionary",
      onlineHelp = "Console-Command-Dictionary-Remove")
  public void dictionaryRemove(
      @ConsoleParameter(name = "key", description = "The key to remove") final String iKey) {
    checkForDatabase();

    boolean result = currentDatabase.getDictionary().remove(iKey);
    if (!result) message("\nEntry not found in dictionary.");
    else message("\nEntry removed from the dictionary.");
  }

  @ConsoleCommand(description = "Displays the status of the cluster nodes")
  public void clusterStatus() throws IOException {
    checkForRemoteServer();
    try {

      message("\nCluster status:");
      ODocument clusterStatus =
          ((OrientDBRemote) OrientDBInternal.extract(orientDB))
              .getClusterStatus(currentDatabaseUserName, currentDatabaseUserPassword);
      out.println(clusterStatus.toJSON("attribSameRow,alwaysFetchEmbedded,fetchPlan:*:0"));

    } catch (Exception e) {
      printError(e);
    }
  }

  @ConsoleCommand(description = "Check database integrity", splitInWords = false)
  public void checkDatabase(
      @ConsoleParameter(name = "options", description = "Options: -v", optional = true)
          final String iOptions)
      throws IOException {
    checkForDatabase();

    if (currentDatabase.getStorage().isRemote()) {
      message("\nCannot check integrity of non-local database. Connect to it using local mode.");
      return;
    }

    boolean verbose = iOptions != null && iOptions.contains("-v");

    message("\nChecking storage.");
    try {
      ((OAbstractPaginatedStorage) currentDatabase.getStorage()).check(verbose, this);
    } catch (ODatabaseImportException e) {
      printError(e);
    }
  }

  @ConsoleCommand(description = "Repair database structure", splitInWords = false)
  public void repairDatabase(
      @ConsoleParameter(
              name = "options",
              description =
                  "Options: [--fix-graph] [--force-embedded-ridbags] [--fix-links] [-v]] [--fix-ridbags] [--fix-bonsai]",
              optional = true)
          String iOptions)
      throws IOException {
    checkForDatabase();
    final boolean force_embedded =
        iOptions == null || iOptions.contains("--force-embedded-ridbags");
    final boolean fix_graph = iOptions == null || iOptions.contains("--fix-graph");
    if (force_embedded) {
      OGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(Integer.MAX_VALUE);
      OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(Integer.MAX_VALUE);
    }
    if (fix_graph || force_embedded) {
      // REPAIR GRAPH
      final Map<String, List<String>> options = parseOptions(iOptions);
      new OGraphRepair().repair(currentDatabase, this, options);
    }

    final boolean fix_links = iOptions == null || iOptions.contains("--fix-links");
    if (fix_links) {
      // REPAIR DATABASE AT LOW LEVEL
      boolean verbose = iOptions != null && iOptions.contains("-v");

      new ODatabaseRepair()
          .setDatabase(currentDatabase)
          .setOutputListener(
              new OCommandOutputListener() {
                @Override
                public void onMessage(String iText) {
                  message(iText);
                }
              })
          .setVerbose(verbose)
          .run();
    }

    if (!currentDatabase.getURL().startsWith("plocal")) {
      message("\n fix-bonsai can be run only on plocal connection \n");
      return;
    }

    final boolean fix_ridbags = iOptions == null || iOptions.contains("--fix-ridbags");
    final boolean fix_bonsai = iOptions == null || iOptions.contains("--fix-bonsai");
    if (fix_ridbags || fix_bonsai || force_embedded) {
      OBonsaiTreeRepair repairer = new OBonsaiTreeRepair();
      repairer.repairDatabaseRidbags(currentDatabase, this);
    }
  }

  @ConsoleCommand(description = "Compare two databases")
  public void compareDatabases(
      @ConsoleParameter(name = "db1-url", description = "URL of the first database")
          final String iDb1URL,
      @ConsoleParameter(name = "db2-url", description = "URL of the second database")
          final String iDb2URL,
      @ConsoleParameter(name = "username", description = "User name", optional = false)
          final String iUserName,
      @ConsoleParameter(name = "password", description = "User password", optional = false)
          final String iUserPassword,
      @ConsoleParameter(
              name = "detect-mapping-data",
              description =
                  "Whether RID mapping data after DB import should be tried to found on the disk",
              optional = true)
          String autoDiscoveringMappingData)
      throws IOException {
    try {
      final ODatabaseCompare compare =
          new ODatabaseCompare(iDb1URL, iDb2URL, iUserName, iUserPassword, this);

      compare.setAutoDetectExportImportMap(
          autoDiscoveringMappingData != null ? Boolean.valueOf(autoDiscoveringMappingData) : true);
      compare.setCompareIndexMetadata(true);
      compare.compare();
    } catch (ODatabaseExportException e) {
      printError(e);
    }
  }

  @ConsoleCommand(
      description = "Load a sql script into the current database",
      splitInWords = true,
      onlineHelp = "Console-Command-Load-Script")
  public void loadScript(
      @ConsoleParameter(name = "scripPath", description = "load script scriptPath")
          final String scriptPath)
      throws IOException {

    checkForDatabase();

    message("\nLoading script " + scriptPath + "...");

    executeBatch(scriptPath);

    message("\nLoaded script " + scriptPath);
  }

  @ConsoleCommand(
      description = "Import a database into the current one",
      splitInWords = false,
      onlineHelp = "Console-Command-Import")
  public void importDatabase(
      @ConsoleParameter(name = "options", description = "Import options") final String text)
      throws IOException {
    checkForDatabase();

    message("\nImporting database " + text + "...");

    final List<String> items = OStringSerializerHelper.smartSplit(text, ' ');
    final String fileName =
        items.size() <= 0 || (items.get(1)).charAt(0) == '-' ? null : items.get(1);
    final String options =
        fileName != null
            ? text.substring((items.get(0)).length() + (items.get(1)).length() + 1).trim()
            : text;

    try {
      if (currentDatabase.isRemote()) {
        ODatabaseImportRemote databaseImport =
            new ODatabaseImportRemote(currentDatabase, fileName, this);

        databaseImport.setOptions(options);
        databaseImport.importDatabase();
        databaseImport.close();

      } else {
        ODatabaseImport databaseImport = new ODatabaseImport(currentDatabase, fileName, this);

        databaseImport.setOptions(options);
        databaseImport.importDatabase();
        databaseImport.close();
      }
    } catch (ODatabaseImportException e) {
      printError(e);
    }
  }

  @ConsoleCommand(
      description = "Backup a database",
      splitInWords = false,
      onlineHelp = "Console-Command-Backup")
  public void backupDatabase(
      @ConsoleParameter(name = "options", description = "Backup options") final String iText)
      throws IOException {
    checkForDatabase();

    final List<String> items = OStringSerializerHelper.smartSplit(iText, ' ', ' ');

    if (items.size() < 2) {
      try {
        syntaxError("backupDatabase", getClass().getMethod("backupDatabase", String.class));
      } catch (NoSuchMethodException ignored) {
      }
      return;
    }

    final String fileName =
        items.size() <= 0 || items.get(1).charAt(0) == '-' ? null : items.get(1);

    if (fileName == null || fileName.trim().isEmpty()) {
      try {
        syntaxError("backupDatabase", getClass().getMethod("backupDatabase", String.class));
        return;
      } catch (NoSuchMethodException ignored) {
      }
    }

    int bufferSize = Integer.parseInt(properties.get(OConsoleProperties.BACKUP_BUFFER_SIZE));
    int compressionLevel =
        Integer.parseInt(properties.get(OConsoleProperties.BACKUP_COMPRESSION_LEVEL));
    boolean incremental = false;

    for (int i = 2; i < items.size(); ++i) {
      final String item = items.get(i);
      final int sep = item.indexOf('=');

      final String parName;
      final String parValue;
      if (sep > -1) {
        parName = item.substring(1, sep);
        parValue = item.substring(sep + 1);
      } else {
        parName = item.substring(1);
        parValue = null;
      }

      if (parName.equalsIgnoreCase("incremental")) incremental = true;
      else if (parName.equalsIgnoreCase("bufferSize")) bufferSize = Integer.parseInt(parValue);
      else if (parName.equalsIgnoreCase("compressionLevel"))
        compressionLevel = Integer.parseInt(parValue);
    }

    final long startTime = System.currentTimeMillis();
    String fName = null;
    try {
      if (incremental) {
        out.println(
            new StringBuilder(
                    "Executing incremental backup of database '" + currentDatabaseName + "' to: ")
                .append(iText)
                .append("..."));
        fName = currentDatabase.incrementalBackup(fileName);

        message(
            "\nIncremental Backup executed in %.2f seconds stored in file %s",
            ((float) (System.currentTimeMillis() - startTime) / 1000), fName);

      } else {
        out.println(
            new StringBuilder(
                    "Executing full backup of database '" + currentDatabaseName + "' to: ")
                .append(iText)
                .append("..."));
        final FileOutputStream fos = new FileOutputStream(fileName);
        try {
          currentDatabase.backup(fos, null, null, this, compressionLevel, bufferSize);
          fos.flush();
          fos.close();
          message(
              "\nBackup executed in %.2f seconds",
              ((float) (System.currentTimeMillis() - startTime) / 1000));
        } catch (Exception e) {
          fos.close();
          File f = new File(fileName);
          if (f.exists()) f.delete();
          throw e;
        }
      }
    } catch (ODatabaseExportException e) {
      printError(e);
    }
  }

  @ConsoleCommand(
      description = "Restore a database into the current one",
      splitInWords = false,
      onlineHelp = "Console-Command-Restore")
  public void restoreDatabase(
      @ConsoleParameter(name = "options", description = "Restore options") final String text)
      throws IOException {
    checkForDatabase();

    final List<String> items = OStringSerializerHelper.smartSplit(text, ' ');

    if (items.size() < 2)
      try {
        syntaxError("restoreDatabase", getClass().getMethod("restoreDatabase", String.class));
        return;
      } catch (NoSuchMethodException e) {
      }

    final String fileName =
        items.size() <= 0 || (items.get(1)).charAt(0) == '-' ? null : items.get(1);

    final long startTime = System.currentTimeMillis();
    try {

      // FULL RESTORE
      message("\nRestoring database '%s' from full backup...", text);
      final FileInputStream f = new FileInputStream(fileName);
      try {
        currentDatabase.restore(f, null, null, this);
      } finally {
        f.close();
      }
    } catch (ODatabaseImportException e) {
      printError(e);
    } finally {
      message(
          "\nDatabase restored in %.2f seconds",
          ((float) (System.currentTimeMillis() - startTime) / 1000));
    }
  }

  @ConsoleCommand(
      description = "Export a database",
      splitInWords = false,
      onlineHelp = "Console-Command-Export")
  public void exportDatabase(
      @ConsoleParameter(name = "options", description = "Export options") final String iText)
      throws IOException {
    checkForDatabase();

    out.println(
        new StringBuilder("Exporting current database to: ")
            .append(iText)
            .append(" in GZipped JSON format ..."));
    final List<String> items = OStringSerializerHelper.smartSplit(iText, ' ');
    final String fileName =
        items.size() <= 1 || items.get(1).charAt(0) == '-' ? null : items.get(1);
    final String options =
        fileName != null
            ? iText.substring(items.get(0).length() + items.get(1).length() + 1).trim()
            : iText;

    try {
      new ODatabaseExport(currentDatabase, fileName, this)
          .setOptions(options)
          .exportDatabase()
          .close();
    } catch (ODatabaseExportException e) {
      printError(e);
    }
  }

  @ConsoleCommand(description = "Export a database schema")
  public void exportSchema(
      @ConsoleParameter(name = "output-file", description = "Output file path")
          final String iOutputFilePath)
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

  @ConsoleCommand(
      description = "Export the current record in the requested format",
      onlineHelp = "Console-Command-Export-Record")
  public void exportRecord(
      @ConsoleParameter(name = "format", description = "Format, such as 'json'")
          final String iFormat,
      @ConsoleParameter(name = "options", description = "Options", optional = true) String iOptions)
      throws IOException {
    checkForDatabase();
    checkCurrentObject();

    final ORecordSerializer serializer =
        ORecordSerializerFactory.instance().getFormat(iFormat.toLowerCase(Locale.ENGLISH));

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
      out.println(currentRecord.toJSON(iOptions));
    } catch (ODatabaseExportException e) {
      printError(e);
    }
  }

  @ConsoleCommand(description = "Return all configured properties")
  public void properties() {
    message("\nPROPERTIES:");

    final List<ODocument> resultSet = new ArrayList<ODocument>();

    for (Entry<String, String> p : properties.entrySet()) {
      final ODocument row = new ODocument();
      resultSet.add(row);

      row.field("NAME", p.getKey());
      row.field("VALUE", p.getValue());
    }

    final OTableFormatter formatter = new OTableFormatter(this);
    formatter.setMaxWidthSize(getConsoleWidth());
    formatter.setMaxMultiValueEntries(getMaxMultiValueEntries());

    formatter.writeRecords(resultSet, -1);

    message("\n");
  }

  @ConsoleCommand(description = "Return the value of a property")
  public void get(
      @ConsoleParameter(name = "property-name", description = "Name of the property")
          final String iPropertyName) {
    Object value = properties.get(iPropertyName);

    out.println();

    if (value == null) message("\nProperty '" + iPropertyName + "' is not setted");
    else out.println(iPropertyName + " = " + value);
  }

  @ConsoleCommand(
      description = "Change the value of a property",
      onlineHelp = "Console-Command-Set")
  public void set(
      @ConsoleParameter(name = "property-name", description = "Name of the property")
          final String iPropertyName,
      @ConsoleParameter(name = "property-value", description = "Value to set")
          final String iPropertyValue) {
    Object prevValue = properties.get(iPropertyName);

    out.println();

    if (iPropertyName.equalsIgnoreCase("limit")
        && (Integer.parseInt(iPropertyValue) == 0 || Integer.parseInt(iPropertyValue) < -1)) {
      message("\nERROR: Limit must be > 0 or = -1 (no limit)");
    } else {

      if (prevValue != null) message("\nPrevious value was: " + prevValue);

      properties.put(iPropertyName, iPropertyValue);

      out.println();
      out.println(iPropertyName + " = " + iPropertyValue);
    }
  }

  @ConsoleCommand(description = "Declare an intent", onlineHelp = "")
  public void declareIntent(
      @ConsoleParameter(name = "Intent name", description = "name of the intent to execute")
          final String iIntentName) {
    checkForDatabase();

    message("\nDeclaring intent '" + iIntentName + "'...");

    if (iIntentName.equalsIgnoreCase("massiveinsert"))
      currentDatabase.declareIntent(new OIntentMassiveInsert());
    else if (iIntentName.equalsIgnoreCase("massiveread"))
      currentDatabase.declareIntent(new OIntentMassiveRead());
    else if (iIntentName.equalsIgnoreCase("null")) currentDatabase.declareIntent(null);
    else
      throw new IllegalArgumentException(
          "Intent '"
              + iIntentName
              + "' not supported. Available ones are: massiveinsert, massiveread, null");

    message("\nIntent '" + iIntentName + "' set successfully");
  }

  @ConsoleCommand(description = "Execute a command against the profiler")
  public void profiler(
      @ConsoleParameter(
              name = "profiler command",
              description = "command to execute against the profiler")
          final String iCommandName) {
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
  public void configGet(
      @ConsoleParameter(name = "config-name", description = "Name of the configuration")
          final String iConfigName)
      throws IOException {
    final OGlobalConfiguration config = OGlobalConfiguration.findByKey(iConfigName);
    if (config == null)
      throw new IllegalArgumentException(
          "Configuration variable '" + iConfigName + "' wasn't found");

    final String value;
    if (!OrientDBInternal.extract(orientDB).isEmbedded()) {
      value =
          ((OrientDBRemote) OrientDBInternal.extract(orientDB))
              .getGlobalConfiguration(currentDatabaseUserName, currentDatabaseUserPassword, config);
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
  public void configSet(
      @ConsoleParameter(name = "config-name", description = "Name of the configuration")
          final String iConfigName,
      @ConsoleParameter(name = "config-value", description = "Value to set")
          final String iConfigValue)
      throws IOException {
    final OGlobalConfiguration config = OGlobalConfiguration.findByKey(iConfigName);
    if (config == null)
      throw new IllegalArgumentException("Configuration variable '" + iConfigName + "' not found");

    if (orientDB != null && !OrientDBInternal.extract(orientDB).isEmbedded()) {
      ((OrientDBRemote) OrientDBInternal.extract(orientDB))
          .setGlobalConfiguration(
              currentDatabaseUserName, currentDatabaseUserPassword, config, iConfigValue);
      message("\nRemote configuration value changed correctly");
    } else {
      config.setValue(iConfigValue);
      message("\nLocal configuration value changed correctly");
    }
    out.println();
  }

  @ConsoleCommand(description = "Return all the configuration values")
  public void config() throws IOException {
    if (!OrientDBInternal.extract(orientDB).isEmbedded()) {
      final Map<String, String> values =
          ((OrientDBRemote) OrientDBInternal.extract(orientDB))
              .getGlobalConfigurations(currentDatabaseUserName, currentDatabaseUserPassword);

      message("\nREMOTE SERVER CONFIGURATION");

      final List<ODocument> resultSet = new ArrayList<ODocument>();

      for (Entry<String, String> p : values.entrySet()) {
        final ODocument row = new ODocument();
        resultSet.add(row);

        row.field("NAME", p.getKey());
        row.field("VALUE", p.getValue());
      }

      final OTableFormatter formatter = new OTableFormatter(this);
      formatter.setMaxWidthSize(getConsoleWidth());
      formatter.setMaxMultiValueEntries(getMaxMultiValueEntries());

      formatter.writeRecords(resultSet, -1);

    } else {
      // LOCAL STORAGE
      message("\nLOCAL SERVER CONFIGURATION");

      final List<ODocument> resultSet = new ArrayList<ODocument>();

      for (OGlobalConfiguration cfg : OGlobalConfiguration.values()) {
        final ODocument row = new ODocument();
        resultSet.add(row);

        row.field("NAME", cfg.getKey());
        row.field("VALUE", (Object) cfg.getValue());
      }

      final OTableFormatter formatter = new OTableFormatter(this);
      formatter.setMaxWidthSize(getConsoleWidth());
      formatter.setMaxMultiValueEntries(getMaxMultiValueEntries());

      formatter.writeRecords(resultSet, -1);
    }

    message("\n");
  }

  /** Should be used only by console commands */
  public ODatabaseDocument getCurrentDatabase() {
    return currentDatabase;
  }

  /** Pass an existent database instance to be used as current. */
  public OConsoleDatabaseApp setCurrentDatabase(final ODatabaseDocumentInternal iCurrentDatabase) {
    currentDatabase = iCurrentDatabase;
    currentDatabaseName = iCurrentDatabase.getName();
    return this;
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

    currentRecord =
        currentDatabase.executeReadRecord(
            new ORecordId(iRecordId),
            null,
            -1,
            iFetchPlan,
            true,
            false,
            false,
            OStorage.LOCKING_STRATEGY.NONE,
            new SimpleRecordReader(false));
    displayRecord(null);

    message("\nOK");
  }

  /**
   * console command to open a db
   *
   * <p>usage: <code>
   * open dbName dbUser dbPwd
   * </code>
   *
   * @param dbName
   * @param user
   * @param password
   */
  @ConsoleCommand(description = "Open a database", onlineHelp = "Console-Command-Use")
  public void open(
      @ConsoleParameter(name = "db-name", description = "The database name") final String dbName,
      @ConsoleParameter(name = "user", description = "The database user") final String user,
      @ConsoleParameter(name = "password", description = "The database password")
          final String password) {

    if (orientDB == null) {
      message("Invalid context. Please use 'connect env' first");
      return;
    }

    currentDatabase = (ODatabaseDocumentInternal) orientDB.open(dbName, user, password);

    currentDatabaseName = currentDatabase.getName();

    message("OK");

    final ODocument distribCfg = getDistributedConfiguration();
    if (distribCfg != null) listServers();
  }

  @Override
  protected RESULT executeServerCommand(String iCommand) {
    if (super.executeServerCommand(iCommand) == RESULT.NOT_EXECUTED) {
      iCommand = iCommand.trim();
      if (iCommand.toLowerCase().startsWith("connect ")) {
        if (iCommand.substring("connect ".length()).trim().toLowerCase().startsWith("env ")) {
          return connectEnv(iCommand);
        }
        return RESULT.NOT_EXECUTED;
      }
      if (orientDB != null) {
        int displayLimit = 20;
        try {
          if (properties.get(OConsoleProperties.LIMIT) != null) {
            displayLimit = Integer.parseInt(properties.get(OConsoleProperties.LIMIT));
          }
          OResultSet rs = orientDB.execute(iCommand);
          int count = 0;
          List<OIdentifiable> result = new ArrayList<>();
          while (rs.hasNext() && (displayLimit < 0 || count < displayLimit)) {
            OResult item = rs.next();
            if (item.isBlob()) {
              result.add(item.getBlob().get());
            } else {
              result.add(item.toElement());
            }
          }
          setResultset(result);
          dumpResultSet(displayLimit);
          return RESULT.OK;
        } catch (OCommandExecutionException e) {
          printError(e);
          return RESULT.ERROR;
        } catch (Exception e) {
          if (e.getCause() instanceof OCommandExecutionException) {
            printError(e);
            return RESULT.ERROR;
          }
          return RESULT.NOT_EXECUTED;
        }
      }
    }
    return RESULT.NOT_EXECUTED;
  }

  /**
   * console command to open an OrientDB context
   *
   * <p>usage: <code>
   * connect env URL serverUser serverPwd
   * </code> eg. <code>
   * connect env remote:localhost root root
   * <p>
   * connect env embedded:. root root
   *
   * </code>
   *
   * @param iCommand
   * @return
   */
  private RESULT connectEnv(String iCommand) {
    String[] p = iCommand.split(" ");
    List<String> parts = Arrays.stream(p).filter(x -> x.length() > 0).collect(Collectors.toList());
    if (parts.size() < 3) {
      error("\n!Invalid syntax: '%s'", iCommand);
      return RESULT.ERROR;
    }
    String url = parts.get(2);
    String user = null;
    String pw = null;

    if (parts.size() > 4) {
      user = parts.get(3);
      pw = parts.get(4);
    }

    orientDB = new OrientDB(url, user, pw, OrientDBConfig.defaultConfig());
    return RESULT.OK;
  }

  /** Should be used only by console commands */
  protected void checkForRemoteServer() {
    if (orientDB == null || OrientDBInternal.extract(orientDB).isEmbedded())
      throw new OSystemException(
          "Remote server is not connected. Use 'connect remote:<host>[:<port>][/<database-name>]' to connect");
  }

  /** Should be used only by console commands */
  protected void checkForDatabase() {
    if (currentDatabase == null)
      throw new OSystemException(
          "Database not selected. Use 'connect <url> <user> <password>' to connect to a database.");
    if (currentDatabase.isClosed())
      throw new ODatabaseException("Database '" + currentDatabaseName + "' is closed");
  }

  /** Should be used only by console commands */
  protected void checkCurrentObject() {
    if (currentRecord == null)
      throw new OSystemException("The is no current object selected: create a new one or load it");
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
      for (int i = 0; i < 10; ++i) message(" ");
      message("]   0%");
    }
  }

  public boolean onProgress(final Object iTask, final long iCounter, final float iPercent) {
    final int completitionBar = (int) iPercent / 10;

    if (((int) (iPercent * 10)) == lastPercentStep) return true;

    final StringBuilder buffer = new StringBuilder(64);

    if (interactiveMode) {
      buffer.append("\r[");
      for (int i = 0; i < completitionBar; ++i) buffer.append('=');
      for (int i = completitionBar; i < 10; ++i) buffer.append(' ');
      message("] %3.1f%% ", iPercent);
    } else {
      for (int i = lastPercentStep / 100; i < completitionBar; ++i) buffer.append('=');
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
      if (iSucceed) message("\r[==========] 100% Done.");
      else message(" Error!");
    else message(iSucceed ? "] Done." : " Error!");
  }

  /** Closes the console freeing all the used resources. */
  public void close() {
    if (currentDatabase != null) {
      currentDatabase.activateOnCurrentThread();
      currentDatabase.close();
      currentDatabase = null;
    }
    if (orientDB != null) {
      orientDB.close();
    }
    currentResultSet = null;
    currentRecord = null;
    currentResult = null;
    commandBuffer.setLength(0);
  }

  protected void dumpDistributedConfiguration(final boolean iForce) {
    if (currentDatabase == null) return;

    if (currentDatabase.isRemote()) {
      final OStorageRemote stg = ((ODatabaseDocumentRemote) currentDatabase).getStorageRemote();
      final ODocument distributedCfg = stg.getClusterConfiguration();
      if (distributedCfg != null && !distributedCfg.isEmpty()) {
        message("\n\nDISTRIBUTED CONFIGURATION:\n" + distributedCfg.toJSON("prettyPrint"));
      } else if (iForce)
        message("\n\nDISTRIBUTED CONFIGURATION: none (OrientDB is running in standalone mode)");
    }
  }

  protected ODocument getDistributedConfiguration() {
    if (currentDatabase != null) {
      final OStorage stg = currentDatabase.getStorage();
      if (stg instanceof OStorageRemote) return ((OStorageRemote) stg).getClusterConfiguration();
    }
    return null;
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
    properties.put(OConsoleProperties.LIMIT, "20");
    properties.put(OConsoleProperties.DEBUG, "false");
    properties.put(OConsoleProperties.COLLECTION_MAX_ITEMS, "10");
    properties.put(OConsoleProperties.MAX_BINARY_DISPLAY, "150");
    properties.put(OConsoleProperties.VERBOSE, "2");
    properties.put(OConsoleProperties.IGNORE_ERRORS, "false");
    properties.put(OConsoleProperties.BACKUP_COMPRESSION_LEVEL, "9"); // 9 = MAX
    properties.put(OConsoleProperties.BACKUP_BUFFER_SIZE, "1048576"); // 1MB
    properties.put(
        OConsoleProperties.COMPATIBILITY_LEVEL, "" + OConsoleProperties.COMPATIBILITY_LEVEL_LATEST);
  }

  protected OIdentifiable setCurrentRecord(final int iIndex) {
    currentRecordIdx = iIndex;
    if (iIndex < currentResultSet.size()) currentRecord = (ORecord) currentResultSet.get(iIndex);
    else currentRecord = null;
    return currentRecord;
  }

  protected void printApplicationInfo() {
    message("\nOrientDB console v." + OConstants.getVersion() + " " + OConstants.ORIENT_URL);
    message("\nType 'help' to display all the supported commands.");
  }

  protected void dumpResultSet(final int limit) {
    new OTableFormatter(this)
        .setMaxWidthSize(getConsoleWidth())
        .setMaxMultiValueEntries(getMaxMultiValueEntries())
        .writeRecords(currentResultSet, limit);
  }

  protected float getElapsedSecs(final long start) {
    return (float) (System.currentTimeMillis() - start) / 1000;
  }

  protected void printError(final Exception e) {
    if (properties.get(OConsoleProperties.DEBUG) != null
        && Boolean.parseBoolean(properties.get(OConsoleProperties.DEBUG))) {
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
    currentDatabase.reload();
  }

  @Override
  protected String getContext() {
    final StringBuilder buffer = new StringBuilder(64);

    if (currentDatabase != null && currentDatabaseName != null) {
      currentDatabase.activateOnCurrentThread();

      buffer.append(" {db=");
      buffer.append(currentDatabaseName);
      if (currentDatabase.getTransaction().isActive()) {
        buffer.append(" tx=[");
        buffer.append(currentDatabase.getTransaction().getEntryCount());
        buffer.append(" entries]");
      }
    } else if (urlConnection != null) {
      buffer.append(" {server=");
      buffer.append(urlConnection.getUrl());
    }

    final String promptDateFormat = properties.get(OConsoleProperties.PROMPT_DATE_FORMAT);
    if (promptDateFormat != null) {
      buffer.append(" (");
      final SimpleDateFormat df = new SimpleDateFormat(promptDateFormat);
      buffer.append(df.format(new Date()));
      buffer.append(")");
    }

    if (buffer.length() > 0) buffer.append("}");

    return buffer.toString();
  }

  @Override
  protected String getPrompt() {
    return String.format("orientdb%s> ", getContext());
  }

  protected void parseResult() {
    setResultset(null);

    if (currentResult instanceof Map<?, ?>) return;

    final Object first = OMultiValue.getFirstValue(currentResult);

    if (first instanceof OIdentifiable) {
      if (currentResult instanceof List<?>) currentResultSet = (List<OIdentifiable>) currentResult;
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
    currentRecord =
        iResultset == null || iResultset.isEmpty() ? null : (ORecord) iResultset.get(0).getRecord();
  }

  protected void resetResultSet() {
    currentResultSet = null;
    currentRecord = null;
  }

  protected void executeServerSideScript(final String iLanguage, final String iText) {
    if (iText == null) return;

    resetResultSet();

    long start = System.currentTimeMillis();

    OResultSet rs = currentDatabase.execute(iLanguage, iText);
    currentResult = rs.stream().map(x -> x.toElement()).collect(Collectors.toList());
    rs.close();
    float elapsedSeconds = getElapsedSecs(start);

    parseResult();
    if (currentResultSet != null) {
      dumpResultSet(-1);
      message(
          "\nServer side script executed in %f sec(s). Returned %d records",
          elapsedSeconds, currentResultSet.size());
    } else {
      String lineFeed = currentResult instanceof Map<?, ?> ? "\n" : "";
      message(
          "\nServer side script executed in %f sec(s). Value returned is: %s%s",
          elapsedSeconds, lineFeed, currentResult);
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

        options.put(option, items);
      }
    }
    return options;
  }

  public int getMaxMultiValueEntries() {
    if (properties.containsKey(OConsoleProperties.MAX_MULTI_VALUE_ENTRIES))
      return Integer.parseInt(properties.get(OConsoleProperties.MAX_MULTI_VALUE_ENTRIES));
    return maxMultiValueEntries;
  }

  private void dumpRecordDetails() {
    if (currentRecord == null) return;
    else if (currentRecord instanceof ODocument) {
      ODocument rec = (ODocument) currentRecord;
      if (rec.getClassName() != null || rec.getIdentity().isValid()) {
        message(
            "\nDOCUMENT @class:%s @rid:%s @version:%d",
            rec.getClassName(), rec.getIdentity().toString(), rec.getVersion());
      }

      final List<ODocument> resultSet = new ArrayList<ODocument>();

      Object value;
      for (String fieldName : rec.getPropertyNames()) {
        value = rec.getProperty(fieldName);
        if (value instanceof byte[]) value = "byte[" + ((byte[]) value).length + "]";
        else if (value instanceof Iterator<?>) {
          final List<Object> coll = new ArrayList<Object>();
          while (((Iterator<?>) value).hasNext()) coll.add(((Iterator<?>) value).next());
          value = coll;
        } else if (OMultiValue.isMultiValue(value)) {
          value =
              OTableFormatter.getPrettyFieldMultiValue(
                  OMultiValue.getMultiValueIterator(value), getMaxMultiValueEntries());
        }

        final ODocument row = new ODocument();
        resultSet.add(row);

        row.field("NAME", fieldName);
        row.field("VALUE", value);
      }

      final OTableFormatter formatter = new OTableFormatter(this);
      formatter.setMaxWidthSize(getConsoleWidth());
      formatter.setMaxMultiValueEntries(getMaxMultiValueEntries());

      formatter.writeRecords(resultSet, -1);

    } else if (currentRecord instanceof OBlob) {
      OBlob rec = (OBlob) currentRecord;
      message(
          "\n+-------------------------------------------------------------------------------------------------+");
      message(
          "\n| Bytes    - @rid: %s @version: %d", rec.getIdentity().toString(), rec.getVersion());
      message(
          "\n+-------------------------------------------------------------------------------------------------+");

      final byte[] value = rec.toStream();
      final int max =
          Math.min(
              Integer.parseInt(properties.get(OConsoleProperties.MAX_BINARY_DISPLAY)),
              Array.getLength(value));
      for (int i = 0; i < max; ++i) {
        message("%03d", Array.getByte(value, i));
      }
      message(
          "\n+-------------------------------------------------------------------------------------------------+");

    } else {
      message(
          "\n+-------------------------------------------------------------------------------------------------+");
      message(
          "\n| %s - record id: %s   v.%d",
          currentRecord.getClass().getSimpleName(),
          currentRecord.getIdentity().toString(),
          currentRecord.getVersion());
      message(
          "\n+-------------------------------------------------------------------------------------------------+");
    }
    out.println();
  }

  private void printSupportedSerializerFormat() {
    message("\nSupported formats are:");

    for (ORecordSerializer s : ORecordSerializerFactory.instance().getFormats()) {
      if (s instanceof ORecordSerializerStringAbstract) message("\n- " + s.toString());
    }
  }

  private void browseRecords(final OIdentifiableIterator<?> it) {
    final int limit = Integer.parseInt(properties.get(OConsoleProperties.LIMIT));

    final OTableFormatter tableFormatter =
        new OTableFormatter(this)
            .setMaxWidthSize(getConsoleWidth())
            .setMaxMultiValueEntries(maxMultiValueEntries);

    setResultset(new ArrayList<OIdentifiable>());
    while (it.hasNext() && currentResultSet.size() <= limit) currentResultSet.add(it.next());

    tableFormatter.writeRecords(currentResultSet, limit);
  }

  private Object sqlCommand(
      final String iExpectedCommand,
      String iReceivedCommand,
      final String iMessageSuccess,
      final boolean iIncludeResult) {
    final String iMessageFailure = "\nCommand failed.\n";
    checkForDatabase();

    if (iReceivedCommand == null) return null;

    iReceivedCommand = iExpectedCommand + " " + iReceivedCommand.trim();

    resetResultSet();

    final long start = System.currentTimeMillis();

    final Object result;
    try (OResultSet rs = currentDatabase.command(iReceivedCommand)) {
      result = rs.stream().map(x -> x.toElement()).collect(Collectors.toList());
    }
    float elapsedSeconds = getElapsedSecs(start);

    if (iIncludeResult) message(iMessageSuccess, result, elapsedSeconds);
    else message(iMessageSuccess, elapsedSeconds);

    return result;
  }
}
