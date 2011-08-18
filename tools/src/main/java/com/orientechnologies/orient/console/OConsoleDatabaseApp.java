/*
 * Copyright 1999-2010 Luca Garulli (l.garulli--at--orientechnologies.com)
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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.orientechnologies.common.console.TTYConsoleReader;
import com.orientechnologies.common.console.annotation.ConsoleCommand;
import com.orientechnologies.common.console.annotation.ConsoleParameter;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.orient.client.remote.OEngineRemote;
import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.client.remote.OStorageRemote;
import com.orientechnologies.orient.client.remote.OStorageRemoteThread;
import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.command.OCommandResultListener;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.tool.ODatabaseCompare;
import com.orientechnologies.orient.core.db.tool.ODatabaseExport;
import com.orientechnologies.orient.core.db.tool.ODatabaseExportException;
import com.orientechnologies.orient.core.db.tool.ODatabaseImport;
import com.orientechnologies.orient.core.db.tool.ODatabaseImportException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.intent.OIntentMassiveRead;
import com.orientechnologies.orient.core.iterator.ORecordIterator;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.security.OUser;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.ORecordSchemaAwareAbstract;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;
import com.orientechnologies.orient.core.record.impl.ORecordFlat;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializerFactory;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerStringAbstract;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLAsynchQuery;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.ODataHoleInfo;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocal;
import com.orientechnologies.orient.enterprise.command.script.OCommandScript;

public class OConsoleDatabaseApp extends OrientConsole implements OCommandOutputListener, OProgressListener {
	protected ODatabaseDocument		currentDatabase;
	protected String							currentDatabaseName;
	protected ORecordInternal<?>	currentRecord;
	protected List<OIdentifiable>	currentResultSet;
	protected OServerAdmin				serverAdmin;
	private int										lastPercentStep;
	private String								currentDatabaseUserName;
	private String								currentDatabaseUserPassword;

	public OConsoleDatabaseApp(final String[] args) {
		super(args);
	}

	public static void main(final String[] args) {
		try {
			boolean tty = false;
			try {
				if (setTerminalToCBreak())
					tty = true;

			} catch (Exception e) {
			}

			final OConsoleDatabaseApp console = new OConsoleDatabaseApp(args);
			if (tty)
				console.setReader(new TTYConsoleReader());

			console.run();

		} finally {
			try {
				stty("echo");
			} catch (Exception e) {
			}
		}
	}

	@Override
	protected void onBefore() {
		super.onBefore();

		currentResultSet = new ArrayList<OIdentifiable>();

		OGlobalConfiguration.STORAGE_KEEP_OPEN.setValue(false);

		properties.put("limit", "20");
		properties.put("debug", "false");
	}

	@Override
	protected void onAfter() {
		super.onAfter();
		Orient.instance().shutdown();
	}

	@ConsoleCommand(aliases = { "use database" }, description = "Connect to a database or a remote Server instance")
	public void connect(
			@ConsoleParameter(name = "url", description = "The url of the remote server or the database to connect in the format '<mode>:<path>'") String iURL,
			@ConsoleParameter(name = "user", description = "User name") String iUserName,
			@ConsoleParameter(name = "password", description = "User password") String iUserPassword) throws IOException {
		disconnect();

		currentDatabaseUserName = iUserName;
		currentDatabaseUserPassword = iUserPassword;

		if (iURL.contains("/")) {
			// OPEN DB
			out.print("Connecting to database [" + iURL + "] with user '" + iUserName + "'...");

			currentDatabase = new ODatabaseDocumentTx(iURL);
			if (currentDatabase == null)
				throw new OException("Database " + iURL + " not found");
			currentDatabase.open(iUserName, iUserPassword);

			currentDatabaseName = currentDatabase.getName();

			if (currentDatabase.getStorage() instanceof OStorageRemote)
				serverAdmin = new OServerAdmin((OStorageRemote) currentDatabase.getStorage());
		} else {
			// CONNECT TO REMOTE SERVER
			out.print("Connecting to remote Server instance [" + iURL + "] with user '" + iUserName + "'...");

			serverAdmin = new OServerAdmin(iURL).connect(iUserName, iUserPassword);
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
			@ConsoleParameter(name = "storage-type", description = "The type of the storage between 'local' for disk-based database and 'memory' for in memory only database") String iStorageType)
			throws IOException {
		out.println("Creating database [" + iDatabaseURL + "] using the storage type [" + iStorageType + "]...");

		currentDatabaseUserName = iUserName;
		currentDatabaseUserPassword = iUserPassword;

		if (iDatabaseURL.startsWith(OEngineRemote.NAME)) {
			// REMOTE CONNECTION
			final String dbURL = iDatabaseURL.substring(OEngineRemote.NAME.length() + 1);
			new OServerAdmin(dbURL).connect(iUserName, iUserPassword).createDatabase(iStorageType).close();
			connect(iDatabaseURL, OUser.ADMIN, OUser.ADMIN);

		} else {
			// LOCAL CONNECTION
			currentDatabase = new ODatabaseDocumentTx(iDatabaseURL);
			currentDatabase.create();
			currentDatabaseName = currentDatabase.getName();
		}

		out.println("Database created successfully.");
		out.println("\nCurrent database is: " + iDatabaseURL);
	}

	@ConsoleCommand(description = "Reload the database schema")
	public void reloadSchema() throws IOException {
		out.println("reloading database schema...");
		updateDatabaseInfo();
		out.println("\nDone.");
	}

	@ConsoleCommand(description = "Create a new cluster in the current database. The cluster can be physical or logical")
	public void createCluster(
			@ConsoleParameter(name = "cluster-name", description = "The name of the cluster to create") String iClusterName,
			@ConsoleParameter(name = "cluster-type", description = "Cluster type: 'physical' or 'logical'") String iClusterType,
			@ConsoleParameter(name = "position", description = "cluster id to replace an empty position or 'append' to append at the end") String iPosition) {
		checkCurrentDatabase();

		final int position = iPosition.toLowerCase().equals("append") ? -1 : Integer.parseInt(iPosition);

		out.println("Creating cluster [" + iClusterName + "] of type '" + iClusterType + "' in database " + currentDatabaseName
				+ (position == -1 ? " as last one" : " in place of #" + position) + "...");

		int clusterId = iClusterType.equalsIgnoreCase("physical") ? currentDatabase.addPhysicalCluster(iClusterName, iClusterName, -1)
				: currentDatabase.addLogicalCluster(iClusterName, currentDatabase.getClusterIdByName(OStorage.CLUSTER_INTERNAL_NAME));

		out.println((iClusterType.equalsIgnoreCase("physical") ? "Physical" : "Logical") + " cluster created correctly with id #"
				+ clusterId);
		updateDatabaseInfo();
	}

	@ConsoleCommand(description = "Remove a cluster in the current database. The cluster can be physical or logical")
	public void dropCluster(
			@ConsoleParameter(name = "cluster-name", description = "The name or the id of the cluster to remove") String iClusterName) {
		checkCurrentDatabase();

		out.println("Dropping cluster [" + iClusterName + "] in database " + currentDatabaseName + "...");

		boolean result = currentDatabase.dropCluster(iClusterName);

		if (!result) {
			// TRY TO GET AS CLUSTER ID
			try {
				int clusterId = Integer.parseInt(iClusterName);
				if (clusterId > -1) {
					result = currentDatabase.dropCluster(clusterId);
				}
			} catch (Exception e) {
			}
		}

		if (result)
			out.println("Cluster correctly removed");
		else
			out.println("Can't find the cluster to remove");
		updateDatabaseInfo();
	}

	@ConsoleCommand(splitInWords = false, description = "Alters a cluster in the current database. The cluster can be physical or logical")
	public void alterCluster(@ConsoleParameter(name = "command-text", description = "The command text to execute") String iCommandText) {
		sqlCommand("alter", iCommandText, "\nCluster updated successfully\n");
		updateDatabaseInfo();
	}

	@ConsoleCommand(description = "Shows the holes in current storage")
	public void showHoles() throws IOException {
		checkCurrentDatabase();

		if (!(currentDatabase.getStorage() instanceof OStorageLocal)) {
			out.println("Error: can't show holes in remote databases: connect as local");
			return;
		}

		final OStorageLocal storage = (OStorageLocal) currentDatabase.getStorage();

		out.println("List of holes in database " + currentDatabaseName + "...");

		out.println("--------------------------------------------------");
		out.println("Position             Size");
		out.println("--------------------------------------------------");

		final List<ODataHoleInfo> result = storage.getHolesList();

		for (ODataHoleInfo ppos : result) {
			out.printf("%20d %11d\n", ppos.dataOffset, ppos.size);
		}
		out.println("--------------------------------------------------");
	}

	@ConsoleCommand(description = "Begins a transaction. All the changes will remain local")
	public void begin() throws IOException {
		checkCurrentDatabase();

		if (currentDatabase.getTransaction().isActive()) {
			out.println("Error: an active transaction is running right now (id=" + currentDatabase.getTransaction().getId()
					+ "). Commit or rollback it before to start a new one.");
			return;
		}

		currentDatabase.begin();
		out.println("Transaction " + currentDatabase.getTransaction().getId() + " is running");
	}

	@ConsoleCommand(description = "Commits transaction changes to the database")
	public void commit() throws IOException {
		checkCurrentDatabase();

		if (!currentDatabase.getTransaction().isActive()) {
			out.println("Error: no active transaction is running right now.");
			return;
		}

		currentDatabase.commit();
		out.println("Transaction " + currentDatabase.getTransaction().getId() + " has been committed");
	}

	@ConsoleCommand(description = "Rollbacks transaction changes to the previous state")
	public void rollback() throws IOException {
		checkCurrentDatabase();

		if (!currentDatabase.getTransaction().isActive()) {
			out.println("Error: no active transaction is running right now.");
			return;
		}

		currentDatabase.rollback();
		out.println("Transaction " + currentDatabase.getTransaction().getId() + " has been rollbacked");
	}

	@ConsoleCommand(splitInWords = false, description = "Truncate the class content in the current database")
	public void truncateClass(@ConsoleParameter(name = "text", description = "The name of the class to truncate") String iCommandText) {
		sqlCommand("truncate", iCommandText, "\nTruncated %d record(s) in %f sec(s).\n");
	}

	@ConsoleCommand(splitInWords = false, description = "Truncate the cluster content in the current database")
	public void truncateCluster(
			@ConsoleParameter(name = "text", description = "The name of the class to truncate") String iCommandText) {
		sqlCommand("truncate", iCommandText, "\nTruncated %d record(s) in %f sec(s).\n");
	}

	@ConsoleCommand(splitInWords = false, description = "Truncate a record deleting it at low level")
	public void truncateRecord(@ConsoleParameter(name = "text", description = "The record(s) to truncate") String iCommandText) {
		sqlCommand("truncate", iCommandText, "\nTruncated %d record(s) in %f sec(s).\n");
	}

	@ConsoleCommand(description = "Load a record in memory using passed fetch plan")
	public void loadRecord(
			@ConsoleParameter(name = "record-id", description = "The unique Record Id of the record to load. If you don't have the Record Id execute a query first") String iRecordId,
			@ConsoleParameter(name = "fetch-plan", description = "The fetch plan to load the record with") String iFetchPlan) {
		loadRecordInternal(iRecordId, iFetchPlan);
	}

	@ConsoleCommand(description = "Load a record in memory and set it as the current one")
	public void loadRecord(
			@ConsoleParameter(name = "record-id", description = "The unique Record Id of the record to load. If you don't have the Record Id execute a query first") String iRecordId) {
		loadRecordInternal(iRecordId, null);
	}

	@ConsoleCommand(splitInWords = false, description = "Insert a new record into the database")
	public void insert(@ConsoleParameter(name = "command-text", description = "The command text to execute") String iCommandText) {
		sqlCommand("insert", iCommandText, "\nInserted record '%s' in %f sec(s).\n");
	}

	@ConsoleCommand(splitInWords = false, description = "Update records in the database")
	public void update(@ConsoleParameter(name = "command-text", description = "The command text to execute") String iCommandText) {
		sqlCommand("update", iCommandText, "\nUpdated %d record(s) in %f sec(s).\n");
		updateDatabaseInfo();
		currentDatabase.getLevel1Cache().invalidate();
		currentDatabase.getLevel2Cache().clear();
	}

	@ConsoleCommand(splitInWords = false, description = "Delete records from the database")
	public void delete(@ConsoleParameter(name = "command-text", description = "The command text to execute") String iCommandText) {
		sqlCommand("delete", iCommandText, "\nDelete %d record(s) in %f sec(s).\n");
		updateDatabaseInfo();
		currentDatabase.getLevel1Cache().invalidate();
		currentDatabase.getLevel2Cache().clear();
	}

	@ConsoleCommand(splitInWords = false, description = "Grant privileges to a role")
	public void grant(@ConsoleParameter(name = "text", description = "Grant command") String iCommandText) {
		sqlCommand("grant", iCommandText, "\nPrivilege granted to the role: %s\n");
	}

	@ConsoleCommand(splitInWords = false, description = "Revoke privileges to a role")
	public void revoke(@ConsoleParameter(name = "text", description = "Revoke command") String iCommandText) {
		sqlCommand("revoke", iCommandText, "\nPrivilege revoked to the role: %s\n");
	}

	@ConsoleCommand(splitInWords = false, description = "Create a link from a JOIN")
	public void createLink(@ConsoleParameter(name = "command-text", description = "The command text to execute") String iCommandText) {
		sqlCommand("create", iCommandText, "\nCreated %d link(s) in %f sec(s).\n");
	}

	@ConsoleCommand(splitInWords = false, description = "Find references in all database of target record given @rid")
	public void findReferences(
			@ConsoleParameter(name = "command-text", description = "The command text to execute") String iCommandText) {
		sqlCommand("find", iCommandText, "\nFound %s in %f sec(s).\n");
	}

	@ConsoleCommand(splitInWords = false, description = "Alter a database's property")
	public void alterDatabase(
			@ConsoleParameter(name = "command-text", description = "The command text to execute") String iCommandText) {
		sqlCommand("alter", iCommandText, "\nDatabase updated successfully\n");
		updateDatabaseInfo();
	}

	@ConsoleCommand(splitInWords = false, description = "Alter a class in the database schema")
	public void alterClass(@ConsoleParameter(name = "command-text", description = "The command text to execute") String iCommandText) {
		sqlCommand("alter", iCommandText, "\nClass updated successfully\n");
		updateDatabaseInfo();
	}

	@ConsoleCommand(splitInWords = false, description = "Create a class")
	public void createClass(@ConsoleParameter(name = "command-text", description = "The command text to execute") String iCommandText) {
		sqlCommand("create", iCommandText, "\nClass created successfully with id=%d\n");
		updateDatabaseInfo();
	}

	@ConsoleCommand(splitInWords = false, description = "Alter a class's property in the database schema")
	public void alterProperty(
			@ConsoleParameter(name = "command-text", description = "The command text to execute") String iCommandText) {
		sqlCommand("alter", iCommandText, "\nProperty updated successfully\n");
		updateDatabaseInfo();
	}

	@ConsoleCommand(splitInWords = false, description = "Create a property")
	public void createProperty(
			@ConsoleParameter(name = "command-text", description = "The command text to execute") String iCommandText) {
		sqlCommand("create", iCommandText, "\nProperty created successfully with id=%d\n");
		updateDatabaseInfo();
	}

	@ConsoleCommand(splitInWords = false, description = "Execute a query against the database and display the results")
	public void select(@ConsoleParameter(name = "query-text", description = "The query to execute") String iQueryText) {
		checkCurrentDatabase();

		if (iQueryText == null)
			return;

		iQueryText = iQueryText.trim();

		if (iQueryText.length() == 0 || iQueryText.equalsIgnoreCase("select"))
			return;

		iQueryText = "select " + iQueryText;

		currentResultSet.clear();

		final List<String> columns = new ArrayList<String>();

		final int limit = Integer.parseInt((String) properties.get("limit"));

		long start = System.currentTimeMillis();
		currentDatabase.query(new OSQLAsynchQuery<ODocument>(iQueryText, limit, new OCommandResultListener() {
			public boolean result(final Object iRecord) {
				final OIdentifiable record = (OIdentifiable) iRecord;

				dumpRecordInTable(currentResultSet.size(), record, columns);
				currentResultSet.add(record);
				return true;
			}

		}).setFetchPlan("*:1"));

		if (currentResultSet.size() > 0 && (limit == -1 || currentResultSet.size() < limit))
			printHeaderLine(columns);

		out.println("\n" + currentResultSet.size() + " item(s) found. Query executed in "
				+ (float) (System.currentTimeMillis() - start) / 1000 + " sec(s).");
	}

	@ConsoleCommand(splitInWords = false, description = "Execute a script against the current database. If the database is remote, then the script will be executed remotely")
	public void script(@ConsoleParameter(name = "script-text", description = "The script text to execute") final String iScriptText) {
		checkCurrentDatabase();

		if (iScriptText == null)
			return;

		long start = System.currentTimeMillis();

		currentResultSet.clear();

		Object result = new OCommandScript("Javascript", iScriptText).execute();

		out.printf("Script executed in %f sec(s). Value returned is: %s", (float) (System.currentTimeMillis() - start) / 1000, result);
	}

	@ConsoleCommand(splitInWords = false, description = "Create an index against a property")
	public void createIndex(@ConsoleParameter(name = "command-text", description = "The command text to execute") String iCommandText)
			throws IOException {
		out.println("\nCreating index...");

		sqlCommand("create", iCommandText, "\nCreated index successfully with %d entries in %f sec(s).\n");
		updateDatabaseInfo();
		out.println("\nIndex created successfully");
	}

	@ConsoleCommand(description = "Delete the current database")
	public void dropDatabase() throws IOException {
		checkCurrentDatabase();

		final String dbName = currentDatabase.getName();

		if (currentDatabase.getURL().startsWith(OEngineRemote.NAME)) {
			if (serverAdmin == null) {
				out.println("\nCan't drop a remote database without connecting to the server with a valid server's user");
				return;
			}

			// REMOTE CONNECTION
			final String dbURL = currentDatabase.getURL().substring(OEngineRemote.NAME.length() + 1);
			new OServerAdmin(dbURL).connect(currentDatabaseUserName, currentDatabaseUserPassword).dropDatabase();
		} else {
			// LOCAL CONNECTION
			currentDatabase.delete();
			currentDatabase = null;
		}

		out.println("\nDatabase '" + dbName + "' deleted successfully");
	}

	@ConsoleCommand(description = "Delete the specified database")
	public void dropDatabase(
			@ConsoleParameter(name = "database-url", description = "The url of the database to drop in the format '<mode>:<path>'") String iDatabaseURL,
			@ConsoleParameter(name = "user", description = "Server administrator name") String iUserName,
			@ConsoleParameter(name = "password", description = "Server administrator password") String iUserPassword) throws IOException {

		if (iDatabaseURL.startsWith(OEngineRemote.NAME)) {
			// REMOTE CONNECTION
			final String dbURL = iDatabaseURL.substring(OEngineRemote.NAME.length() + 1);
			serverAdmin = new OServerAdmin(dbURL).connect(iUserName, iUserPassword);
			serverAdmin.dropDatabase();
			disconnect();
		} else {
			// LOCAL CONNECTION
			currentDatabase = new ODatabaseDocumentTx(iDatabaseURL);
			currentDatabase.delete();
			currentDatabase = null;
		}

		out.println("\nDatabase '" + iDatabaseURL + "' deleted successfully");
	}

	@ConsoleCommand(splitInWords = false, description = "Remove an index")
	public void dropIndex(@ConsoleParameter(name = "command-text", description = "The command text to execute") String iCommandText)
			throws IOException {
		out.println("\nRemoving index...");

		sqlCommand("drop", iCommandText, "\nRemoved index %d link(s) in %f sec(s).\n");
		updateDatabaseInfo();
		out.println("\nIndex removed successfully");
	}

	@ConsoleCommand(splitInWords = false, description = "rebuild an index if it's automatic")
	public void rebuildIndex(@ConsoleParameter(name = "command-text", description = "The command text to execute") String iCommandText)
			throws IOException {
		out.println("\nRebuilding index(es)...");

		sqlCommand("rebuild", iCommandText, "\nRebuilt index(es). Found %d link(s) in %f sec(s).\n");
		updateDatabaseInfo();
		out.println("\nIndex(es) rebuilt successfully");
	}

	@ConsoleCommand(splitInWords = false, description = "Remove a class from the schema")
	public void dropClass(@ConsoleParameter(name = "command-text", description = "The command text to execute") String iCommandText)
			throws IOException {
		sqlCommand("drop", iCommandText, "\nRemoved class in %f sec(s).\n");
		updateDatabaseInfo();
		out.println("\nClass removed successfully");
	}

	@ConsoleCommand(splitInWords = false, description = "Remove a property from a class")
	public void dropProperty(@ConsoleParameter(name = "command-text", description = "The command text to execute") String iCommandText)
			throws IOException {
		sqlCommand("drop", iCommandText, "\nRemoved class property in %f sec(s).\n");
		updateDatabaseInfo();
		out.println("\nClass property removed successfully");
	}

	@ConsoleCommand(description = "Browse all the records of a class")
	public void browseClass(@ConsoleParameter(name = "class-name", description = "The name of the class") final String iClassName) {
		checkCurrentDatabase();

		currentResultSet.clear();

		final List<String> columns = new ArrayList<String>();

		final int limit = Integer.parseInt((String) properties.get("limit"));

		ORecordIterator<?> it = currentDatabase.browseClass(iClassName);

		browseRecords(columns, limit, it);
	}

	@ConsoleCommand(description = "Browse all the records of a cluster")
	public void browseCluster(
			@ConsoleParameter(name = "cluster-name", description = "The name of the cluster") final String iClusterName) {
		checkCurrentDatabase();

		currentResultSet.clear();

		final List<String> columns = new ArrayList<String>();

		final int limit = Integer.parseInt((String) properties.get("limit"));

		ORecordIterator<?> it = currentDatabase.browseCluster(iClusterName);

		browseRecords(columns, limit, it);
	}

	@ConsoleCommand(aliases = { "display" }, description = "Display current record's attributes")
	public void displayRecord(
			@ConsoleParameter(name = "number", description = "The number of the record in the last result set") final String iRecordNumber) {
		checkCurrentDatabase();

		if (iRecordNumber == null)
			checkCurrentObject();
		else {
			int recNumber = Integer.parseInt(iRecordNumber);
			if (currentResultSet.size() == 0)
				throw new OException("No result set where to find the requested record. Execute a query first.");

			if (currentResultSet.size() <= recNumber)
				throw new OException("The record requested is not part of current result set (0"
						+ (currentResultSet.size() > 0 ? "-" + (currentResultSet.size() - 1) : "") + ")");

			currentRecord = (ORecordInternal<?>) currentResultSet.get(recNumber).getRecord();
		}

		dumpRecordDetails();
	}

	@ConsoleCommand(description = "Display a record as raw bytes")
	public void displayRawRecord(@ConsoleParameter(name = "rid", description = "The record id to display") final String iRecordId) {
		checkCurrentDatabase();

		ORecordId rid = new ORecordId(iRecordId);
		final ORawBuffer buffer = currentDatabase.getStorage().readRecord(currentDatabase, rid, null);

		if (buffer == null)
			throw new OException("The record has been deleted");

		out.println("Raw record content (size=" + buffer.buffer.length + "):\n\n" + new String(buffer.buffer));
	}

	@ConsoleCommand(aliases = { "status" }, description = "Display information about the database")
	public void info() {
		if (currentDatabaseName != null) {
			out.println("Current database: " + currentDatabaseName + " (url=" + currentDatabase.getURL() + ")");

			OStorage stg = currentDatabase.getStorage();

			out.println("\nTotal size: " + OFileUtils.getSizeAsString(stg.getSize()));

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

			clusters();
			classes();
			indexes();
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
			out.println("! Class '" + iClassName + "' doesn't exist in the database '" + currentDatabaseName + "'");
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

		if (cls.getBaseClasses() != null) {
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
			out.println("-------------------------------+-------------+-------------------------------+-----------+-----------+----------+------+------+");
			out.println(" NAME                          | TYPE        | LINKED TYPE/CLASS             | INDEX     | MANDATORY | NOT NULL | MIN  | MAX  |");
			out.println("-------------------------------+-------------+-------------------------------+-----------+-----------+----------+------+------+");

			for (OProperty p : cls.properties()) {
				try {
					out.printf(" %-30s| %-12s| %-30s| %-10s| %-10s| %-9s| %-5s| %-5s|\n", p.getName(), p.getType(),
							p.getLinkedClass() != null ? p.getLinkedClass() : p.getLinkedType(), p.getIndex() != null ? p.getIndex()
									.getUnderlying().getType() : "", p.isMandatory(), p.isNotNull(), p.getMin() != null ? p.getMin() : "",
							p.getMax() != null ? p.getMax() : "");
				} catch (Exception e) {
				}
			}
			out.println("-------------------------------+-------------+-------------------------------+-----------+-----------+----------+------+------+");
		}
	}

	@ConsoleCommand(description = "Display all indexes")
	public void indexes() {
		if (currentDatabaseName != null) {
			out.println("\nINDEXES:");
			out.println("----------------------------------------------+------------+----+-----------+");
			out.println(" NAME                                         | TYPE       |AUTO| RECORDS   |");
			out.println("----------------------------------------------+------------+----+-----------+");

			int totalIndexes = 0;
			long totalRecords = 0;
			for (OIndex<?> index : currentDatabase.getMetadata().getIndexManager().getIndexes()) {
				try {
					out.printf(" %-45s| %-10s | %1s  |%10d |\n", index.getName(), index.getType(), index.isAutomatic() ? "Y" : "N",
							index.getSize());

					totalIndexes++;
					totalRecords += index.getSize();
				} catch (Exception e) {
				}
			}
			out.println("----------------------------------------------+------------+----+-----------+");
			out.printf(" TOTAL = %-3d                                                %15d |\n", totalIndexes, totalRecords);
			out.println("----------------------------------------------------------------------------+\n");
		} else
			out.println("No database selected yet.");
	}

	@ConsoleCommand(description = "Display all the configured clusters")
	public void clusters() {
		if (currentDatabaseName != null) {
			out.println("\nCLUSTERS:");
			out.println("----------------------------------------------+------+---------------------+-----------+");
			out.println(" NAME                                         |  ID  | TYPE                | RECORDS   |");
			out.println("----------------------------------------------+------+---------------------+-----------+");

			int clusterId;
			String clusterType = null;
			long totalElements = 0;
			long count;
			for (String clusterName : currentDatabase.getClusterNames()) {
				try {
					clusterId = currentDatabase.getClusterIdByName(clusterName);
					clusterType = currentDatabase.getClusterType(clusterName);
					count = currentDatabase.countClusterElements(clusterName);
					totalElements += count;
					out.printf(" %-45s|%6d| %-20s|%10d |\n", clusterName, clusterId, clusterType, count);
				} catch (Exception e) {
				}
			}
			out.println("----------------------------------------------+------+---------------------+-----------+");
			out.printf(" TOTAL                                                                 %15d |\n", totalElements);
			out.println("---------------------------------------------------------------------------------------+");
		} else
			out.println("No database selected yet.");
	}

	@ConsoleCommand(description = "Display all the configured classes")
	public void classes() {
		if (currentDatabaseName != null) {
			out.println("\nCLASSES:");
			out.println("----------------------------------------------+---------------------+-----------+");
			out.println(" NAME                                         | CLUSTERS            | RECORDS   |");
			out.println("----------------------------------------------+---------------------+-----------+");

			long totalElements = 0;
			long count;
			for (OClass cls : currentDatabase.getMetadata().getSchema().getClasses()) {
				try {
					StringBuilder clusters = new StringBuilder();
					for (int i = 0; i < cls.getClusterIds().length; ++i) {
						if (i > 0)
							clusters.append(", ");
						clusters.append(cls.getClusterIds()[i]);
					}

					count = currentDatabase.countClass(cls.getName());
					totalElements += count;

					out.printf(" %-45s| %-20s|%10d |\n", cls.getName(), clusters, count);
				} catch (Exception e) {
				}
			}
			out.println("----------------------------------------------+---------------------+-----------+");
			out.printf(" TOTAL                                                          %15d |\n", totalElements);
			out.println("--------------------------------------------------------------------------------+");

		} else
			out.println("No database selected yet.");
	}

	@ConsoleCommand(description = "Display all the keys in the database dictionary")
	public void dictionaryKeys() {
		checkCurrentDatabase();

		Iterable<Object> keys = currentDatabase.getDictionary().keys();

		int i = 0;
		for (Object k : keys) {
			out.print(String.format("#%d: %s\n", i++, k));
		}

		out.println("Found " + i + " keys:");
	}

	@ConsoleCommand(description = "Loookup for a record using the dictionary. If found set it as the current record")
	public void dictionaryGet(@ConsoleParameter(name = "key", description = "The key to search") final String iKey) {
		checkCurrentDatabase();

		currentRecord = currentDatabase.getDictionary().get(iKey);
		if (currentRecord == null)
			out.println("Entry not found in dictionary.");
		else {
			currentRecord = (ORecordInternal<?>) currentRecord.load();
			displayRecord(null);
		}
	}

	@ConsoleCommand(description = "Insert or modify an entry in the database dictionary. The entry is composed by key=String, value=record-id")
	public void dictionaryPut(
			@ConsoleParameter(name = "key", description = "The key to bind") final String iKey,
			@ConsoleParameter(name = "record-id", description = "The record-id of the record to bind to the key passes") final String iRecordId) {
		checkCurrentDatabase();

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
		checkCurrentDatabase();

		boolean result = currentDatabase.getDictionary().remove(iKey);
		if (!result)
			out.println("Entry not found in dictionary.");
		else
			out.println("Entry removed from the dictionary.");
	}

	@ConsoleCommand(description = "Share a database with a remote server")
	public void shareDatabase(
			@ConsoleParameter(name = "db-name", description = "Name of the database to share") final String iDatabaseName,
			@ConsoleParameter(name = "db-user", description = "Database user") final String iDatabaseUserName,
			@ConsoleParameter(name = "db-password", description = "Database password") String iDatabaseUserPassword,
			@ConsoleParameter(name = "server-name", description = "Remote server's name as <address>:<port>") final String iRemoteName,
			@ConsoleParameter(name = "mode", description = "replication mode: 'synch' or 'asynch'") final String iMode)
			throws IOException {

		try {
			if (serverAdmin == null)
				throw new IllegalStateException("You must be connected to a remote server to share a database");

			serverAdmin.shareDatabase(iDatabaseName, iDatabaseUserName, iDatabaseUserPassword, iRemoteName,
					iMode.equalsIgnoreCase("synch"));

			out.println("Database '" + iDatabaseName + "' has been shared in '" + iMode + "' mode with the server '" + iRemoteName + "'");

		} catch (Exception e) {
			printError(e);
		}
	}

	@ConsoleCommand(description = "Compare two databases")
	public void compareDatabases(@ConsoleParameter(name = "db1-url", description = "URL of the first database") final String iDb1URL,
			@ConsoleParameter(name = "db2-url", description = "URL of the second database") final String iDb2URL) throws IOException {
		try {
			new ODatabaseCompare(iDb1URL, iDb2URL, this).compare();
		} catch (ODatabaseExportException e) {
			printError(e);
		}
	}

	@ConsoleCommand(description = "Export a database")
	public void exportDatabase(@ConsoleParameter(name = "output-file", description = "Output file path") final String iOutputFilePath)
			throws IOException {
		checkCurrentDatabase();

		out.println("Exporting current database to: " + iOutputFilePath + "...");

		try {
			new ODatabaseExport(currentDatabase, iOutputFilePath, this).exportDatabase().close();
		} catch (ODatabaseExportException e) {
			printError(e);
		}
	}

	@ConsoleCommand(description = "Import a database into the current one")
	public void importDatabase(@ConsoleParameter(name = "input-file", description = "Input file path") final String iInputFilePath)
			throws IOException {
		checkCurrentDatabase();

		out.println("Importing database from file " + iInputFilePath + "...");

		try {
			new ODatabaseImport(currentDatabase, iInputFilePath, this).importDatabase().close();
		} catch (ODatabaseImportException e) {
			printError(e);
		}
	}

	@ConsoleCommand(description = "Export the current record in the requested format")
	public void exportRecord(@ConsoleParameter(name = "format", description = "Format, such as 'json'") final String iFormat)
			throws IOException {
		checkCurrentDatabase();
		checkCurrentObject();

		final ORecordSerializer serializer = ORecordSerializerFactory.instance().getFormat(iFormat.toLowerCase());

		if (serializer == null) {
			out.println("ERROR: Format '" + iFormat + "' was not found.");
			printSupportedSerializerFormat();
			return;
		} else if (!(serializer instanceof ORecordSerializerStringAbstract)) {
			out.println("ERROR: Format '" + iFormat + "' doesn't export as text.");
			printSupportedSerializerFormat();
			return;
		}

		try {
			out.println(((ORecordSerializerStringAbstract) serializer).toString(currentRecord, null));
		} catch (ODatabaseExportException e) {
			printError(e);
		}
	}

	@ConsoleCommand(description = "Return all the configured properties")
	public void properties() {
		out.println("PROPERTIES:");
		out.println("+---------------------+----------------------+");
		out.printf("| %-30s| %-30s |\n", "NAME", "VALUE");
		out.println("+---------------------+----------------------+");
		for (Entry<String, Object> p : properties.entrySet()) {
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
		checkCurrentDatabase();

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
			OProfiler.getInstance().startRecording();
			out.println("Profiler is ON now, use 'profiler off' to turn off.");
		} else if (iCommandName.equalsIgnoreCase("off")) {
			OProfiler.getInstance().stopRecording();
			out.println("Profiler is OFF now, use 'profiler on' to turn on.");
		} else if (iCommandName.equalsIgnoreCase("dump")) {
			out.println(OProfiler.getInstance().dump());
		} else if (iCommandName.equalsIgnoreCase("reset")) {
			OProfiler.getInstance().reset();
			out.println("Profiler has been resetted");
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
		}
	}

	@ConsoleCommand(description = "Change the value of a configuration value")
	public void configSet(
			@ConsoleParameter(name = "config-name", description = "Name of the configuration") final String iConfigName,
			@ConsoleParameter(name = "config-value", description = "Value to set") final String iConfigValue) throws IOException {
		final OGlobalConfiguration config = OGlobalConfiguration.findByKey(iConfigName);
		if (config == null)
			throw new IllegalArgumentException("Configuration variable '" + iConfigName + "' wasn't found");

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

	protected void loadRecordInternal(String iRecordId, String iFetchPlan) {
		checkCurrentDatabase();

		currentRecord = currentDatabase.load(new ORecordId(iRecordId), iFetchPlan);
		displayRecord(null);

		out.println("OK");
	}

	protected void checkCurrentDatabase() {
		if (currentDatabase == null)
			throw new OException("Database not selected. Use 'connect <database-name>' to connect to a database.");
		if (currentDatabase.isClosed())
			throw new OException("Database is closed");
	}

	protected void checkCurrentObject() {
		if (currentRecord == null)
			throw new OException("The is no current object selected: create a new one or load it");
	}

	protected void dumpRecordInTable(final int iIndex, final OIdentifiable iRecord, final List<String> iColumns) {
		// CHECK IF HAVE TO ADD NEW COLUMN (BECAUSE IT CAN BE SCHEMA-LESS)
		final List<String> recordColumns = new ArrayList<String>();
		if (iRecord instanceof ODocument)
			for (String fieldName : ((ODocument) iRecord).fieldNames())
				recordColumns.add(fieldName);

		dumpRecordInTable(iIndex, iRecord, recordColumns, iColumns);
	}

	protected void dumpRecordInTable(final int iIndex, final OIdentifiable iRecord, final List<String> iRecordColumns,
			final List<String> iColumns) {
		// CHECK IF HAVE TO ADD NEW COLUMN (BECAUSE IT CAN BE SCHEMA-LESS)
		for (String fieldName : iRecordColumns) {
			boolean foundCol = false;
			for (String colName : iColumns) {
				if (fieldName.equals(colName)) {
					foundCol = true;
					break;
				}
			}

			if (!foundCol)
				// NEW COLUMN: ADD IT
				iColumns.add(fieldName);
		}

		if (iIndex == 0) {
			out.printf("\n");
			printHeaderLine(iColumns);
			out.print("  #| RID     |");
			int col = 0;
			for (String colName : iColumns) {
				if (col++ > 0)
					out.printf("|");
				out.printf("%-20s", colName);
			}
			out.printf("\n");
			printHeaderLine(iColumns);
		}

		// FORMAT THE LINE DYNAMICALLY
		StringBuilder format = new StringBuilder("%3d|%9s");
		List<Object> vargs = new ArrayList<Object>();
		vargs.add(iIndex);

		if (iRecord.getIdentity().isValid())
			vargs.add(iRecord.getIdentity());
		else
			vargs.add("");

		try {
			Object value = null;

			if (iRecord instanceof ODocument)
				((ODocument) iRecord).setLazyLoad(false);

			for (String colName : iColumns) {
				format.append("|%-20s");

				if (iRecord instanceof ORecordSchemaAwareAbstract<?>)
					value = ((ORecordSchemaAwareAbstract<?>) iRecord).field(colName);

				if (value instanceof Collection<?>)
					value = "[" + ((Collection<?>) value).size() + "]";
				else if (value instanceof ORecord<?>)
					value = ((ORecord<?>) value).getIdentity().toString();
				else if (value instanceof Date)
					value = currentDatabase.getStorage().getConfiguration().getDateTimeFormatInstance().format((Date) value);
				else if (value instanceof byte[])
					value = "byte[" + ((byte[]) value).length + "]";

				vargs.add(value);
			}

			out.println(String.format(format.toString(), vargs.toArray()));
		} catch (Throwable t) {
			out.printf("%3d|%9s|%s\n", iIndex, iRecord.getIdentity(), "Error on loading record dued to: " + t);
		}
	}

	private void printHeaderLine(final List<String> iColumns) {
		out.print("---+---------");
		if (iColumns.size() > 0) {
			for (int i = 0; i < iColumns.size(); ++i) {
				out.print("+");
				for (int k = 0; k < 20; ++k)
					out.print("-");
			}
		}
		out.print("\n");
	}

	private void dumpRecordDetails() {
		if (currentRecord instanceof ODocument) {
			ODocument rec = (ODocument) currentRecord;
			out.println("--------------------------------------------------");
			out.printf("ODocument - Class: %s   id: %s   v.%d\n", rec.getClassName(), rec.getIdentity().toString(), rec.getVersion());
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
			out.printf("Flat - record id: %s   v.%d\n", rec.getIdentity().toString(), rec.getVersion());
			out.println("--------------------------------------------------");
			out.print(rec.value());

		} else if (currentRecord instanceof ORecordBytes) {
			ORecordBytes rec = (ORecordBytes) currentRecord;
			out.println("--------------------------------------------------");
			out.printf("Flat - record id: %s   v.%d\n", rec.getIdentity().toString(), rec.getVersion());
			out.println("--------------------------------------------------");
			byte[] value = rec.toStream();
			for (int i = 0; i < Array.getLength(value); ++i) {
				out.printf("%03d", Array.getByte(value, i));
			}

		} else {
			out.println("--------------------------------------------------");
			out.printf("%s - record id: %s   v.%d\n", currentRecord.getClass().getSimpleName(), currentRecord.getIdentity().toString(),
					currentRecord.getVersion());
		}
		out.println();
	}

	public void onMessage(String iText) {
		out.print(iText);
	}

	private void printSupportedSerializerFormat() {
		out.println("Supported formats are:");

		for (ORecordSerializer s : ORecordSerializerFactory.instance().getFormats()) {
			if (s instanceof ORecordSerializerStringAbstract)
				out.println("- " + s.toString());
		}
	}

	private void browseRecords(final List<String> columns, final int limit, ORecordIterator<?> it) {
		while (it.hasNext()) {
			currentRecord = it.next();

			try {
				if (currentRecord instanceof ORecordSchemaAwareAbstract<?>)
					dumpRecordInTable(currentResultSet.size(), (ORecordSchemaAwareAbstract<?>) currentRecord, columns);
				else if (currentRecord != null) {
					dumpRecordDetails();
					out.println();
				}

				currentResultSet.add(currentRecord);
			} catch (Exception e) {
				out.printf("\n!Error on displaying record " + currentRecord.getIdentity() + ". Cause: " + e.getMessage());
			}

			if (limit > -1 && currentResultSet.size() >= limit) {
				printHeaderLine(columns);
				out.println("\nResultset contains more items not displayed (max=" + limit + ")");
				return;
			}
		}

		printHeaderLine(columns);
	}

	private Object sqlCommand(final String iExpectedCommand, String iReceivedCommand, final String iMessage) {
		checkCurrentDatabase();

		if (iReceivedCommand == null)
			return null;

		long start = System.currentTimeMillis();

		iReceivedCommand = iExpectedCommand + " " + iReceivedCommand.trim();

		currentResultSet.clear();

		final Object result = new OCommandSQL(iReceivedCommand).setDatabase(currentDatabase).setProgressListener(this).execute();

		if (result != null)
			out.printf(iMessage, result, (float) (System.currentTimeMillis() - start) / 1000);

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
	private static int exec(final String[] cmd) throws IOException, InterruptedException {
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

	private void printError(final Exception e) {
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

	private void updateDatabaseInfo() {
		currentDatabase.getMetadata().getSchema().reload();
	}
}
