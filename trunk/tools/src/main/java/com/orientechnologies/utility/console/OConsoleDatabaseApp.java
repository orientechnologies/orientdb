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
package com.orientechnologies.utility.console;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.orientechnologies.common.console.annotation.ConsoleCommand;
import com.orientechnologies.common.console.annotation.ConsoleParameter;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.client.admin.OServerAdmin;
import com.orientechnologies.orient.core.db.vobject.ODatabaseVObject;
import com.orientechnologies.orient.core.db.vobject.ODatabaseVObjectTx;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.query.OAsynchQueryResultListener;
import com.orientechnologies.orient.core.query.sql.OSQLAsynchQuery;
import com.orientechnologies.orient.core.record.impl.ORecordVObject;
import com.orientechnologies.utility.impexp.OConsoleDatabaseExport;
import com.orientechnologies.utility.impexp.ODatabaseExportException;

public class OConsoleDatabaseApp extends OrientConsole implements OCommandListener {
	protected ODatabaseVObject			currentDatabase;
	protected String								currentDatabaseName;
	protected ORecordVObject				currentRecord;
	protected List<ORecordVObject>	currentResultSet;

	private static final int				RESULTSET_LIMIT	= 20;

	public static void main(String[] args) {
		new OConsoleDatabaseApp(args);
	}

	public OConsoleDatabaseApp(String[] args) {
		super(args);
	}

	@ConsoleCommand(aliases = { "use database" }, description = "Connect to a database")
	public void connect(
			@ConsoleParameter(name = "database-url", description = "The url of the database to connect") String iDatabaseURL) {
		out.print("Connecting to database [" + iDatabaseURL + "]...");

		currentDatabase = new ODatabaseVObjectTx(iDatabaseURL);
		if (currentDatabase == null)
			throw new OException("Database " + iDatabaseURL + " not found.");
		currentDatabase.open("admin", "admin");

		currentDatabaseName = currentDatabase.getName();

		out.println("OK");
	}

	@ConsoleCommand(aliases = { "close database" }, description = "Disconnect from the current database")
	public void disconnect() {
		checkCurrentDatabase();

		out.print("Disconnecting from the database [" + currentDatabaseName + "]...");

		currentDatabase.close();
		currentDatabase = null;
		currentDatabaseName = null;
		currentRecord = null;

		out.println("OK");
	}

	@ConsoleCommand(description = "Create a new database")
	public void createDatabase(
			@ConsoleParameter(name = "database-name", description = "The name of the database to create") String iDatabaseName)
			throws IOException {
		out.println("Creating database [" + iDatabaseName + "] in current server node...");

		new OServerAdmin(currentDatabaseName).createDatabase(iDatabaseName, iDatabaseName, "csv").close();

		out.println("OK.");

		connect(iDatabaseName);
	}

	@ConsoleCommand(description = "Create a new cluster inside a database")
	public void createCluster(
			@ConsoleParameter(name = "cluster-name", description = "The name of the cluster to create") String iClassName) {
		checkCurrentDatabase();

		out.println("Creating class [" + iClassName + "] in database " + currentDatabaseName + "...");

		int clusterId = currentDatabase.getStorage().addCluster(iClassName);

		out.println("OK. Assigned id #" + clusterId);
	}

	@ConsoleCommand(description = "Create a new record")
	public void createRecord(@ConsoleParameter(name = "cluster-name", description = "The name of the cluster") String iClusterName,
			@ConsoleParameter(name = "content", description = "Record's content") String iContent) {
		checkCurrentDatabase();

		out.println("Creating a new record in cluster [" + iClusterName + "]...");

		currentRecord = currentDatabase.newInstance(iContent);

		out.println("OK");
	}

	@ConsoleCommand(description = "Load a record in memory and set it as the current one.")
	public void loadRecord(
			@ConsoleParameter(name = "cluster-name", description = "The name of the cluster") String iClusterName,
			@ConsoleParameter(name = "oid", description = "The unique Record Id to load the object directly. If you don't have the Record Id use the query system") String iRecordId) {
		checkCurrentDatabase();

		currentRecord = currentDatabase.load(new ORecordId(iRecordId));
		displayObject(null);

		out.println("OK");
	}

	@ConsoleCommand(splitInWords = false, description = "Execute a query against the database and display the results")
	public void select(@ConsoleParameter(name = "query-text", description = "The query to execute") String iQuery) {
		checkCurrentDatabase();

		if (iQuery == null)
			return;

		iQuery = iQuery.trim();

		if (iQuery.length() == 0)
			return;

		iQuery = "select " + iQuery;

		currentResultSet = new ArrayList<ORecordVObject>();

		final List<String> columns = new ArrayList<String>();

		long start = System.currentTimeMillis();
		currentDatabase.query(new OSQLAsynchQuery<ORecordVObject>(iQuery, new OAsynchQueryResultListener<ORecordVObject>() {
			public boolean result(final ORecordVObject iRecord) {
				if (currentResultSet.size() > RESULTSET_LIMIT) {
					printHeaderLine(columns);
					out.println("\nResultset contains more items not displayed (max=" + RESULTSET_LIMIT + ")");
					return false;
				}

				dumpRecordInTable(currentResultSet.size(), iRecord, columns);
				currentResultSet.add(iRecord);
				return true;
			}

		})).execute(RESULTSET_LIMIT + 1);

		if (currentResultSet.size() > 0 && currentResultSet.size() <= RESULTSET_LIMIT)
			printHeaderLine(columns);

		out.println(currentResultSet.size() + " item(s) found. Query executed in " + (float) (System.currentTimeMillis() - start)
				/ 1000 + " sec(s).");

	}

	@ConsoleCommand(aliases = { "display" }, description = "Display current object's attributes")
	public void displayObject(
			@ConsoleParameter(name = "number", description = "The number of the record in the last result set") final String iRecordNumber) {
		checkCurrentDatabase();

		if (iRecordNumber == null)
			checkCurrentObject();
		else {
			int recNumber = Integer.parseInt(iRecordNumber);
			if (currentResultSet == null)
				throw new OException("No result set where to find the requested record. Execute a query first.");

			if (currentResultSet.size() <= recNumber)
				throw new OException("The record requested is not part of current result set (0"
						+ (currentResultSet.size() > 0 ? "-" + (currentResultSet.size() - 1) : "") + ")");

			currentRecord = currentResultSet.get(recNumber);
		}

		dumpRecordDetails();
	}

	@ConsoleCommand(aliases = { "status" }, description = "Display information about current status")
	public void info() {
		if (currentDatabaseName != null) {
			out.println("Current database: " + currentDatabaseName);
			clusters();
			classes();
		}
	}

	@ConsoleCommand(description = "Display all the configured clusters")
	public void clusters() {
		if (currentDatabaseName != null) {
			out.println("CLUSTERS:");
			out.println("--------------------+------+--------------------+-----------+");
			out.println("NAME                |  ID  | TYPE               | ELEMENTS  |");
			out.println("--------------------+------+--------------------+-----------+");

			int clusterId;
			long totalElements = 0;
			long count;
			for (String clusterName : currentDatabase.getClusterNames()) {
				clusterId = currentDatabase.getClusterIdByName(clusterName);
				count = currentDatabase.countClusterElements(clusterName);
				totalElements += count;

				out.printf("%-20s|%6d|%-20s|%10d |\n", clusterName, clusterId, clusterId < -1 ? "Logical" : "Physical", count);

			}
			out.println("--------------------+------+--------------------+-----------+");
			out.printf("TOTAL                                            %10d |\n", totalElements);
			out.println("------------------------------------------------------------+\n");
		} else
			out.println("No database selected yet.");
	}

	@ConsoleCommand(description = "Display all the configured classes")
	public void classes() {
		if (currentDatabaseName != null) {
			out.println("CLASSES:");
			out.println("--------------------+------+------------------------------------------+-----------+");
			out.println("NAME                |  ID  | CLUSTERS                                 | ELEMENTS  |");
			out.println("--------------------+------+------------------------------------------+-----------+");

			long totalElements = 0;
			long count;
			for (OClass cls : currentDatabase.getMetadata().getSchema().getClasses()) {
				StringBuilder clusters = new StringBuilder();
				for (int i = 0; i < cls.getClusterIds().length; ++i) {
					if (i > 0)
						clusters.append(", ");
					clusters.append(currentDatabase.getClusterNameById(cls.getClusterIds()[i]));
				}

				count = currentDatabase.countClass(cls.getName());
				totalElements += count;

				out.printf("%-20s|%6d| %-40s |%10d |\n", cls.getName(), cls.getId(), clusters, count);
			}
			out.println("--------------------+------+------------------------------------------+-----------+");
			out.printf("TOTAL                                                                  %10d |\n", totalElements);
			out.println("----------------------------------------------------------------------------------+");

		} else
			out.println("No database selected yet.");
	}

	@ConsoleCommand(description = "Loookup for a record using the dictionary and if found set it as the current one.")
	public void lookup(@ConsoleParameter(name = "key", description = "The key to search") final String iKey) {
		checkCurrentDatabase();

		currentRecord = (ORecordVObject) currentDatabase.getDictionary().get(iKey);
		if (currentRecord == null)
			out.println("Entry not found in dictionary.");
		else
			displayObject(null);
	}

	@ConsoleCommand(description = "Export a database")
	public void export(@ConsoleParameter(name = "output-file", description = "Output file path") final String iOutputFilePath)
			throws IOException {
		out.println("Exporting current database to: " + iOutputFilePath + "...");

		try {
			new OConsoleDatabaseExport(currentDatabase, iOutputFilePath, this).exportDatabase();
		} catch (ODatabaseExportException e) {
			out.println("ERROR: " + e.toString());
		}
	}

	@Override
	public String toString() {
		return "database";
	}

	protected void checkCurrentDatabase() {
		if (currentDatabase == null)
			throw new OException("Database not selected. Use 'connect <database-name>' to connect to a database.");
	}

	protected void checkCurrentObject() {
		if (currentRecord == null)
			throw new OException("The is no current object selected: create a new one or load it");
	}

	protected void dumpRecordInTable(final int iIndex, final ORecordVObject iRecord, final List<String> iColumns) {
		// CHECK IF HAVE TO ADD NEW COLUMN (BECAUSE IT CAN BE SCHEMA-LESS)
		for (String fieldName : iRecord.fields()) {
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
			out.print("  #| REC ID |");
			int col = 0;
			for (String colName : iColumns) {
				if (col++ > 0)
					out.printf("|");
				out.printf("%-20s", colName.toUpperCase());
			}
			out.printf("\n");
			printHeaderLine(iColumns);
		}

		// FORMAT THE LINE DYNAMICALLY
		StringBuilder format = new StringBuilder("%3d|%8s");
		List<Object> vargs = new ArrayList<Object>();
		vargs.add(iIndex);
		vargs.add(iRecord.getIdentity());

		Object value;
		for (String colName : iColumns) {
			format.append("|%-20s");
			value = iRecord.field(colName);
			if (value instanceof Collection<?>)
				value = "[" + ((Collection<?>) value).size() + "]";
			vargs.add(value);
		}

		out.println(String.format(format.toString(), vargs.toArray()));
	}

	private void printHeaderLine(final List<String> iColumns) {
		out.print("---+--------");
		for (int i = 0; i < iColumns.size(); ++i) {
			out.print("+");
			for (int k = 0; k < 20; ++k)
				out.print("-");
		}
		out.print("\n");
	}

	private void dumpRecordDetails() {
		out.println("--------------------------------------------------");
		out.printf("Class: %s   id: %s   v.%d\n", currentRecord.getClassName(), currentRecord.getIdentity().toString(), currentRecord
				.getVersion());
		out.println("--------------------------------------------------");
		for (String fieldName : currentRecord.fields()) {
			out.printf("%20s : %-20s\n", fieldName, currentRecord.field(fieldName));
		}
		out.println("--------------------------------------------------");
	}

	public void onMessage(String iText) {
		out.print(iText);
	}
}
