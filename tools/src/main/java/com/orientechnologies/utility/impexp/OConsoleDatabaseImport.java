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
package com.orientechnologies.utility.impexp;

import java.io.File;
import java.io.IOException;

import com.orientechnologies.common.parser.OStringForwardReader;
import com.orientechnologies.common.parser.OStringParser;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.intent.OIntentDefault;
import com.orientechnologies.orient.core.intent.OIntentMassiveRead;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.utility.console.OCommandListener;

/**
 * Import data into a database.
 * 
 * @author Luca Garulli
 * 
 */
public class OConsoleDatabaseImport {
	private static final String		MODE_ARRAY	= "array";
	private ODatabaseDocument			database;
	private String								fileName;
	private OCommandListener			listener;

	private OStringForwardReader	reader;

	public OConsoleDatabaseImport(final ODatabaseDocument database, final String iFileName, final OCommandListener iListener)
			throws IOException {
		this.database = database;
		this.fileName = iFileName;
		listener = iListener;

		reader = new OStringForwardReader(new File(fileName));

		database.declareIntent(new OIntentMassiveRead());
	}

	public OConsoleDatabaseImport importDatabase() {
		try {
			listener.onMessage("\nImport of database completed.");

		} catch (Exception e) {
			throw new ODatabaseExportException("Error on importing database '" + database.getName() + " to: " + fileName, e);
		} finally {
			close();
		}

		return this;
	}

	public OConsoleDatabaseImport importRecords(final String iMode, final String iClusterType) throws IOException {
		if (iMode == null)
			throw new IllegalArgumentException("Importing mode not specified received");

		int offset = OStringParser.jump(reader, (int) reader.getPosition(), OStringParser.COMMON_JUMP);
		if (offset == -1 || reader.charAt(offset) != '{')
			throw new IllegalStateException("Missed begin of json (expected char '{')");

		long importedRecordsTotal = 0;
		long notImportedRecordsTotal = 0;

		long beginChronoTotal = System.currentTimeMillis();

		final ODocument doc = new ODocument(database);

		try {
			while (reader.ready()) {
				String className = parse(":");
				if (className.length() > 2)
					className = className.substring(1, className.length() - 1);

				OClass cls = database.getMetadata().getSchema().getClass(className);

				if (cls == null) {
					// CREATE THE CLASS IF NOT EXISTS YET
					if (iClusterType.equalsIgnoreCase("logical"))
						cls = database.getMetadata().getSchema().createClass(className);
					else
						cls = database.getMetadata().getSchema().createClass(className, database.addPhysicalCluster(className, className, -1));

					database.getMetadata().getSchema().save();
				}

				doc.setClassName(className);

				listener.onMessage("\n- Importing document(s) of class '" + cls.getName() + "'...");

				offset = OStringParser.jump(reader, (int) reader.getPosition() + 1, OStringParser.COMMON_JUMP);

				if (iMode.equalsIgnoreCase(MODE_ARRAY)) {
					if (offset == -1 || reader.charAt(offset) != '[')
						throw new IllegalStateException("Missed begin of array (expected char '[')");
				} else
					throw new IllegalArgumentException("mode '" + iMode + "' not supported");

				long beginChronoClass = System.currentTimeMillis();

				long importedRecordsClass = 0;
				long notImportedRecordsClass = 0;

				try {
					char c;

					while (reader.ready()) {

						String chunk = OStringParser.getWord(reader, (int) reader.getPosition() + 1, "}");
						if (chunk == null)
							throw new IllegalStateException("Missed end of record (expected char '}')");

						chunk += "}";

						try {
							doc.reset();
							doc.fromJSON(chunk);
							doc.save();
						} catch (Exception e) {
							listener.onMessage("\nError on importing document: " + chunk);
							listener.onMessage("\n  The cause is " + e + "\n");
							notImportedRecordsClass++;
						}

						importedRecordsClass++;

						offset = OStringParser.jump(reader, (int) reader.getPosition() + 1, OStringParser.COMMON_JUMP);
						c = reader.charAt(offset);
						if (offset == -1 || (c != ']' && c != ','))
							throw new IllegalStateException("Missed separator or end of array (expected chars ',' or ']')");

						if (c == ']')
							// END OF CLUSTER
							break;
					}
				} finally {
					listener.onMessage("Done. Imported " + importedRecordsClass + " record(s) in "
							+ (System.currentTimeMillis() - beginChronoClass) + "ms. " + notImportedRecordsClass + " error(s)");
					importedRecordsTotal += importedRecordsClass;
					notImportedRecordsTotal += notImportedRecordsClass;
				}

				offset = OStringParser.jump(reader, (int) reader.getPosition() + 1, OStringParser.COMMON_JUMP);

				if (offset == -1 || reader.charAt(offset) == '}')
					break;
			}
		} finally {
			listener.onMessage("\n\nImported " + importedRecordsTotal + " document(s) in "
					+ (System.currentTimeMillis() - beginChronoTotal) + "ms. " + notImportedRecordsTotal + " error(s)\n");
		}

		return this;
	}

	private String parse(final String iSeparatorChars) throws IOException {
		int offset = OStringParser.jump(reader, (int) reader.getPosition(), OStringParser.COMMON_JUMP);

		if (offset == -1)
			throw new IllegalStateException("End of input caught");

		return OStringParser.getWord(reader, (int) reader.getPosition() + 1, iSeparatorChars);
	}

	public void close() {
		database.declareIntent(new OIntentDefault());

		if (reader == null)
			return;

		try {
			reader.close();
			reader = null;
		} catch (IOException e) {
		}
	}
}
