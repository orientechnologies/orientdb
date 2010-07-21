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

import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.dictionary.ODictionary;
import com.orientechnologies.orient.core.intent.OIntentMassiveRead;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;
import com.orientechnologies.orient.core.record.impl.ORecordColumn;
import com.orientechnologies.orient.core.record.impl.ORecordFlat;
import com.orientechnologies.orient.core.serialization.serializer.OJSONWriter;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerJSON;
import com.orientechnologies.utility.console.OCommandListener;

public class OConsoleDatabaseExport {
	private ODatabaseDocument		database;
	private String							fileName;
	private OJSONWriter					writer;
	private OCommandListener		listener;

	private static final String	ATTRIBUTE_TYPE	= "_type";

	public OConsoleDatabaseExport(final ODatabaseDocument database, final String iFileName, final OCommandListener iListener)
			throws IOException {
		this.database = database;
		this.fileName = iFileName;
		listener = iListener;

		writer = new OJSONWriter(new FileWriter(fileName));
		writer.beginObject();

		database.declareIntent(new OIntentMassiveRead());
	}

	public OConsoleDatabaseExport exportDatabase() {
		try {
			exportInfo();
			exportDictionary();
			exportSchema();
			exportAllClusters();

			listener.onMessage("\nExport of database completed.");

			writer.flush();
		} catch (Exception e) {
			throw new ODatabaseExportException("Error on exporting database '" + database.getName() + " to: " + fileName, e);
		} finally {
			close();
		}

		return this;
	}

	public OConsoleDatabaseExport exportClusters(final String[] iClusters, final int iLevel) throws IOException {
		for (String clusterName : iClusters) {
			long recordTot = database.countClusterElements(clusterName);
			listener.onMessage("\n- Exporting cluster '" + clusterName + "' (records=" + recordTot + ") -> ");

			writer.beginObject(iLevel, true, "cluster");
			writer.writeAttribute(iLevel + 1, true, "name", clusterName);
			writer.writeAttribute(0, false, "id", database.getClusterIdByName(clusterName));

			if (recordTot > 0) {
				writer.beginCollection(iLevel + 1, true, "records");

				long recordNum = 0;
				for (ORecord<?> rec : database.browseCluster(clusterName))
					exportRecord(recordTot, recordNum, rec);

				writer.endCollection(iLevel + 1, true);
			}

			listener.onMessage("OK");

			writer.endObject(iLevel, true);
		}
		return this;
	}

	public void close() {
		database.declareIntent(null);

		if (writer == null)
			return;

		try {
			writer.endObject();
			writer.close();
			writer = null;
		} catch (IOException e) {
		}
	}

	private void exportAllClusters() throws IOException {
		listener.onMessage("\nExporting clusters...");

		writer.beginObject(1, true, "clusters");
		exportClusters(database.getClusterNames().toArray(new String[database.getClusterNames().size()]), 2);
		writer.endObject(1, true);

	}

	private void exportInfo() throws IOException {
		listener.onMessage("\nExporting database info...");

		writer.beginObject(1, true, "info");
		writer.writeAttribute(2, true, "name", database.getName());
		writer.writeAttribute(2, true, "default-cluster-id", database.getDefaultClusterId());
		writer.endObject(1, true);

		listener.onMessage("OK");
	}

	private void exportDictionary() throws IOException {
		listener.onMessage("\nExporting dictionary...");

		writer.beginObject(1, true, "dictionary");
		ODictionary<ODocument> d = database.getDictionary();
		if (d != null) {
			Entry<String, ODocument> entry;
			for (Iterator<Entry<String, ODocument>> iterator = d.iterator(); iterator.hasNext();) {
				entry = iterator.next();
				writer.writeAttribute(2, true, "key", entry.getKey());
				writer.writeAttribute(0, false, "value", OJSONWriter.writeValue(entry.getValue()));
			}
		}
		writer.endObject(1, true);

		listener.onMessage("OK");
	}

	private void exportSchema() throws IOException {
		listener.onMessage("\nExporting schema...");

		writer.beginObject(1, true, "schema");
		OSchema s = database.getMetadata().getSchema();
		writer.writeAttribute(2, true, "version", s.getDocument().getVersion());

		if (s.getClasses().size() > 0) {
			writer.beginCollection(2, true, "classes");
			for (OClass cls : s.getClasses()) {
				writer.beginObject(3, true, "class");
				writer.writeAttribute(0, false, "name", cls.getName());
				writer.writeAttribute(0, false, "id", cls.getId());
				writer.writeAttribute(0, false, "default-cluster-id", cls.getDefaultClusterId());
				writer.writeAttribute(0, false, "cluster-ids", cls.getClusterIds());

				if (cls.properties().size() > 0) {
					writer.beginCollection(4, true, "properties");
					for (OProperty p : cls.properties()) {
						writer.beginObject(5, true, null);
						writer.writeAttribute(0, false, "name", p.getName());
						writer.writeAttribute(0, false, "id", p.getId());
						writer.writeAttribute(0, false, "type", p.getType());
						if (p.getLinkedClass() != null)
							writer.writeAttribute(0, false, "linked-class", p.getLinkedClass().getName());
						if (p.getLinkedType() != null)
							writer.writeAttribute(0, false, "linked-type", p.getLinkedType());
						if (p.getMin() != null)
							writer.writeAttribute(0, false, "min", p.getMin());
						if (p.getMax() != null)
							writer.writeAttribute(0, false, "max", p.getMax());
						writer.endObject(0, false);
					}
					writer.endCollection(4, true);
				}

				writer.endObject(3, true);
			}
			writer.endCollection(2, true);
		}

		writer.endObject(1, true);

		listener.onMessage("OK");
	}

	private void exportRecord(long recordTot, long recordNum, ORecord<?> rec) throws IOException {
		if (rec == null)
			return;

		writer.beginObject(4, true, null);
		writer.writeAttribute(0, false, ORecordSerializerJSON.ATTRIBUTE_ID, rec.getIdentity());
		writer.writeAttribute(0, false, ORecordSerializerJSON.ATTRIBUTE_VERSION, rec.getVersion());

		if (rec.getIdentity().isValid())
			rec.load();

		if (rec instanceof ODocument) {
			ODocument vobj = (ODocument) rec;

			writer.writeAttribute(0, false, ATTRIBUTE_TYPE, "v");

			Object value;
			if (vobj.fieldNames() != null && vobj.fieldNames().length > 0) {
				writer.beginObject(5, true, "fields");
				for (String f : vobj.fieldNames()) {
					value = vobj.field(f);

					writer.writeAttribute(6, true, f, value);
				}
				writer.endObject(5, true);
			}
		} else if (rec instanceof ORecordColumn) {
			ORecordColumn csv = (ORecordColumn) rec;

			writer.writeAttribute(0, false, ATTRIBUTE_TYPE, "c");

			if (csv.size() > 0) {
				writer.beginCollection(5, true, "values");
				for (int i = 0; i < csv.size(); ++i) {
					writer.writeValue(6, true, csv.field(i));
				}
				writer.endCollection(5, true);
			}
		} else if (rec instanceof ORecordFlat) {
			ORecordFlat flat = (ORecordFlat) rec;

			writer.writeAttribute(0, false, ATTRIBUTE_TYPE, "f");
			writer.writeAttribute(6, true, "value", flat.value());
		} else if (rec instanceof ORecordBytes) {

			ORecordBytes bytes = (ORecordBytes) rec;

			writer.writeAttribute(0, false, ATTRIBUTE_TYPE, "b");
			writer.writeAttribute(6, true, "value", bytes.toStream());
		}

		writer.endObject(4, true);

		recordNum++;

		if (recordTot > 10 && (recordNum + 1) % (recordTot / 10) == 0)
			listener.onMessage(".");
	}
}
