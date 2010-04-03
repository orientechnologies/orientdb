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
import java.util.Collection;
import java.util.Iterator;
import java.util.Map.Entry;

import com.orientechnologies.orient.core.db.vobject.ODatabaseVObject;
import com.orientechnologies.orient.core.dictionary.ODictionary;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ORecordVObject;

public class OConsoleDatabaseExport {
	private ODatabaseVObject	database;
	private String						fileName;
	private OJSONWriter				writer;

	public OConsoleDatabaseExport(final ODatabaseVObject database, final String iFileName) {
		this.database = database;
		this.fileName = iFileName;
	}

	public void exportDatabase() {
		writer = null;
		try {
			writer = new OJSONWriter(new FileWriter(fileName));

			writer.beginSection(0, true, "");
			exportInfo();
			exportDictionary();
			exportSchema();
			exportClusters();
			writer.endSection(0, true);

			writer.flush();
		} catch (Exception e) {
			throw new ODatabaseExportException("Error on exporting database '" + database.getName() + " to: " + fileName, e);
		} finally {
			if (writer != null)
				try {
					writer.close();
				} catch (IOException e) {
				}
		}
	}

	private void exportInfo() throws IOException {
		writer.beginSection(1, true, "info");
		writer.writeAttribute(2, true, "name", database.getName());
		writer.writeAttribute(2, true, "default-cluster-id", database.getDefaultClusterId());
		writer.endSection(1, true);
	}

	private void exportDictionary() throws IOException {
		writer.beginSection(1, true, "dictionary");
		ODictionary<ORecordVObject> d = database.getDictionary();
		if (d != null) {
			Entry<String, ORecordVObject> entry;
			for (Iterator<Entry<String, ORecordVObject>> iterator = d.iterator(); iterator.hasNext();) {
				entry = iterator.next();
				writer.writeAttribute(2, true, "key", entry.getKey());
				writer.writeAttribute(0, false, "value", entry.getValue());
			}
		}
		writer.endSection(1, true);
	}

	private void exportSchema() throws IOException {
		writer.beginSection(1, true, "schema");
		OSchema s = database.getMetadata().getSchema();
		writer.writeAttribute(2, true, "version", s.getVersion());
		writer.endSection(1, true);
	}

	@SuppressWarnings("unchecked")
	private void exportClusters() throws IOException {
		writer.beginSection(1, true, "clusters");
		Collection<String> clusterNames = database.getClusterNames();
		for (String clusterName : clusterNames) {
			writer.beginSection(2, true, "cluster");
			writer.writeAttribute(3, true, "name", clusterName);
			writer.writeAttribute(3, true, "id", database.getClusterIdByName(clusterName));

			writer.beginSection(3, true, "content");
			for (ORecord<?> rec : database.browseCluster(clusterName)) {
				writer.beginSection(4, true, rec.getIdentity());
				writer.writeAttribute(5, true, "version", rec.getVersion());

				if (rec instanceof ORecordVObject) {
					ORecordVObject vobj = (ORecordVObject) rec;
					if (vobj.getIdentity().isValid())
						vobj.load();

					writer.writeAttribute(0, false, "type", "v");

					Object value;
					if (vobj.fields() != null && vobj.fields().length > 0) {
						writer.beginSection(5, true, "fields");
						for (String f : vobj.fields()) {
							value = vobj.field(f);

							if (value != null)
								if (value instanceof ORecordVObject) {
									value = getOutputValue(value);
								} else if (value instanceof Collection<?>) {
									Collection<Object> coll = (Collection<Object>) value;

									StringBuilder buffer = new StringBuilder();
									buffer.append("[");
									for (Object v : coll) {
										if (buffer.length() > 1)
											buffer.append(", ");

										v = getOutputValue(v);

										buffer.append(v);
									}
									buffer.append("]");
									
									value = buffer.toString();
								}

							writer.writeAttribute(6, true, f, value);
						}
						writer.endSection(5, true);
					}
				}

				writer.endSection(4, true);
			}
			writer.endSection(3, true);

			writer.endSection(2, true);
		}
		writer.endSection(1, true);
	}

	private Object getOutputValue(Object iValue) {
		ORecordVObject linked = (ORecordVObject) iValue;
		if (linked.getIdentity().isValid())
			iValue = linked.getIdentity().toString();
		else
			iValue = linked.toString();
		return iValue;
	}
}
