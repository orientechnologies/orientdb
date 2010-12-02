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
package com.orientechnologies.orient.core.db.tool;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Iterator;
import java.util.Map.Entry;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.dictionary.ODictionary;
import com.orientechnologies.orient.core.index.OPropertyIndex;
import com.orientechnologies.orient.core.intent.OIntentMassiveRead;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OJSONWriter;
import com.orientechnologies.orient.core.storage.impl.local.OClusterLogical;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocal;

/**
 * Export data from a database to a file.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class ODatabaseExport extends ODatabaseImpExpAbstract {
	private OJSONWriter	writer;
	private long				recordExported;

	public ODatabaseExport(final ODatabaseRecord<?> iDatabase, final String iFileName, final OCommandOutputListener iListener)
			throws IOException {
		super(iDatabase, iFileName, iListener);

		final File f = new File(fileName);
		if (f.exists())
			f.delete();
		else
			f.mkdirs();

		writer = new OJSONWriter(new FileWriter(fileName));
		writer.beginObject();

		iDatabase.declareIntent(new OIntentMassiveRead());
	}

	public ODatabaseExport(final ODatabaseRecord<?> iDatabase, final OutputStream iOutputStream,
			final OCommandOutputListener iListener) throws IOException {
		super(iDatabase, "streaming", iListener);

		writer = new OJSONWriter(new OutputStreamWriter(iOutputStream));
		writer.beginObject();

		iDatabase.declareIntent(new OIntentMassiveRead());
	}

	public ODatabaseExport exportDatabase() {
		try {
			listener.onMessage("\nStarted export of database '" + database.getName() + "' to " + fileName + "...");

			long time = System.currentTimeMillis();

			if (includeInfo)
				exportInfo();
			exportClusters();
			if (includeSchema)
				exportSchema();
			exportRecords();
			if (includeDictionary)
				exportDictionary();

			listener.onMessage("\n\nDatabase export completed in " + (System.currentTimeMillis() - time) + "ms");

			writer.flush();
		} catch (Exception e) {
			e.printStackTrace();
			throw new ODatabaseExportException("Error on exporting database '" + database.getName() + "' to: " + fileName, e);
		} finally {
			close();
		}

		return this;
	}

	public long exportRecords() throws IOException {
		long totalRecords = 0;
		int level = 1;
		listener.onMessage("\nExporting records...");

		writer.beginCollection(level, true, "records");
		for (String clusterName : database.getClusterNames()) {
			// CHECK IF THE CLUSTER IS INCLUDED
			if (includeClusters != null) {
				if (!includeClusters.contains(clusterName))
					continue;
			} else if (excludeClusters != null) {
				if (excludeClusters.contains(clusterName))
					continue;
			}

			if (excludeClusters.contains(clusterName))
				continue;

			long recordTot = database.countClusterElements(clusterName);
			listener.onMessage("\n- Exporting record of cluster '" + clusterName + "'...");

			long recordNum = 0;
			for (ORecordInternal<?> rec : database.browseCluster(clusterName)) {

				if (rec instanceof ODocument) {
					// CHECK IF THE CLASS OF THE DOCUMENT IS INCLUDED
					ODocument doc = (ODocument) rec;
					if (includeClasses != null) {
						if (!includeClasses.contains(doc.getClassName()))
							continue;
					} else if (excludeClasses != null) {
						if (excludeClasses.contains(doc.getClassName()))
							continue;
					}
				}

				exportRecord(recordTot, recordNum++, rec);
			}

			listener.onMessage("OK (records=" + recordTot + ")");

			writer.flush();

			totalRecords += recordTot;
		}
		writer.endCollection(level, true);

		listener.onMessage("\n\nDone. Exported " + totalRecords + " records\n");

		return totalRecords;
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

	private void exportClusters() throws IOException {
		listener.onMessage("\nExporting clusters...");

		writer.beginCollection(1, true, "clusters");
		int exportedClusters = 0;
		int totalCluster = database.getClusterNames().size();
		String clusterName;

		for (int clusterId = 0; clusterId < totalCluster; ++clusterId) {
			clusterName = database.getClusterNameById(clusterId);

			// CHECK IF THE CLUSTER IS INCLUDED
			if (includeClusters != null) {
				if (!includeClusters.contains(clusterName))
					continue;
			} else if (excludeClusters != null) {
				if (excludeClusters.contains(clusterName))
					continue;
			}

			writer.beginObject(2, true, null);
			writer.writeAttribute(0, false, "name", clusterName);
			writer.writeAttribute(0, false, "id", clusterId);
			writer.writeAttribute(0, false, "type", database.getClusterType(clusterName));

			if (database.getStorage() instanceof OStorageLocal)
				if (database.getClusterType(clusterName).equals("LOGICAL")) {
					OClusterLogical cluster = (OClusterLogical) database.getStorage().getClusterById(clusterId);
					writer.writeAttribute(0, false, "rid", cluster.getRID());
				}

			exportedClusters++;
			writer.endObject(2, false);
		}

		listener.onMessage("OK (" + exportedClusters + " clusters)");

		writer.endCollection(1, true);
	}

	private void exportInfo() throws IOException {
		listener.onMessage("\nExporting database info...");

		writer.beginObject(1, true, "info");
		writer.writeAttribute(2, true, "name", database.getName().replace('\\', '/'));
		writer.writeAttribute(2, true, "default-cluster-id", database.getDefaultClusterId());
		writer.endObject(1, true);

		listener.onMessage("OK");
	}

	@SuppressWarnings("unchecked")
	private void exportDictionary() throws IOException {
		listener.onMessage("\nExporting dictionary...");

		long tot = 0;
		writer.beginObject(1, true, "dictionary");
		ODictionary<ODocument> d = (ODictionary<ODocument>) database.getDictionary();
		if (d != null) {
			Entry<String, ODocument> entry;
			for (Iterator<Entry<String, ODocument>> iterator = d.iterator(); iterator.hasNext();) {
				entry = iterator.next();
				writer.writeAttribute(2, true, "key", entry.getKey());
				writer.writeAttribute(0, false, "value", OJSONWriter.writeValue(entry.getValue()));
				++tot;
			}
		}
		writer.endObject(1, true);

		listener.onMessage("OK (" + tot + " entries)");
	}

	private void exportSchema() throws IOException {
		listener.onMessage("\nExporting schema...");

		writer.beginObject(1, true, "schema");
		OSchema s = database.getMetadata().getSchema();
		writer.writeAttribute(2, true, "version", s.getDocument().getVersion());

		if (s.getClasses().size() > 0) {
			writer.beginCollection(2, true, "classes");
			for (OClass cls : s.getClasses()) {
				writer.beginObject(3, true, null);
				writer.writeAttribute(0, false, "name", cls.getName());
				writer.writeAttribute(0, false, "id", cls.getId());
				writer.writeAttribute(0, false, "default-cluster-id", cls.getDefaultClusterId());
				writer.writeAttribute(0, false, "cluster-ids", cls.getClusterIds());
				if (cls.getSuperClass() != null)
					writer.writeAttribute(0, false, "super-class", cls.getSuperClass().getName());

				if (cls.properties().size() > 0) {
					writer.beginCollection(4, true, "properties");
					for (OProperty p : cls.declaredProperties()) {
						writer.beginObject(5, true, null);
						writer.writeAttribute(0, false, "name", p.getName());
						writer.writeAttribute(0, false, "id", p.getId());
						writer.writeAttribute(0, false, "type", p.getType().toString());
						if (p.getLinkedClass() != null)
							writer.writeAttribute(0, false, "linked-class", p.getLinkedClass().getName());
						if (p.getLinkedType() != null)
							writer.writeAttribute(0, false, "linked-type", p.getLinkedType().toString());
						if (p.getMin() != null)
							writer.writeAttribute(0, false, "min", p.getMin());
						if (p.getMax() != null)
							writer.writeAttribute(0, false, "max", p.getMax());
						if (p.getIndex() != null) {
							writer.writeAttribute(0, false, "index-rid", p.getIndex().getIdentity());

							OPropertyIndex idx = p.getIndex();
							writer.writeAttribute(0, false, "index-type", idx.getType());
						}
						writer.endObject(0, false);
					}
					writer.endCollection(4, true);
				}

				writer.endObject(3, true);
			}
			writer.endCollection(2, true);
		}

		writer.endObject(1, true);

		listener.onMessage("OK (" + s.getClasses().size() + " classes)");
	}

	private void exportRecord(long recordTot, long recordNum, ORecordInternal<?> rec) throws IOException {
		if (rec == null)
			return;

		try {
			if (rec.getIdentity().isValid())
				rec.load();

			if (recordExported > 0)
				writer.append(",");

			writer.append(rec.toJSON("rid,type,version,class,attribSameRow,indent:4"));
		} catch (Throwable t) {
			byte[] buffer = rec.toStream();

			OLogManager.instance().error(this,
					"Error on exporting record #%s. It seems corrupted; size: %d bytes, raw content (as string): %s", t, rec.getIdentity(),
					buffer.length, new String(buffer));
		}

		recordExported++;
		recordNum++;

		if (recordTot > 10 && (recordNum + 1) % (recordTot / 10) == 0)
			listener.onMessage(".");
	}
}
