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

import java.io.IOException;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ORecordFlat;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.OStorage;

public class ODatabaseCompare extends ODatabaseImpExpAbstract {
	private OStorage	storage1;
	private OStorage	storage2;
	private int				differences	= 0;

	public ODatabaseCompare(String iDb1URL, String iDb2URL, final OCommandOutputListener iListener) throws IOException {
		super(null, null, iListener);

		listener.onMessage("\nComparing two local databases:\n1) " + iDb1URL + "\n2) " + iDb2URL + "\n");

		storage1 = Orient.instance().loadStorage(iDb1URL);
		storage1.open(null, null, null);

		storage2 = Orient.instance().loadStorage(iDb2URL);
		storage2.open(null, null, null);
	}

	public boolean compare() {
		try {
			compareClusters();
			compareRecords();

			if (differences == 0) {
				listener.onMessage("\n\nDatabases match.");
				return true;
			} else {
				listener.onMessage("\n\nDatabases do not match. Found " + differences + " difference(s).");
				return false;
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new ODatabaseExportException("Error on compare of database '" + storage1.getName() + "' against '" + storage2.getName()
					+ "'", e);
		} finally {
			storage1.close();
			storage2.close();
		}
	}

	private boolean compareClusters() {
		listener.onMessage("\nStarting shallow comparison of clusters:");

		listener.onMessage("\nChecking the number of clusters...");

		if (storage1.getClusterNames().size() != storage1.getClusterNames().size()) {
			listener.onMessage("ERR: cluster sizes are different: " + storage1.getClusterNames().size() + " <-> "
					+ storage1.getClusterNames().size());
			++differences;
		}

		int cluster2Id;
		boolean ok;

		for (String clusterName : storage1.getClusterNames()) {
			// CHECK IF THE CLUSTER IS INCLUDED
			if (includeClusters != null) {
				if (!includeClusters.contains(clusterName))
					continue;
			} else if (excludeClusters != null) {
				if (excludeClusters.contains(clusterName))
					continue;
			}

			ok = true;
			cluster2Id = storage2.getClusterIdByName(clusterName);

			listener.onMessage("\n- Checking cluster " + String.format("%-25s: ", "'" + clusterName + "'"));

			if (cluster2Id == -1) {
				listener.onMessage("ERR: cluster name " + clusterName + " was not found on database " + storage2);
				++differences;
				ok = false;
			}

			if (cluster2Id != storage1.getClusterIdByName(clusterName)) {
				listener.onMessage("ERR: cluster id is different for cluster " + clusterName + ": "
						+ storage1.getClusterIdByName(clusterName) + " <-> " + cluster2Id);
				++differences;
				ok = false;
			}

			if (storage1.count(cluster2Id) != storage2.count(cluster2Id)) {
				listener.onMessage("ERR: number of records different in cluster '" + clusterName + "' (id=" + cluster2Id + "): "
						+ storage1.count(cluster2Id) + " <-> " + storage2.count(cluster2Id));
				++differences;
				ok = false;
			}

			if (ok)
				listener.onMessage("OK");
		}

		listener.onMessage("\n\nShallow analysis done.");
		return true;
	}

	private boolean compareRecords() {
		listener.onMessage("\nStarting deep comparison record by record. This may take a few minutes. Wait please...");

		int clusterId;

		ORawBuffer buffer1, buffer2;

		for (String clusterName : storage1.getClusterNames()) {
			// CHECK IF THE CLUSTER IS INCLUDED
			if (includeClusters != null) {
				if (!includeClusters.contains(clusterName))
					continue;
			} else if (excludeClusters != null) {
				if (excludeClusters.contains(clusterName))
					continue;
			}

			clusterId = storage1.getClusterIdByName(clusterName);

			final long db1Max = storage1.getClusterDataRange(clusterId)[1];
			final long db2Max = storage2.getClusterDataRange(clusterId)[1];

			final ODocument doc1 = new ODocument();
			final ODocument doc2 = new ODocument();

			final ORecordId rid = new ORecordId(clusterId);

			final long clusterMax = Math.max(db1Max, db2Max);
			for (int i = 0; i <= clusterMax; ++i) {
				rid.clusterPosition = i;
				buffer1 = i <= db1Max ? storage1.readRecord(rid, null, null) : null;
				buffer2 = i <= db2Max ? storage2.readRecord(rid, null, null) : null;

				if (buffer1 == null && buffer2 == null)
					// BOTH RECORD NULL, OK
					continue;
				else if (buffer1 == null && buffer2 != null) {
					// REC1 NULL
					listener.onMessage("\n- ERR: RID=" + clusterId + ":" + i + " is null in DB1");
					++differences;
				} else if (buffer1 != null && buffer2 == null) {
					// REC2 NULL
					listener.onMessage("\n- ERR: RID=" + clusterId + ":" + i + " is null in DB2");
					++differences;
				} else {
					if (buffer1.recordType != buffer2.recordType) {
						listener.onMessage("\n- ERR: RID=" + clusterId + ":" + i + " recordType is different: " + (char) buffer1.recordType
								+ " <-> " + (char) buffer2.recordType);
						++differences;
					}

					if (buffer1.buffer == null && buffer2.buffer == null) {
					} else if (buffer1.buffer == null && buffer2.buffer != null) {
						listener.onMessage("\n- ERR: RID=" + clusterId + ":" + i + " content is different: null <-> " + buffer2.buffer.length);
						++differences;

					} else if (buffer1.buffer != null && buffer2.buffer == null) {
						listener.onMessage("\n- ERR: RID=" + clusterId + ":" + i + " content is different: " + buffer1.buffer.length
								+ " <-> null");
						++differences;

					} else if (buffer1.buffer.length != buffer2.buffer.length) {
						// CHECK IF THE TRIMMED SIZE IS THE SAME
						final String rec1 = new String(buffer1.buffer).trim();
						final String rec2 = new String(buffer2.buffer).trim();

						if (rec1.length() != rec2.length()) {
							listener.onMessage("\n- ERR: RID=" + clusterId + ":" + i + " content length is different: " + buffer1.buffer.length
									+ " <-> " + buffer2.buffer.length);

							if (buffer1.recordType == ODocument.RECORD_TYPE || buffer1.recordType == ORecordFlat.RECORD_TYPE)
								listener.onMessage("\n--- REC1: " + rec1);
							if (buffer2.recordType == ODocument.RECORD_TYPE || buffer2.recordType == ORecordFlat.RECORD_TYPE)
								listener.onMessage("\n--- REC2: " + rec2);
							listener.onMessage("\n");

							++differences;
						}

					} else {
						if (buffer1.recordType == ODocument.RECORD_TYPE) {
							// DOCUMENT: TRY TO INSTANTIATE AND COMPARE
							doc1.reset();
							doc1.fromStream(buffer1.buffer);
							doc2.reset();
							doc2.fromStream(buffer2.buffer);

							if (!doc1.hasSameContentOf(doc2)) {
								listener.onMessage("\n- ERR: RID=" + clusterId + ":" + i + " document content is different");
								listener.onMessage("\n--- REC1: " + new String(buffer1.buffer));
								listener.onMessage("\n--- REC2: " + new String(buffer2.buffer));
								listener.onMessage("\n");
								++differences;
							}
						} else {
							// CHECK BYTE PER BYTE
							for (int b = 0; b < buffer1.buffer.length; ++b) {
								if (buffer1.buffer[b] != buffer2.buffer[b]) {
									listener.onMessage("\n- ERR: RID=" + clusterId + ":" + i + " content is different at byte #" + b + ": "
											+ buffer1.buffer[b] + " <-> " + buffer2.buffer[b]);
									listener.onMessage("\n--- REC1: " + new String(buffer1.buffer));
									listener.onMessage("\n--- REC2: " + new String(buffer2.buffer));
									listener.onMessage("\n");
									++differences;
									break;
								}
							}
						}
					}
				}
			}
		}

		return true;
	}
}
