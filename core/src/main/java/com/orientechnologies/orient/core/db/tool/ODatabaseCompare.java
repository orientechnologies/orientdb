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
package com.orientechnologies.orient.core.db.tool;

import static com.orientechnologies.orient.core.record.impl.ODocumentHelper.makeDbCall;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexManager;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentHelper;
import com.orientechnologies.orient.core.record.impl.ODocumentHelper.ODbRelatedCall;
import com.orientechnologies.orient.core.record.impl.ORecordFlat;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.OStorage;

public class ODatabaseCompare extends ODatabaseImpExpAbstract {
  private OStorage            storage1;
  private OStorage            storage2;

  private ODatabaseDocumentTx databaseDocumentTxOne;
  private ODatabaseDocumentTx databaseDocumentTxTwo;

  private boolean             compareEntriesForAutomaticIndexes = false;

  private int                 differences                       = 0;

  public ODatabaseCompare(String iDb1URL, String iDb2URL, final OCommandOutputListener iListener) throws IOException {
    super(null, null, iListener);

    listener.onMessage("\nComparing two local databases:\n1) " + iDb1URL + "\n2) " + iDb2URL + "\n");

    storage1 = Orient.instance().loadStorage(iDb1URL);
    storage1.open(null, null, null);

    storage2 = Orient.instance().loadStorage(iDb2URL);
    storage2.open(null, null, null);
  }

  public ODatabaseCompare(String iDb1URL, String iDb2URL, final String userName, final String userPassword,
      final OCommandOutputListener iListener) throws IOException {
    super(null, null, iListener);

    listener.onMessage("\nComparing two local databases:\n1) " + iDb1URL + "\n2) " + iDb2URL + "\n");

    databaseDocumentTxOne = new ODatabaseDocumentTx(iDb1URL);
    databaseDocumentTxOne.open(userName, userPassword);

    databaseDocumentTxTwo = new ODatabaseDocumentTx(iDb2URL);
    databaseDocumentTxTwo.open(userName, userPassword);

    storage1 = databaseDocumentTxOne.getStorage();

    storage2 = databaseDocumentTxTwo.getStorage();

    // exclude automatically generated clusters
    excludeClusters.add("orids");
    excludeClusters.add("index");
    excludeClusters.add("manindex");
  }

  public boolean isCompareEntriesForAutomaticIndexes() {
    return compareEntriesForAutomaticIndexes;
  }

  public void setCompareEntriesForAutomaticIndexes(boolean compareEntriesForAutomaticIndexes) {
    this.compareEntriesForAutomaticIndexes = compareEntriesForAutomaticIndexes;
  }

  public boolean compare() {
    if (isDocumentDatabases() && (databaseDocumentTxOne == null || databaseDocumentTxTwo == null)) {
      listener.onMessage("\nPassed in URLs are related to document databases but credentials "
          + "were not provided to open them. Please provide user name + password for databases to compare");
      return false;
    }

    if (!isDocumentDatabases() && (databaseDocumentTxOne != null || databaseDocumentTxTwo != null)) {
      listener.onMessage("\nPassed in URLs are not related to document databases but credentials "
          + "were provided to open them. Please do not provide user name + password for databases to compare");
      return false;
    }

    try {
      compareClusters();
      compareRecords();

      if (isDocumentDatabases())
        compareIndexes();

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

  @SuppressWarnings({ "unchecked", "rawtypes" })
  private void compareIndexes() {
    listener.onMessage("\nStarting index comparison:");

    boolean ok = true;

    final OIndexManager indexManagerOne = makeDbCall(databaseDocumentTxOne, new ODbRelatedCall<OIndexManager>() {
      public OIndexManager call() {
        return databaseDocumentTxOne.getMetadata().getIndexManager();
      }
    });

    final OIndexManager indexManagerTwo = makeDbCall(databaseDocumentTxTwo, new ODbRelatedCall<OIndexManager>() {
      public OIndexManager call() {
        return databaseDocumentTxTwo.getMetadata().getIndexManager();
      }
    });

    final Collection<? extends OIndex<?>> indexesOne = makeDbCall(databaseDocumentTxOne,
        new ODbRelatedCall<Collection<? extends OIndex<?>>>() {
          public Collection<? extends OIndex<?>> call() {
            return indexManagerOne.getIndexes();
          }
        });

    final int indexesSizeOne = makeDbCall(databaseDocumentTxTwo, new ODbRelatedCall<Integer>() {
      public Integer call() {
        return indexesOne.size();
      }
    });

    final int indexesSizeTwo = makeDbCall(databaseDocumentTxTwo, new ODbRelatedCall<Integer>() {
      public Integer call() {
        return indexManagerTwo.getIndexes().size();
      }
    });

    if (indexesSizeOne != indexesSizeTwo) {
      ok = false;
      listener.onMessage("\n- ERR: Amount of indexes are different.");
      listener.onMessage("\n--- DB1: " + indexesSizeOne);
      listener.onMessage("\n--- DB2: " + indexesSizeTwo);
      listener.onMessage("\n");
      ++differences;
    }

    final Iterator<? extends OIndex<?>> iteratorOne = makeDbCall(databaseDocumentTxOne,
        new ODbRelatedCall<Iterator<? extends OIndex<?>>>() {
          public Iterator<? extends OIndex<?>> call() {
            return indexesOne.iterator();
          }
        });

    while (makeDbCall(databaseDocumentTxOne, new ODbRelatedCall<Boolean>() {
      public Boolean call() {
        return iteratorOne.hasNext();
      }
    })) {
      final OIndex indexOne = makeDbCall(databaseDocumentTxOne, new ODbRelatedCall<OIndex<?>>() {
        public OIndex<?> call() {
          return iteratorOne.next();
        }
      });

      final OIndex<?> indexTwo = makeDbCall(databaseDocumentTxTwo, new ODbRelatedCall<OIndex<?>>() {
        public OIndex<?> call() {
          return indexManagerTwo.getIndex(indexOne.getName());
        }
      });

      if (indexTwo == null) {
        ok = false;
        listener.onMessage("\n- ERR: Index " + indexOne.getName() + " is absent in DB2.");
        ++differences;
        continue;
      }

      if (!indexOne.getType().equals(indexTwo.getType())) {
        ok = false;
        listener.onMessage("\n- ERR: Index types for index " + indexOne.getName() + " are different.");
        listener.onMessage("\n--- DB1: " + indexOne.getType());
        listener.onMessage("\n--- DB2: " + indexTwo.getType());
        listener.onMessage("\n");
        ++differences;
        continue;
      }

      if (!indexOne.getClusters().equals(indexTwo.getClusters())) {
        ok = false;
        listener.onMessage("\n- ERR: Clusters to index for index " + indexOne.getName() + " are different.");
        listener.onMessage("\n--- DB1: " + indexOne.getClusters());
        listener.onMessage("\n--- DB2: " + indexTwo.getClusters());
        listener.onMessage("\n");
        ++differences;
        continue;
      }

      if (indexOne.getDefinition() == null && indexTwo.getDefinition() != null) {
        ok = false;
        listener.onMessage("\n- ERR: Index definition for index " + indexOne.getName() + " for DB2 is not null.");
        ++differences;
        continue;
      } else if (indexOne.getDefinition() != null && indexTwo.getDefinition() == null) {
        ok = false;
        listener.onMessage("\n- ERR: Index definition for index " + indexOne.getName() + " for DB2 is null.");
        ++differences;
        continue;
      } else if (indexOne.getDefinition() != null && !indexOne.getDefinition().equals(indexTwo.getDefinition())) {
        ok = false;
        listener.onMessage("\n- ERR: Index definitions for index " + indexOne.getName() + " are different.");
        listener.onMessage("\n--- DB1: " + indexOne.getDefinition());
        listener.onMessage("\n--- DB2: " + indexTwo.getDefinition());
        listener.onMessage("\n");
        ++differences;
        continue;
      }

      final long indexOneSize = makeDbCall(databaseDocumentTxOne, new ODbRelatedCall<Long>() {
        public Long call() {
          return indexOne.getSize();
        }
      });

      final long indexTwoSize = makeDbCall(databaseDocumentTxTwo, new ODbRelatedCall<Long>() {
        public Long call() {
          return indexTwo.getSize();
        }
      });

      if (indexOneSize != indexTwoSize) {
        ok = false;
        listener.onMessage("\n- ERR: Amount of entries for index " + indexOne.getName() + " are different.");
        listener.onMessage("\n--- DB1: " + indexOneSize);
        listener.onMessage("\n--- DB2: " + indexTwoSize);
        listener.onMessage("\n");
        ++differences;
      }

      if (((compareEntriesForAutomaticIndexes && !indexOne.getType().equals("DICTIONARY")) || !indexOne.isAutomatic())) {
        final Iterator<Map.Entry<Object, Object>> indexIteratorOne = makeDbCall(databaseDocumentTxOne,
            new ODbRelatedCall<Iterator<Map.Entry<Object, Object>>>() {
              public Iterator<Map.Entry<Object, Object>> call() {
                return indexOne.iterator();
              }
            });

        while (makeDbCall(databaseDocumentTxOne, new ODbRelatedCall<Boolean>() {
          public Boolean call() {
            return indexIteratorOne.hasNext();
          }
        })) {
          final Map.Entry<Object, Object> indexOneEntry = makeDbCall(databaseDocumentTxOne,
              new ODbRelatedCall<Map.Entry<Object, Object>>() {
                public Map.Entry<Object, Object> call() {
                  return indexIteratorOne.next();
                }
              });

          final Object key = makeDbCall(databaseDocumentTxOne, new ODbRelatedCall<Object>() {
            public Object call() {
              return indexOneEntry.getKey();
            }
          });

          final Object indexOneValue = makeDbCall(databaseDocumentTxOne, new ODbRelatedCall<Object>() {
            public Object call() {
              return indexOneEntry.getValue();
            }
          });

          final Object indexTwoValue = makeDbCall(databaseDocumentTxTwo, new ODbRelatedCall<Object>() {
            public Object call() {
              return indexTwo.get(key);
            }
          });

          if (indexTwoValue == null) {
            ok = false;
            listener.onMessage("\n- ERR: Entry with key " + key + " is absent in index " + indexOne.getName() + " for DB2.");
            ++differences;
            continue;
          }

          if (indexOneValue instanceof Set && indexTwoValue instanceof Set) {
            final Set<Object> indexOneValueSet = (Set<Object>) indexOneValue;
            final Set<Object> indexTwoValueSet = (Set<Object>) indexTwoValue;

            if (!ODocumentHelper.compareSets(databaseDocumentTxOne, indexOneValueSet, databaseDocumentTxTwo, indexTwoValueSet)) {
              ok = false;
              reportIndexDiff(indexOne, key, indexOneValue, indexTwoValue);
            }
          } else if (!indexOneValue.equals(indexTwoValue)) {
            ok = false;
            reportIndexDiff(indexOne, key, indexOneValue, indexTwoValue);
          }
        }
      }
    }

    if (ok)
      listener.onMessage("OK");
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
      final OStorage storage;

      if (clusterMax == db1Max)
        storage = storage1;
      else
        storage = storage2;

      for (int i = 0; i <= clusterMax; ++i) {
        final long[] positions = storage.getClusterPositionsForEntry(clusterId, i);

        for (long position : positions) {
          rid.clusterPosition = position;

          if (isDocumentDatabases() && rid.equals(new ORecordId(storage1.getConfiguration().indexMgrRecordId))
              && rid.equals(new ORecordId(storage2.getConfiguration().indexMgrRecordId)))
            continue;

          final ORawBuffer buffer1 = storage1.readRecord(rid, null, true, null);
          final ORawBuffer buffer2 = storage2.readRecord(rid, null, true, null);

          if (buffer1 == null && buffer2 == null)
            // BOTH RECORD NULL, OK
            continue;
          else if (buffer1 == null && buffer2 != null) {
            // REC1 NULL
            listener.onMessage("\n- ERR: RID=" + clusterId + ":" + position + " is null in DB1");
            ++differences;
          } else if (buffer1 != null && buffer2 == null) {
            // REC2 NULL
            listener.onMessage("\n- ERR: RID=" + clusterId + ":" + position + " is null in DB2");
            ++differences;
          } else {
            if (buffer1.recordType != buffer2.recordType) {
              listener.onMessage("\n- ERR: RID=" + clusterId + ":" + position + " recordType is different: "
                  + (char) buffer1.recordType + " <-> " + (char) buffer2.recordType);
              ++differences;
            }

            if (buffer1.buffer == null && buffer2.buffer == null) {
            } else if (buffer1.buffer == null && buffer2.buffer != null) {
              listener.onMessage("\n- ERR: RID=" + clusterId + ":" + position + " content is different: null <-> "
											+ buffer2.buffer.length);
              ++differences;

            } else if (buffer1.buffer != null && buffer2.buffer == null) {
              listener.onMessage("\n- ERR: RID=" + clusterId + ":" + position + " content is different: " + buffer1.buffer.length
                  + " <-> null");
              ++differences;

            } else {
              if (buffer1.recordType == ODocument.RECORD_TYPE) {
                // DOCUMENT: TRY TO INSTANTIATE AND COMPARE

                makeDbCall(databaseDocumentTxOne, new ODocumentHelper.ODbRelatedCall<Object>() {
                  public Object call() {
                    doc1.reset();
                    doc1.fromStream(buffer1.buffer);
                    return null;
                  }
                });

                makeDbCall(databaseDocumentTxTwo, new ODocumentHelper.ODbRelatedCall<Object>() {
                  public Object call() {
                    doc2.reset();
                    doc2.fromStream(buffer2.buffer);
                    return null;
                  }
                });

                if (rid.toString().equals(storage1.getConfiguration().schemaRecordId)
                    && rid.toString().equals(storage2.getConfiguration().schemaRecordId)) {
                  makeDbCall(databaseDocumentTxOne, new ODocumentHelper.ODbRelatedCall<java.lang.Object>() {
                    public Object call() {
                      convertSchemaDoc(doc1);
                      return null;
                    }
                  });

                  makeDbCall(databaseDocumentTxTwo, new ODocumentHelper.ODbRelatedCall<java.lang.Object>() {
                    public Object call() {
                      convertSchemaDoc(doc2);
                      return null;
                    }
                  });
                }

                if (!ODocumentHelper.hasSameContentOf(doc1, databaseDocumentTxOne, doc2, databaseDocumentTxTwo)) {
                  listener.onMessage("\n- ERR: RID=" + clusterId + ":" + position + " document content is different");
                  listener.onMessage("\n--- REC1: " + new String(buffer1.buffer));
                  listener.onMessage("\n--- REC2: " + new String(buffer2.buffer));
                  listener.onMessage("\n");
                  ++differences;
                }
              } else {
                if (buffer1.buffer.length != buffer2.buffer.length) {
                  // CHECK IF THE TRIMMED SIZE IS THE SAME
                  final String rec1 = new String(buffer1.buffer).trim();
                  final String rec2 = new String(buffer2.buffer).trim();

                  if (rec1.length() != rec2.length()) {
                    listener.onMessage("\n- ERR: RID=" + clusterId + ":" + position + " content length is different: "
                        + buffer1.buffer.length + " <-> " + buffer2.buffer.length);

                    if (buffer1.recordType == ODocument.RECORD_TYPE || buffer1.recordType == ORecordFlat.RECORD_TYPE)
                      listener.onMessage("\n--- REC1: " + rec1);
                    if (buffer2.recordType == ODocument.RECORD_TYPE || buffer2.recordType == ORecordFlat.RECORD_TYPE)
                      listener.onMessage("\n--- REC2: " + rec2);
                    listener.onMessage("\n");

                    ++differences;
                  }
                } else {
                  // CHECK BYTE PER BYTE
                  for (int b = 0; b < buffer1.buffer.length; ++b) {
                    if (buffer1.buffer[b] != buffer2.buffer[b]) {
                      listener.onMessage("\n- ERR: RID=" + clusterId + ":" + position + " content is different at byte #" + b
                          + ": " + buffer1.buffer[b] + " <-> " + buffer2.buffer[b]);
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
      }
    }

    return true;
  }

  private void convertSchemaDoc(final ODocument document) {
    if (document.field("classes") != null) {
      document.setFieldType("classes", OType.EMBEDDEDSET);
      for (ODocument classDoc : document.<Set<ODocument>> field("classes")) {
        classDoc.setFieldType("properties", OType.EMBEDDEDSET);
      }
    }
  }

  private boolean isDocumentDatabases() {
    return storage1.getConfiguration().schemaRecordId != null && storage2.getConfiguration().schemaRecordId != null;
  }

  private void reportIndexDiff(OIndex<?> indexOne, Object key, final Object indexOneValue, final Object indexTwoValue) {
    listener.onMessage("\n- ERR: Entry values for key '" + key + "' are different for index " + indexOne.getName());
    listener.onMessage("\n--- DB1: " + makeDbCall(databaseDocumentTxOne, new ODbRelatedCall<String>() {
      public String call() {
        return indexOneValue.toString();
      }
    }));
    listener.onMessage("\n--- DB2: " + makeDbCall(databaseDocumentTxOne, new ODbRelatedCall<String>() {
      public String call() {
        return indexTwoValue.toString();
      }
    }));
    listener.onMessage("\n");
    ++differences;
  }
}
