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
package com.orientechnologies.orient.core.db.tool;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexKeyCursor;
import com.orientechnologies.orient.core.index.OIndexManager;
import com.orientechnologies.orient.core.metadata.OMetadataDefault;
import com.orientechnologies.orient.core.metadata.OMetadataInternal;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentHelper;
import com.orientechnologies.orient.core.record.impl.ODocumentHelper.ODbRelatedCall;
import com.orientechnologies.orient.core.record.impl.ORecordFlat;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.OStorage;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import static com.orientechnologies.orient.core.record.impl.ODocumentHelper.makeDbCall;

public class ODatabaseCompare extends ODatabaseImpExpAbstract {
  private OStorage              storage1;
  private OStorage              storage2;

  private ODatabaseDocumentTx   databaseDocumentTxOne;
  private ODatabaseDocumentTx   databaseDocumentTxTwo;

  private boolean               compareEntriesForAutomaticIndexes = false;
  private boolean               autoDetectExportImportMap         = true;

  private OIndex<OIdentifiable> exportImportHashTable             = null;
  private int                   differences                       = 0;
  private boolean               compareIndexMetadata              = false;

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
    excludeClusters.add(OMetadataDefault.CLUSTER_INDEX_NAME);
    excludeClusters.add(OMetadataDefault.CLUSTER_MANUAL_INDEX_NAME);
  }

  public void setCompareIndexMetadata(boolean compareIndexMetadata) {
    this.compareIndexMetadata = compareIndexMetadata;
  }

  public boolean isCompareEntriesForAutomaticIndexes() {
    return compareEntriesForAutomaticIndexes;
  }

  public void setCompareEntriesForAutomaticIndexes(boolean compareEntriesForAutomaticIndexes) {
    this.compareEntriesForAutomaticIndexes = compareEntriesForAutomaticIndexes;
  }

  public void setAutoDetectExportImportMap(boolean autoDetectExportImportMap) {
    this.autoDetectExportImportMap = autoDetectExportImportMap;
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
      ODocumentHelper.RIDMapper ridMapper = null;
      if (autoDetectExportImportMap) {
        listener
            .onMessage("\nAuto discovery of mapping between RIDs of exported and imported records is switched on, try to discover mapping data on disk.");
        exportImportHashTable = (OIndex<OIdentifiable>) databaseDocumentTxTwo.getMetadata().getIndexManager()
            .getIndex(ODatabaseImport.EXPORT_IMPORT_MAP_NAME);
        if (exportImportHashTable != null) {
          listener.onMessage("\nMapping data were found and will be loaded.");
          ridMapper = new ODocumentHelper.RIDMapper() {
            @Override
            public ORID map(ORID rid) {
              if (rid == null)
                return null;

              if (!rid.isPersistent())
                return null;

              final OIdentifiable result = exportImportHashTable.get(rid);
              if (result == null)
                return null;

              return result.getIdentity();
            }
          };
        } else
          listener.onMessage("\nMapping data were not found.");
      }

      compareClusters();
      compareRecords(ridMapper);

      if (isDocumentDatabases()) {
        compareSchama();
        compareIndexes(ridMapper);
      }

      if (differences == 0) {
        listener.onMessage("\n\nDatabases match.");
        return true;
      } else {
        listener.onMessage("\n\nDatabases do not match. Found " + differences + " difference(s).");
        return false;
      }
    } catch (Exception e) {
      OLogManager.instance()
          .error(this, "Error on comparing database '%s' against '%s'", e, storage1.getName(), storage2.getName());
      throw new ODatabaseExportException("Error on comparing database '" + storage1.getName() + "' against '" + storage2.getName()
          + "'", e);
    } finally {
      storage1.close();
      storage2.close();
    }
  }

  private void compareSchama() {
    OSchema schema1 = ((OMetadataInternal) databaseDocumentTxOne.getMetadata()).getImmutableSchemaSnapshot();
    OSchema schema2 = ((OMetadataInternal) databaseDocumentTxTwo.getMetadata()).getImmutableSchemaSnapshot();
    boolean ok = true;
    for (OClass clazz : schema1.getClasses()) {
      OClass clazz2 = schema2.getClass(clazz.getName());
      if (clazz2 == null) {
        listener.onMessage("\n- ERR: Class definition " + clazz.getName() + " for DB2 is null.");
        continue;
      }
      if (clazz.getSuperClass() != null) {
        if (!clazz.getSuperClass().getName().equals(clazz2.getSuperClass().getName())) {
          listener.onMessage("\n- ERR: Class definition for " + clazz.getName() + " as not same superclass in DB2.");
          ok = false;
        }
      }
      if (!clazz.getClassIndexes().equals(clazz2.getClassIndexes())) {
        listener.onMessage("\n- ERR: Class definition for " + clazz.getName() + " as not same defined indexes in DB2.");
        ok = false;
      }
      if (!Arrays.equals(clazz.getClusterIds(), clazz2.getClusterIds())) {
        listener.onMessage("\n- ERR: Class definition for " + clazz.getName() + " as not same defined clusters in DB2.");
        ok = false;
      }
      if (!clazz.getCustomKeys().equals(clazz2.getCustomKeys())) {
        listener.onMessage("\n- ERR: Class definition for " + clazz.getName() + " as not same defined custom keys in DB2.");
        ok = false;
      }
      if ((clazz.getJavaClass() == null && clazz2.getJavaClass() != null)
          || (clazz.getJavaClass() != null && !clazz.getJavaClass().equals(clazz2.getJavaClass()))) {
        listener.onMessage("\n- ERR: Class definition for " + clazz.getName() + " as not same defined Java class in DB2.");
        ok = false;
      }
      if (clazz.getOverSize() != clazz2.getOverSize()) {
        listener.onMessage("\n- ERR: Class definition for " + clazz.getName() + " as not same defined overSize in DB2.");
        ok = false;
      }

      if (clazz.getDefaultClusterId() != clazz2.getDefaultClusterId()) {
        listener.onMessage("\n- ERR: Class definition for " + clazz.getName() + " as not same defined default cluser id in DB2.");
        ok = false;
      }

      if (clazz.getSize() != clazz2.getSize()) {
        listener.onMessage("\n- ERR: Class definition for " + clazz.getName() + " as not same defined size in DB2.");
        ok = false;
      }
      for (OProperty prop : clazz.declaredProperties()) {
        OProperty prop2 = clazz2.getProperty(prop.getName());
        if (prop2 == null) {
          listener.onMessage("\n- ERR: Class definition for " + clazz.getName() + " as missed property " + prop.getName()
              + "in DB2.");
          ok = false;
          continue;
        }
        if (prop.getType() != prop2.getType()) {
          listener.onMessage("\n- ERR: Class definition for " + clazz.getName() + " as not same type for property "
              + prop.getName() + "in DB2. ");
          ok = false;
        }

        if (prop.getLinkedType() != prop2.getLinkedType()) {
          listener.onMessage("\n- ERR: Class definition for " + clazz.getName() + " as not same linkedtype for property "
              + prop.getName() + "in DB2.");
          ok = false;
        }

        if (prop.getMin() != null) {
          if (!prop.getMin().equals(prop2.getMin())) {
            listener.onMessage("\n- ERR: Class definition for " + clazz.getName() + " as not same min for property "
                + prop.getName() + "in DB2.");
            ok = false;
          }
        }
        if (prop.getMax() != null) {
          if (!prop.getMax().equals(prop2.getMax())) {
            listener.onMessage("\n- ERR: Class definition for " + clazz.getName() + " as not same max for property "
                + prop.getName() + "in DB2.");
            ok = false;
          }
        }

        if (prop.getMax() != null) {
          if (!prop.getMax().equals(prop2.getMax())) {
            listener.onMessage("\n- ERR: Class definition for " + clazz.getName() + " as not same regexp for property "
                + prop.getName() + "in DB2.");
            ok = false;
          }
        }

        if (prop.getLinkedClass() != null) {
          if (!prop.getLinkedClass().equals(prop2.getLinkedClass())) {
            listener.onMessage("\n- ERR: Class definition for " + clazz.getName() + " as not same linked class for property "
                + prop.getName() + "in DB2.");
            ok = false;
          }
        }

        if (prop.getLinkedClass() != null) {
          if (!prop.getCustomKeys().equals(prop2.getCustomKeys())) {
            listener.onMessage("\n- ERR: Class definition for " + clazz.getName() + " as not same custom keys for property "
                + prop.getName() + "in DB2.");
            ok = false;
          }
        }
        if (prop.isMandatory() != prop2.isMandatory()) {
          listener.onMessage("\n- ERR: Class definition for " + clazz.getName() + " as not same mandatory flag for property "
              + prop.getName() + "in DB2.");
          ok = false;
        }
        if (prop.isNotNull() != prop2.isNotNull()) {
          listener.onMessage("\n- ERR: Class definition for " + clazz.getName() + " as not same nut null flag for property "
              + prop.getName() + "in DB2.");
          ok = false;
        }
        if (prop.isReadonly() != prop2.isReadonly()) {
          listener.onMessage("\n- ERR: Class definition for " + clazz.getName()
              + " as not same readonly flag setting for property " + prop.getName() + "in DB2.");
          ok = false;
        }

      }
      if (!ok) {
        ++differences;
        ok = true;
      }
    }
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  private void compareIndexes(ODocumentHelper.RIDMapper ridMapper) {
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

    int indexesSizeOne = makeDbCall(databaseDocumentTxTwo, new ODbRelatedCall<Integer>() {
      public Integer call() {
        return indexesOne.size();
      }
    });

    int indexesSizeTwo = makeDbCall(databaseDocumentTxTwo, new ODbRelatedCall<Integer>() {
      public Integer call() {
        return indexManagerTwo.getIndexes().size();
      }
    });

    if (exportImportHashTable != null)
      indexesSizeTwo--;

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

      if (compareIndexMetadata) {
        final ODocument metadataOne = indexOne.getMetadata();
        final ODocument metadataTwo = indexTwo.getMetadata();

        if (metadataOne == null && metadataTwo != null) {
          ok = false;
          listener.onMessage("\n- ERR: Metadata for index " + indexOne.getName() + " for DB1 is null but for DB2 is not.");
          listener.onMessage("\n");
          ++differences;
        } else if (metadataOne != null && metadataTwo == null) {
          ok = false;
          listener.onMessage("\n- ERR: Metadata for index " + indexOne.getName() + " for DB1 is not null but for DB2 is null.");
          listener.onMessage("\n");
          ++differences;
        } else if (metadataOne != null && metadataTwo != null
            && !ODocumentHelper.hasSameContentOf(metadataOne, databaseDocumentTxOne, metadataTwo, databaseDocumentTxTwo, ridMapper)) {
          ok = false;
          listener.onMessage("\n- ERR: Metadata for index " + indexOne.getName() + " for DB1 and for DB2 are different.");
          makeDbCall(databaseDocumentTxOne, new ODbRelatedCall<Object>() {
            @Override
            public Object call() {
              listener.onMessage("\n--- M1: " + metadataOne);
              return null;
            }
          });
          makeDbCall(databaseDocumentTxTwo, new ODbRelatedCall<Object>() {
            @Override
            public Object call() {
              listener.onMessage("\n--- M2: " + metadataTwo);
              return null;
            }
          });
          listener.onMessage("\n");
          ++differences;
        }
      }

      if (((compareEntriesForAutomaticIndexes && !indexOne.getType().equals("DICTIONARY")) || !indexOne.isAutomatic())) {
        final OIndexKeyCursor indexKeyCursorOne = makeDbCall(databaseDocumentTxOne, new ODbRelatedCall<OIndexKeyCursor>() {
          public OIndexKeyCursor call() {
            return indexOne.keyCursor();
          }
        });

        Object key = makeDbCall(databaseDocumentTxOne, new ODbRelatedCall<Object>() {
          @Override
          public Object call() {
            return indexKeyCursorOne.next(-1);
          }
        });

        while (key != null) {
          final Object indexKey = key;

          Object indexOneValue = makeDbCall(databaseDocumentTxOne, new ODbRelatedCall<Object>() {
            public Object call() {
              return indexOne.get(indexKey);
            }
          });

          final Object indexTwoValue = makeDbCall(databaseDocumentTxTwo, new ODbRelatedCall<Object>() {
            public Object call() {
              return indexTwo.get(indexKey);
            }
          });

          if (indexTwoValue == null) {
            ok = false;
            listener.onMessage("\n- ERR: Entry with key " + key + " is absent in index " + indexOne.getName() + " for DB2.");
            ++differences;
          } else if (indexOneValue instanceof Set && indexTwoValue instanceof Set) {
            final Set<Object> indexOneValueSet = (Set<Object>) indexOneValue;
            final Set<Object> indexTwoValueSet = (Set<Object>) indexTwoValue;

            if (!ODocumentHelper.compareSets(databaseDocumentTxOne, indexOneValueSet, databaseDocumentTxTwo, indexTwoValueSet,
                ridMapper)) {
              ok = false;
              reportIndexDiff(indexOne, key, indexOneValue, indexTwoValue);
            }
          } else if (indexOneValue instanceof ORID && indexTwoValue instanceof ORID) {
            if (ridMapper != null && ((ORID) indexOneValue).isPersistent()) {
              OIdentifiable identifiable = ridMapper.map((ORID) indexOneValue);

              if (identifiable != null)
                indexOneValue = identifiable.getIdentity();
            }
            if (!indexOneValue.equals(indexTwoValue)) {
              ok = false;
              reportIndexDiff(indexOne, key, indexOneValue, indexTwoValue);
            }
          } else if (!indexOneValue.equals(indexTwoValue)) {
            ok = false;
            reportIndexDiff(indexOne, key, indexOneValue, indexTwoValue);
          }

          key = makeDbCall(databaseDocumentTxOne, new ODbRelatedCall<Object>() {
            @Override
            public Object call() {
              return indexKeyCursorOne.next(-1);
            }
          });
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
        listener.onMessage("ERR: cluster name '" + clusterName + "' was not found on database " + storage2);
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

  private boolean compareRecords(ODocumentHelper.RIDMapper ridMapper) {
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

      final long[] db1Range = storage1.getClusterDataRange(clusterId);
      final long[] db2Range = storage2.getClusterDataRange(clusterId);

      final long db1Max = db1Range[1];
      final long db2Max = db2Range[1];

      ODatabaseRecordThreadLocal.INSTANCE.set(databaseDocumentTxOne);
      final ODocument doc1 = new ODocument();
      ODatabaseRecordThreadLocal.INSTANCE.set(databaseDocumentTxTwo);
      final ODocument doc2 = new ODocument();

      final ORecordId rid = new ORecordId(clusterId);

      // TODO why this maximums can be different?
      final long clusterMax = Math.max(db1Max, db2Max);

      final OStorage storage;

      if (clusterMax == db1Max)
        storage = storage1;
      else
        storage = storage2;

      OPhysicalPosition[] physicalPositions = storage.ceilingPhysicalPositions(clusterId, new OPhysicalPosition(0));

      long recordsCounter = 0;
      while (physicalPositions.length > 0) {
        for (OPhysicalPosition physicalPosition : physicalPositions) {
          recordsCounter++;

          final long position = physicalPosition.clusterPosition;
          rid.clusterPosition = position;

          if (isDocumentDatabases() && rid.equals(new ORecordId(storage1.getConfiguration().indexMgrRecordId))
              && rid.equals(new ORecordId(storage2.getConfiguration().indexMgrRecordId)))
            continue;
          if (isDocumentDatabases() && rid.equals(new ORecordId(storage1.getConfiguration().schemaRecordId))
              && rid.equals(new ORecordId(storage2.getConfiguration().schemaRecordId)))
            continue;

          final ORawBuffer buffer1 = storage1.readRecord(rid, null, true, null).getResult();
          final ORawBuffer buffer2;
          if (ridMapper == null)
            buffer2 = storage2.readRecord(rid, null, true, null).getResult();
          else {
            final ORID newRid = ridMapper.map(rid);
            if (newRid == null)
              buffer2 = storage2.readRecord(rid, null, true, null).getResult();
            else
              buffer2 = storage2.readRecord(new ORecordId(newRid), null, true, null).getResult();
          }

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

                if (!ODocumentHelper.hasSameContentOf(doc1, databaseDocumentTxOne, doc2, databaseDocumentTxTwo, ridMapper)) {
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

        physicalPositions = storage.higherPhysicalPositions(clusterId, physicalPositions[physicalPositions.length - 1]);
        if (recordsCounter % 10000 == 0)
          listener.onMessage("\n" + recordsCounter + " records were processed for cluster " + clusterName + " ...");
      }

      listener.onMessage("\nCluster comparison was finished, " + recordsCounter + " records were processed for cluster "
          + clusterName + " ...");
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
