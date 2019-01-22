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
package com.orientechnologies.orient.core.db.tool;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexKeyCursor;
import com.orientechnologies.orient.core.index.OIndexManager;
import com.orientechnologies.orient.core.metadata.OMetadataDefault;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentHelper;
import com.orientechnologies.orient.core.record.impl.ODocumentHelper.ODbRelatedCall;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.OStorage;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.orientechnologies.orient.core.record.impl.ODocumentHelper.makeDbCall;

public class ODatabaseCompare extends ODatabaseImpExpAbstract {
  private ODatabaseDocumentInternal databaseOne;
  private ODatabaseDocumentInternal databaseTwo;

  private boolean compareEntriesForAutomaticIndexes = false;
  private boolean autoDetectExportImportMap         = true;

  private OIndex<OIdentifiable> exportImportHashTable = null;
  private int                   differences           = 0;
  private boolean               compareIndexMetadata  = false;

  public ODatabaseCompare(String iDb1URL, String iDb2URL, final String userName, final String userPassword,
      final OCommandOutputListener iListener) throws IOException {
    super(null, null, iListener);

    listener.onMessage("\nComparing two local databases:\n1) " + iDb1URL + "\n2) " + iDb2URL + "\n");

    databaseOne = new ODatabaseDocumentTx(iDb1URL);
    databaseOne.open(userName, userPassword);

    databaseTwo = new ODatabaseDocumentTx(iDb2URL);
    databaseTwo.open(userName, userPassword);

    // exclude automatically generated clusters
    excludeClusters.add("orids");
    excludeClusters.add(OMetadataDefault.CLUSTER_INDEX_NAME);
    excludeClusters.add(OMetadataDefault.CLUSTER_MANUAL_INDEX_NAME);
  }

  @Override
  public void run() {
    compare();
  }

  public boolean compare() {
    try {
      ODocumentHelper.RIDMapper ridMapper = null;
      if (autoDetectExportImportMap) {
        listener.onMessage(
            "\nAuto discovery of mapping between RIDs of exported and imported records is switched on, try to discover mapping data on disk.");
        exportImportHashTable = (OIndex<OIdentifiable>) databaseTwo.getMetadata().getIndexManager()
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

              databaseTwo.activateOnCurrentThread();
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

      compareSchema();
      compareIndexes(ridMapper);

      if (differences == 0) {
        listener.onMessage("\n\nDatabases match.");
        return true;
      } else {
        listener.onMessage("\n\nDatabases do not match. Found " + differences + " difference(s).");
        return false;
      }
    } catch (Exception e) {
      OLogManager.instance()
          .error(this, "Error on comparing database '%s' against '%s'", e, databaseOne.getName(), databaseTwo.getName());
      throw new ODatabaseExportException(
          "Error on comparing database '" + databaseOne.getName() + "' against '" + databaseTwo.getName() + "'", e);
    } finally {
      makeDbCall(databaseOne, new ODbRelatedCall<Void>() {
        @Override
        public Void call(ODatabaseDocumentInternal database) {
          database.close();
          return null;
        }
      });
      makeDbCall(databaseTwo, new ODbRelatedCall<Void>() {
        @Override
        public Void call(ODatabaseDocumentInternal database) {
          database.close();
          return null;
        }
      });

    }
  }

  private void compareSchema() {
    OSchema schema1 = databaseOne.getMetadata().getImmutableSchemaSnapshot();
    OSchema schema2 = databaseTwo.getMetadata().getImmutableSchemaSnapshot();
    boolean ok = true;
    for (OClass clazz : schema1.getClasses()) {
      OClass clazz2 = schema2.getClass(clazz.getName());

      if (clazz2 == null) {
        listener.onMessage("\n- ERR: Class definition " + clazz.getName() + " for DB2 is null.");
        continue;
      }

      final List<String> sc1 = clazz.getSuperClassesNames();
      final List<String> sc2 = clazz2.getSuperClassesNames();

      if (!sc1.isEmpty() || !sc2.isEmpty()) {
        if (!sc1.containsAll(sc2) || !sc2.containsAll(sc1)) {
          listener.onMessage("\n- ERR: Class definition for " + clazz.getName() + " in DB1 is not equals in superclasses in DB2.");
          ok = false;
        }
      }
      if (!clazz.getClassIndexes().equals(clazz2.getClassIndexes())) {
        listener.onMessage("\n- ERR: Class definition for " + clazz.getName() + " in DB1 is not equals in indexes in DB2.");
        ok = false;
      }
      if (!Arrays.equals(clazz.getClusterIds(), clazz2.getClusterIds())) {
        listener.onMessage("\n- ERR: Class definition for " + clazz.getName() + " in DB1 is not equals in clusters in DB2.");
        ok = false;
      }
      if (!clazz.getCustomKeys().equals(clazz2.getCustomKeys())) {
        listener.onMessage("\n- ERR: Class definition for " + clazz.getName() + " in DB1 is not equals in custom keys in DB2.");
        ok = false;
      }
      if (clazz.getOverSize() != clazz2.getOverSize()) {
        listener.onMessage("\n- ERR: Class definition for " + clazz.getName() + " in DB1 is not equals in overSize in DB2.");
        ok = false;
      }

      if (clazz.getDefaultClusterId() != clazz2.getDefaultClusterId()) {
        listener
            .onMessage("\n- ERR: Class definition for " + clazz.getName() + " in DB1 is not equals in default cluser id in DB2.");
        ok = false;
      }

      // if (clazz.getSize() != clazz2.getSize()) {
      // listener.onMessage("\n- ERR: Class definition for " + clazz.getName() + " in DB1 is not equals in size in DB2.");
      // ok = false;
      // }

      for (OProperty prop : clazz.declaredProperties()) {
        OProperty prop2 = clazz2.getProperty(prop.getName());
        if (prop2 == null) {
          listener
              .onMessage("\n- ERR: Class definition for " + clazz.getName() + " as missed property " + prop.getName() + "in DB2.");
          ok = false;
          continue;
        }
        if (prop.getType() != prop2.getType()) {
          listener.onMessage(
              "\n- ERR: Class definition for " + clazz.getName() + " as not same type for property " + prop.getName() + "in DB2. ");
          ok = false;
        }

        if (prop.getLinkedType() != prop2.getLinkedType()) {
          listener.onMessage(
              "\n- ERR: Class definition for " + clazz.getName() + " as not same linkedtype for property " + prop.getName()
                  + "in DB2.");
          ok = false;
        }

        if (prop.getMin() != null) {
          if (!prop.getMin().equals(prop2.getMin())) {
            listener.onMessage(
                "\n- ERR: Class definition for " + clazz.getName() + " as not same min for property " + prop.getName() + "in DB2.");
            ok = false;
          }
        }
        if (prop.getMax() != null) {
          if (!prop.getMax().equals(prop2.getMax())) {
            listener.onMessage(
                "\n- ERR: Class definition for " + clazz.getName() + " as not same max for property " + prop.getName() + "in DB2.");
            ok = false;
          }
        }

        if (prop.getMax() != null) {
          if (!prop.getMax().equals(prop2.getMax())) {
            listener.onMessage(
                "\n- ERR: Class definition for " + clazz.getName() + " as not same regexp for property " + prop.getName()
                    + "in DB2.");
            ok = false;
          }
        }

        if (prop.getLinkedClass() != null) {
          if (!prop.getLinkedClass().equals(prop2.getLinkedClass())) {
            listener.onMessage(
                "\n- ERR: Class definition for " + clazz.getName() + " as not same linked class for property " + prop.getName()
                    + "in DB2.");
            ok = false;
          }
        }

        if (prop.getLinkedClass() != null) {
          if (!prop.getCustomKeys().equals(prop2.getCustomKeys())) {
            listener.onMessage(
                "\n- ERR: Class definition for " + clazz.getName() + " as not same custom keys for property " + prop.getName()
                    + "in DB2.");
            ok = false;
          }
        }
        if (prop.isMandatory() != prop2.isMandatory()) {
          listener.onMessage(
              "\n- ERR: Class definition for " + clazz.getName() + " as not same mandatory flag for property " + prop.getName()
                  + "in DB2.");
          ok = false;
        }
        if (prop.isNotNull() != prop2.isNotNull()) {
          listener.onMessage(
              "\n- ERR: Class definition for " + clazz.getName() + " as not same nut null flag for property " + prop.getName()
                  + "in DB2.");
          ok = false;
        }
        if (prop.isReadonly() != prop2.isReadonly()) {
          listener.onMessage(
              "\n- ERR: Class definition for " + clazz.getName() + " as not same readonly flag setting for property " + prop
                  .getName() + "in DB2.");
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

    final OIndexManager indexManagerOne = makeDbCall(databaseOne, new ODbRelatedCall<OIndexManager>() {
      public OIndexManager call(ODatabaseDocumentInternal database) {
        return database.getMetadata().getIndexManager();
      }
    });

    final OIndexManager indexManagerTwo = makeDbCall(databaseTwo, new ODbRelatedCall<OIndexManager>() {
      public OIndexManager call(ODatabaseDocumentInternal database) {
        return database.getMetadata().getIndexManager();
      }
    });

    final Collection<? extends OIndex<?>> indexesOne = makeDbCall(databaseOne,
        new ODbRelatedCall<Collection<? extends OIndex<?>>>() {
          public Collection<? extends OIndex<?>> call(ODatabaseDocumentInternal database) {
            return indexManagerOne.getIndexes();
          }
        });

    int indexesSizeOne = makeDbCall(databaseTwo, new ODbRelatedCall<Integer>() {
      public Integer call(ODatabaseDocumentInternal database) {
        return indexesOne.size();
      }
    });

    int indexesSizeTwo = makeDbCall(databaseTwo, new ODbRelatedCall<Integer>() {
      public Integer call(ODatabaseDocumentInternal database) {
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

    final Iterator<? extends OIndex<?>> iteratorOne = makeDbCall(databaseOne, new ODbRelatedCall<Iterator<? extends OIndex<?>>>() {
      public Iterator<? extends OIndex<?>> call(ODatabaseDocumentInternal database) {
        return indexesOne.iterator();
      }
    });

    while (makeDbCall(databaseOne, new ODbRelatedCall<Boolean>() {
      public Boolean call(ODatabaseDocumentInternal database) {
        return iteratorOne.hasNext();
      }
    })) {
      final OIndex indexOne = makeDbCall(databaseOne, new ODbRelatedCall<OIndex<?>>() {
        public OIndex<?> call(ODatabaseDocumentInternal database) {
          return iteratorOne.next();
        }
      });

      final OIndex<?> indexTwo = makeDbCall(databaseTwo, new ODbRelatedCall<OIndex<?>>() {
        public OIndex<?> call(ODatabaseDocumentInternal database) {
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
        //THIS IS NORMAL SINCE 3.0 DUE OF REMOVING OF INDEX WITHOUT THE DEFINITION,  THE IMPORTER WILL CREATE THE DEFINITION
        listener.onMessage("\n- WARN: Index definition for index " + indexOne.getName() + " for DB2 is not null.");
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

      final long indexOneSize = makeDbCall(databaseOne, new ODbRelatedCall<Long>() {
        public Long call(ODatabaseDocumentInternal database) {
          return indexOne.getSize();
        }
      });

      final long indexTwoSize = makeDbCall(databaseTwo, new ODbRelatedCall<Long>() {
        public Long call(ODatabaseDocumentInternal database) {
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
        } else if (metadataOne != null && metadataTwo != null && !ODocumentHelper
            .hasSameContentOf(metadataOne, databaseOne, metadataTwo, databaseTwo, ridMapper)) {
          ok = false;
          listener.onMessage("\n- ERR: Metadata for index " + indexOne.getName() + " for DB1 and for DB2 are different.");
          makeDbCall(databaseOne, new ODbRelatedCall<Object>() {
            @Override
            public Object call(ODatabaseDocumentInternal database) {
              listener.onMessage("\n--- M1: " + metadataOne);
              return null;
            }
          });
          makeDbCall(databaseTwo, new ODbRelatedCall<Object>() {
            @Override
            public Object call(ODatabaseDocumentInternal database) {
              listener.onMessage("\n--- M2: " + metadataTwo);
              return null;
            }
          });
          listener.onMessage("\n");
          ++differences;
        }
      }

      if (((compareEntriesForAutomaticIndexes && !indexOne.getType().equals("DICTIONARY")) || !indexOne.isAutomatic())) {
        final OIndexKeyCursor indexKeyCursorOne = makeDbCall(databaseOne, new ODbRelatedCall<OIndexKeyCursor>() {
          public OIndexKeyCursor call(ODatabaseDocumentInternal database) {
            return indexOne.keyCursor();
          }
        });

        Object key = makeDbCall(databaseOne, new ODbRelatedCall<Object>() {
          @Override
          public Object call(ODatabaseDocumentInternal database) {
            return indexKeyCursorOne.next(-1);
          }
        });

        while (key != null) {
          final Object indexKey = key;

          Object indexOneValue = makeDbCall(databaseOne, new ODbRelatedCall<Object>() {
            public Object call(ODatabaseDocumentInternal database) {
              return indexOne.get(indexKey);
            }
          });

          final Object indexTwoValue = makeDbCall(databaseTwo, new ODbRelatedCall<Object>() {
            public Object call(ODatabaseDocumentInternal database) {
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

            if (!ODocumentHelper.compareSets(databaseOne, indexOneValueSet, databaseTwo, indexTwoValueSet, ridMapper)) {
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
          } else if (!compareIndexValues((Collection<ORID>) indexOneValue, (Collection<ORID>) indexTwoValue, ridMapper)) {
            ok = false;
            reportIndexDiff(indexOne, key, indexOneValue, indexTwoValue);
          }

          key = makeDbCall(databaseOne, new ODbRelatedCall<Object>() {
            @Override
            public Object call(ODatabaseDocumentInternal database) {
              return indexKeyCursorOne.next(-1);
            }
          });
        }
      }
    }

    if (ok)
      listener.onMessage("OK");
  }

  private boolean compareIndexValues(Collection<ORID> oneValue, Collection<ORID> secondValue, ODocumentHelper.RIDMapper ridMapper) {
    Map<ORID, Integer> firstMap = new HashMap<>();
    Map<ORID, Integer> secondMap = new HashMap<>();

    for (ORID rid : oneValue) {
      ORID ridToInsert;

      if (ridMapper != null) {
        if (rid.isPersistent()) {
          ridToInsert = ridMapper.map(rid);

          if (ridToInsert == null) {
            ridToInsert = rid;
          }
        } else {
          ridToInsert = rid;
        }
      } else {
        ridToInsert = rid;
      }

      firstMap.compute(ridToInsert, (k, v) -> {
        if (v == null) {
          return 1;
        }

        return v + 1;
      });
    }

    for (ORID rid : secondValue) {
      secondMap.compute(rid, (k, v) -> {
        if (v == null) {
          return 1;
        }

        return v + 1;
      });
    }

    return firstMap.equals(secondMap);
  }

  private boolean compareClusters() {
    listener.onMessage("\nStarting shallow comparison of clusters:");

    listener.onMessage("\nChecking the number of clusters...");

    Collection<String> clusterNames1 = makeDbCall(databaseOne, new ODbRelatedCall<Collection<String>>() {
      @Override
      public Collection<String> call(ODatabaseDocumentInternal database) {
        return database.getClusterNames();
      }
    });

    Collection<String> clusterNames2 = makeDbCall(databaseTwo, new ODbRelatedCall<Collection<String>>() {
      @Override
      public Collection<String> call(ODatabaseDocumentInternal database) {
        return database.getClusterNames();
      }
    });

    if (clusterNames1.size() != clusterNames2.size()) {
      listener.onMessage("ERR: cluster sizes are different: " + clusterNames1.size() + " <-> " + clusterNames2.size());
      ++differences;
    }

    boolean ok;

    for (final String clusterName : clusterNames1) {
      // CHECK IF THE CLUSTER IS INCLUDED
      if (includeClusters != null) {
        if (!includeClusters.contains(clusterName))
          continue;
      } else if (excludeClusters != null) {
        if (excludeClusters.contains(clusterName))
          continue;
      }

      ok = true;
      final int cluster1Id = makeDbCall(databaseTwo, new ODbRelatedCall<Integer>() {
        @Override
        public Integer call(ODatabaseDocumentInternal database) {
          return database.getClusterIdByName(clusterName);
        }
      });

      listener.onMessage("\n- Checking cluster " + String.format("%-25s: ", "'" + clusterName + "'"));

      if (cluster1Id == -1) {
        listener.onMessage("ERR: cluster name '" + clusterName + "' was not found on database " + databaseTwo.getName());
        ++differences;
        ok = false;
      }

      final int cluster2Id = makeDbCall(databaseOne, new ODbRelatedCall<Integer>() {
        @Override
        public Integer call(ODatabaseDocumentInternal database) {
          return database.getClusterIdByName(clusterName);
        }
      });
      if (cluster1Id != cluster2Id) {
        listener.onMessage("ERR: cluster id is different for cluster " + clusterName + ": " + cluster2Id + " <-> " + cluster1Id);
        ++differences;
        ok = false;
      }

      long countCluster1 = makeDbCall(databaseOne, new ODbRelatedCall<Long>() {
        @Override
        public Long call(ODatabaseDocumentInternal database) {
          return database.getStorage().count(cluster1Id);
        }
      });
      long countCluster2 = makeDbCall(databaseOne, new ODbRelatedCall<Long>() {
        @Override
        public Long call(ODatabaseDocumentInternal database) {
          return database.getStorage().count(cluster2Id);
        }
      });

      if (countCluster1 != countCluster2) {
        listener.onMessage(
            "ERR: number of records different in cluster '" + clusterName + "' (id=" + cluster1Id + "): " + countCluster1 + " <-> "
                + countCluster2);
        ++differences;
        ok = false;
      }

      if (ok)
        listener.onMessage("OK");
    }

    listener.onMessage("\n\nShallow analysis done.");
    return true;
  }

  @SuppressFBWarnings("NP_NULL_ON_SOME_PATH")
  private boolean compareRecords(ODocumentHelper.RIDMapper ridMapper) {
    listener.onMessage("\nStarting deep comparison record by record. This may take a few minutes. Wait please...");

    Collection<String> clusterNames1 = makeDbCall(databaseOne, new ODbRelatedCall<Collection<String>>() {
      @Override
      public Collection<String> call(ODatabaseDocumentInternal database) {
        return database.getClusterNames();
      }
    });

    for (final String clusterName : clusterNames1) {
      // CHECK IF THE CLUSTER IS INCLUDED
      if (includeClusters != null) {
        if (!includeClusters.contains(clusterName))
          continue;
      } else if (excludeClusters != null) {
        if (excludeClusters.contains(clusterName))
          continue;
      }

      final int clusterId1 = makeDbCall(databaseOne, new ODbRelatedCall<Integer>() {
        @Override
        public Integer call(ODatabaseDocumentInternal database) {
          return database.getClusterIdByName(clusterName);
        }
      });

      final long[] db1Range = makeDbCall(databaseOne, new ODbRelatedCall<long[]>() {
        @Override
        public long[] call(ODatabaseDocumentInternal database) {
          return database.getStorage().getClusterDataRange(clusterId1);
        }
      });
      final long[] db2Range = makeDbCall(databaseTwo, new ODbRelatedCall<long[]>() {
        @Override
        public long[] call(ODatabaseDocumentInternal database) {
          return database.getStorage().getClusterDataRange(clusterId1);
        }
      });

      final long db1Max = db1Range[1];
      final long db2Max = db2Range[1];

      databaseOne.activateOnCurrentThread();
      final ODocument doc1 = new ODocument();
      databaseTwo.activateOnCurrentThread();
      final ODocument doc2 = new ODocument();

      final ORecordId rid = new ORecordId(clusterId1);

      // TODO why this maximums can be different?
      final long clusterMax = Math.max(db1Max, db2Max);

      final OStorage storage;

      ODatabaseDocumentInternal selectedDatabase;
      if (clusterMax == db1Max)
        selectedDatabase = databaseOne;
      else
        selectedDatabase = databaseTwo;

      OPhysicalPosition[] physicalPositions = makeDbCall(selectedDatabase, new ODbRelatedCall<OPhysicalPosition[]>() {
        @Override
        public OPhysicalPosition[] call(ODatabaseDocumentInternal database) {
          return database.getStorage().ceilingPhysicalPositions(clusterId1, new OPhysicalPosition(0));
        }
      });

      OStorageConfiguration configuration1 = makeDbCall(databaseOne, new ODbRelatedCall<OStorageConfiguration>() {
        @Override
        public OStorageConfiguration call(ODatabaseDocumentInternal database) {
          return database.getStorage().getConfiguration();
        }
      });
      OStorageConfiguration configuration2 = makeDbCall(databaseTwo, new ODbRelatedCall<OStorageConfiguration>() {
        @Override
        public OStorageConfiguration call(ODatabaseDocumentInternal database) {
          return database.getStorage().getConfiguration();
        }
      });
      String storageType1 = makeDbCall(databaseOne, new ODbRelatedCall<String>() {
        @Override
        public String call(ODatabaseDocumentInternal database) {
          return database.getStorage().getType();
        }
      });
      String storageType2 = makeDbCall(databaseTwo, new ODbRelatedCall<String>() {
        @Override
        public String call(ODatabaseDocumentInternal database) {
          return database.getStorage().getType();
        }
      });

      long recordsCounter = 0;
      while (physicalPositions.length > 0) {
        for (OPhysicalPosition physicalPosition : physicalPositions) {
          try {
            recordsCounter++;

            final long position = physicalPosition.clusterPosition;
            rid.setClusterPosition(position);

            if (rid.equals(new ORecordId(configuration1.getIndexMgrRecordId())) && rid
                .equals(new ORecordId(configuration2.getIndexMgrRecordId())))
              continue;
            if (rid.equals(new ORecordId(configuration1.getSchemaRecordId())) && rid
                .equals(new ORecordId(configuration2.getSchemaRecordId())))
              continue;

            if (rid.getClusterId() == 0 && rid.getClusterPosition() == 0) {
              // Skip the compare of raw structure if the storage type are different, due the fact that are different by definition.
              if (!storageType1.equals(storageType2))
                continue;
            }

            final ORecordId rid2;
            if (ridMapper == null)
              rid2 = rid;
            else {
              final ORID newRid = ridMapper.map(rid);
              if (newRid == null)
                rid2 = rid;
              else
                rid2 = new ORecordId(newRid);
            }

            final ORawBuffer buffer1 = makeDbCall(databaseOne, new ODbRelatedCall<ORawBuffer>() {
              @Override
              public ORawBuffer call(ODatabaseDocumentInternal database) {
                return database.getStorage().readRecord(rid, null, true, false, null).getResult();
              }
            });
            final ORawBuffer buffer2 = makeDbCall(databaseTwo, new ODbRelatedCall<ORawBuffer>() {
              @Override
              public ORawBuffer call(ODatabaseDocumentInternal database) {
                return database.getStorage().readRecord(rid2, null, true, false, null).getResult();
              }
            });

            if (buffer1 == null && buffer2 == null)
              // BOTH RECORD NULL, OK
              continue;
            else if (buffer1 == null && buffer2 != null) {
              // REC1 NULL
              listener.onMessage("\n- ERR: RID=" + clusterId1 + ":" + position + " is null in DB1");
              ++differences;
            } else if (buffer1 != null && buffer2 == null) {
              // REC2 NULL
              listener.onMessage("\n- ERR: RID=" + clusterId1 + ":" + position + " is null in DB2");
              ++differences;
            } else {
              if (buffer1.recordType != buffer2.recordType) {
                listener.onMessage(
                    "\n- ERR: RID=" + clusterId1 + ":" + position + " recordType is different: " + (char) buffer1.recordType
                        + " <-> " + (char) buffer2.recordType);
                ++differences;
              }

              if (buffer1.buffer == null && buffer2.buffer == null) {
              } else if (buffer1.buffer == null && buffer2.buffer != null) {
                listener.onMessage(
                    "\n- ERR: RID=" + clusterId1 + ":" + position + " content is different: null <-> " + buffer2.buffer.length);
                ++differences;

              } else if (buffer1.buffer != null && buffer2.buffer == null) {
                listener.onMessage("\n- ERR: RID=" + clusterId1 + ":" + position + " content is different: " + buffer1.buffer.length
                    + " <-> null");
                ++differences;

              } else {
                if (buffer1.recordType == ODocument.RECORD_TYPE) {
                  // DOCUMENT: TRY TO INSTANTIATE AND COMPARE

                  makeDbCall(databaseOne, new ODocumentHelper.ODbRelatedCall<Object>() {
                    public Object call(ODatabaseDocumentInternal database) {
                      doc1.reset();
                      doc1.fromStream(buffer1.buffer);
                      return null;
                    }
                  });

                  makeDbCall(databaseTwo, new ODocumentHelper.ODbRelatedCall<Object>() {
                    public Object call(ODatabaseDocumentInternal database) {
                      doc2.reset();
                      doc2.fromStream(buffer2.buffer);
                      return null;
                    }
                  });

                  if (rid.toString().equals(configuration1.getSchemaRecordId()) && rid.toString()
                      .equals(configuration2.getSchemaRecordId())) {
                    makeDbCall(databaseOne, new ODocumentHelper.ODbRelatedCall<java.lang.Object>() {
                      public Object call(ODatabaseDocumentInternal database) {
                        convertSchemaDoc(doc1);
                        return null;
                      }
                    });

                    makeDbCall(databaseTwo, new ODocumentHelper.ODbRelatedCall<java.lang.Object>() {
                      public Object call(ODatabaseDocumentInternal database) {
                        convertSchemaDoc(doc2);
                        return null;
                      }
                    });
                  }

                  if (!ODocumentHelper.hasSameContentOf(doc1, databaseOne, doc2, databaseTwo, ridMapper)) {
                    listener.onMessage("\n- ERR: RID=" + clusterId1 + ":" + position + " document content is different");
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
                      listener.onMessage(
                          "\n- ERR: RID=" + clusterId1 + ":" + position + " content length is different: " + buffer1.buffer.length
                              + " <-> " + buffer2.buffer.length);

                      if (buffer1.recordType == ODocument.RECORD_TYPE)
                        listener.onMessage("\n--- REC1: " + rec1);
                      if (buffer2.recordType == ODocument.RECORD_TYPE)
                        listener.onMessage("\n--- REC2: " + rec2);
                      listener.onMessage("\n");

                      ++differences;
                    }
                  } else {
                    // CHECK BYTE PER BYTE
                    for (int b = 0; b < buffer1.buffer.length; ++b) {
                      if (buffer1.buffer[b] != buffer2.buffer[b]) {
                        listener.onMessage(
                            "\n- ERR: RID=" + clusterId1 + ":" + position + " content is different at byte #" + b + ": "
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
          } catch (RuntimeException e) {
            OLogManager.instance().error(this, "Error during data comparison of records with rid " + rid, e);
            throw e;
          }
        }
        final OPhysicalPosition[] curPosition = physicalPositions;
        physicalPositions = makeDbCall(selectedDatabase, new ODbRelatedCall<OPhysicalPosition[]>() {
          @Override
          public OPhysicalPosition[] call(ODatabaseDocumentInternal database) {
            return database.getStorage().higherPhysicalPositions(clusterId1, curPosition[curPosition.length - 1]);
          }
        });
        if (recordsCounter % 10000 == 0)
          listener.onMessage("\n" + recordsCounter + " records were processed for cluster " + clusterName + " ...");
      }

      listener.onMessage(
          "\nCluster comparison was finished, " + recordsCounter + " records were processed for cluster " + clusterName + " ...");
    }

    return true;
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

  private void convertSchemaDoc(final ODocument document) {
    if (document.field("classes") != null) {
      document.setFieldType("classes", OType.EMBEDDEDSET);
      for (ODocument classDoc : document.<Set<ODocument>>field("classes")) {
        classDoc.setFieldType("properties", OType.EMBEDDEDSET);
      }
    }
  }

  private void reportIndexDiff(OIndex<?> indexOne, Object key, final Object indexOneValue, final Object indexTwoValue) {
    listener.onMessage("\n- ERR: Entry values for key '" + key + "' are different for index " + indexOne.getName());
    listener.onMessage("\n--- DB1: " + makeDbCall(databaseOne, new ODbRelatedCall<String>() {
      public String call(ODatabaseDocumentInternal database) {
        return indexOneValue.toString();
      }
    }));
    listener.onMessage("\n--- DB2: " + makeDbCall(databaseOne, new ODbRelatedCall<String>() {
      public String call(ODatabaseDocumentInternal database) {
        return indexTwoValue.toString();
      }
    }));
    listener.onMessage("\n");
    ++differences;
  }
}
