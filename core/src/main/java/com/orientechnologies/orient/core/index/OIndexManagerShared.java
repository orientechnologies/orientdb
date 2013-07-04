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
package com.orientechnologies.orient.core.index;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OMultiKey;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.db.record.ORecordTrackedSet;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.OMetadata;
import com.orientechnologies.orient.core.metadata.schema.OSchemaShared;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.OCluster.ATTRIBUTES;
import com.orientechnologies.orient.core.storage.impl.local.OClusterLocal;

/**
 * Manages indexes at database level. A single instance is shared among multiple databases. Contentions are managed by r/w locks.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * @author Artem Orobets added composite index managemement
 * 
 */
public class OIndexManagerShared extends OIndexManagerAbstract implements OIndexManager {
  private static final long serialVersionUID      = 1L;

  protected volatile Thread recreateIndexesThread = null;

  public OIndexManagerShared(final ODatabaseRecord iDatabase) {
    super(iDatabase);
  }

  public OIndex<?> getIndexInternal(final String iName) {
    acquireSharedLock();
    try {
      final OIndex<?> index = indexes.get(iName.toLowerCase());
      return getIndexInstance(index);
    } finally {
      releaseSharedLock();
    }
  }

  /**
   * 
   * 
   * @param iName
   *          - name of index
   * @param iType
   * @param iClusterIdsToIndex
   * @param iProgressListener
   */
  public OIndex<?> createIndex(final String iName, final String iType, final OIndexDefinition indexDefinition,
      final int[] iClusterIdsToIndex, OProgressListener iProgressListener) {
    if (getDatabase().getTransaction().isActive())
      throw new IllegalStateException("Cannot create a new index inside a transaction");

    final Character c = OSchemaShared.checkNameIfValid(iName);
    if (c != null)
      throw new IllegalArgumentException("Invalid index name '" + iName + "'. Character '" + c + "' is invalid");

    acquireExclusiveLock();
    try {
      final OIndexInternal<?> index = OIndexes.createIndex(getDatabase(), iType);

      // decide which cluster to use ("index" - for automatic and "manindex" for manual)
      final String clusterName = indexDefinition != null && indexDefinition.getClassName() != null ? defaultClusterName
          : manualClusterName;

      if (iProgressListener == null)
        // ASSIGN DEFAULT PROGRESS LISTENER
        iProgressListener = new OIndexRebuildOutputListener(index);

      index.create(iName, indexDefinition, getDatabase(), clusterName, iClusterIdsToIndex, true, iProgressListener);
      addIndexInternal(index);

      setDirty();
      save();

      return getIndexInstance(index);
    } finally {
      releaseExclusiveLock();
    }
  }

  public OIndexManager dropIndex(final String iIndexName) {
    if (getDatabase().getTransaction().isActive())
      throw new IllegalStateException("Cannot drop an index inside a transaction");

    acquireExclusiveLock();
    try {
      final OIndex<?> idx = indexes.remove(iIndexName.toLowerCase());
      if (idx != null) {
        removeClassPropertyIndex(idx);

        idx.delete();
        setDirty();
        save();
      }
      return this;
    } finally {
      releaseExclusiveLock();
    }
  }

  private void removeClassPropertyIndex(final OIndex<?> idx) {
    final OIndexDefinition indexDefinition = idx.getDefinition();
    if (indexDefinition == null || indexDefinition.getClassName() == null)
      return;

    final Map<OMultiKey, Set<OIndex<?>>> map = classPropertyIndex.get(indexDefinition.getClassName().toLowerCase());

    if (map == null) {
      return;
    }

    final int paramCount = indexDefinition.getParamCount();

    for (int i = 1; i <= paramCount; i++) {
      final List<String> fields = normalizeFieldNames(indexDefinition.getFields().subList(0, i));
      final OMultiKey multiKey = new OMultiKey(fields);
      final Set<OIndex<?>> indexSet = map.get(multiKey);
      if (indexSet == null)
        continue;
      indexSet.remove(idx);
      if (indexSet.isEmpty()) {
        map.remove(multiKey);
      }
    }

    if (map.isEmpty())
      classPropertyIndex.remove(indexDefinition.getClassName().toLowerCase());
  }

  @Override
  protected void fromStream() {
    acquireExclusiveLock();
    try {
      final Collection<ODocument> idxs = document.field(CONFIG_INDEXES);

      if (idxs != null) {
        OIndexInternal<?> index;
        boolean configUpdated = false;
        for (final ODocument d : idxs) {
          try {
            index = OIndexes.createIndex(getDatabase(), (String) d.field(OIndexInternal.CONFIG_TYPE));
            if (((OIndexInternal<?>) index).loadFromConfiguration(d)) {
              addIndexInternal(index);
            } else
              configUpdated = true;

          } catch (Exception e) {
            OLogManager.instance().error(this, "Error on loading index by configuration: %s", e, d);
          }
        }

        if (configUpdated)
          save();
      }
    } finally {
      releaseExclusiveLock();
    }
  }

  /**
   * Binds POJO to ODocument.
   */
  @Override
  public ODocument toStream() {
    acquireExclusiveLock();
    try {
      document.setInternalStatus(ORecordElement.STATUS.UNMARSHALLING);

      try {
        final ORecordTrackedSet idxs = new ORecordTrackedSet(document);

        for (final OIndex<?> i : indexes.values()) {
          idxs.add(((OIndexInternal<?>) i).updateConfiguration());
        }
        document.field(CONFIG_INDEXES, idxs, OType.EMBEDDEDSET);

      } finally {
        document.setInternalStatus(ORecordElement.STATUS.LOADED);
      }
      document.setDirty();

      return document;
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  protected OIndex<?> getIndexInstance(final OIndex<?> index) {
    return index;
  }

  @Override
  public void recreateIndexes() {
    acquireExclusiveLock();
    try {
      if (recreateIndexesThread != null && recreateIndexesThread.isAlive())
        // BUILDING ALREADY IN PROGRESS
        return;

      final ODatabaseRecord db = getDatabase();
      document = db.load(new ORecordId(getDatabase().getStorage().getConfiguration().indexMgrRecordId));
      final ODocument doc = new ODocument();
      document.copyTo(doc);

      // USE A NEW DB INSTANCE
      final ODatabaseDocumentTx newDb = new ODatabaseDocumentTx(db.getURL());

      Runnable recreateIndexesTask = new Runnable() {
        @Override
        public void run() {
          try {
            // START IT IN BACKGROUND
            newDb.setProperty(ODatabase.OPTIONS.SECURITY.toString(), Boolean.FALSE);
            newDb.open("admin", "nopass");

            ODatabaseRecordThreadLocal.INSTANCE.set(newDb);
            try {
              // DROP AND RE-CREATE 'INDEX' DATA-SEGMENT AND CLUSTER IF ANY
              final int dataId = newDb.getStorage().getDataSegmentIdByName(OMetadata.DATASEGMENT_INDEX_NAME);
              if (dataId > -1)
                newDb.getStorage().dropDataSegment(OMetadata.DATASEGMENT_INDEX_NAME);

              final int clusterId = newDb.getStorage().getClusterIdByName(OMetadata.CLUSTER_INDEX_NAME);
              if (clusterId > -1)
                newDb.dropCluster(clusterId, false);

              newDb.addDataSegment(OMetadata.DATASEGMENT_INDEX_NAME, null);
              newDb.getStorage().addCluster(OClusterLocal.TYPE, OMetadata.CLUSTER_INDEX_NAME, null,
                  OMetadata.DATASEGMENT_INDEX_NAME, true);

            } catch (IllegalArgumentException ex) {
              // OLD DATABASE: CREATE SEPARATE DATASEGMENT AND LET THE INDEX CLUSTER TO POINT TO IT
              OLogManager.instance().info(this, "Creating 'index' data-segment to store all the index content...");

              newDb.addDataSegment(OMetadata.DATASEGMENT_INDEX_NAME, null);
              final OCluster indexCluster = newDb.getStorage().getClusterById(
                  newDb.getStorage().getClusterIdByName(OMetadata.CLUSTER_INDEX_NAME));
              try {
                indexCluster.set(ATTRIBUTES.DATASEGMENT, OMetadata.DATASEGMENT_INDEX_NAME);
                OLogManager.instance().info(this,
                    "Data-segment 'index' create correctly. Indexes will store content into this data-segment");
              } catch (IOException e) {
                OLogManager.instance().error(this, "Error changing data segment for cluster 'index'", e);
              }
            }

            final Collection<ODocument> idxs = doc.field(CONFIG_INDEXES);
            if (idxs == null) {
              OLogManager.instance().warn(this, "List of indexes is empty.");
              return;
            }

            int ok = 0;
            int errors = 0;
            for (ODocument idx : idxs) {
              try {
                String indexType = idx.field(OIndexInternal.CONFIG_TYPE);
                if (indexType == null) {
                  OLogManager.instance().error(this, "Index type is null, will process other record.");
                  errors++;
                  continue;
                }

                final OIndexInternal<?> index = OIndexes.createIndex(newDb, indexType);
                OIndexInternal.IndexMetadata indexMetadata = index.loadMetadata(idx);
                OIndexDefinition indexDefinition = indexMetadata.getIndexDefinition();

                if (indexDefinition == null || !indexDefinition.isAutomatic()) {
                  OLogManager.instance().info(this, "Index %s is not automatic index and will be added as is.",
                      indexMetadata.getName());

                  if (index.loadFromConfiguration(idx)) {
                    addIndexInternal(index);
                    setDirty();
                    save();

                    ok++;
                  } else {
                    index.delete();
                    errors++;
                  }

                  OLogManager.instance().info(this, "Index %s was added in DB index list.", index.getName());
                } else {
                  String indexName = indexMetadata.getName();
                  Set<String> clusters = indexMetadata.getClustersToIndex();
                  String type = indexMetadata.getType();

                  if (indexName != null && indexDefinition != null && clusters != null && !clusters.isEmpty() && type != null) {
                    int[] clustersToIndex = new int[clusters.size()];
                    int clustersCounter = 0;

                    for (String clusterName : clusters) {
                      int clusterId = newDb.getClusterIdByName(clusterName);
                      if (clusterId == -1)
                        OLogManager.instance().error(this, "Cluster %s is required for index %s but is absent.", clusterName,
                            indexName);
                      else {
                        clustersToIndex[clustersCounter] = clusterId;
                        clustersCounter++;
                      }
                    }

                    clustersToIndex = Arrays.copyOf(clustersToIndex, clustersCounter);

                    OLogManager.instance().info(this, "Start creation of index %s", indexName);

                    index.create(indexName, indexDefinition, newDb, defaultClusterName, clustersToIndex, false,
                        new OIndexRebuildOutputListener(index));

                    index.setRebuildingFlag();
                    addIndexInternal(index);

                    OLogManager.instance().info(this, "Index %s was successfully created and rebuild is going to be started.",
                        indexName);

                    index.rebuild(new OIndexRebuildOutputListener(index));
                    index.flush();

                    setDirty();
                    save();

                    ok++;

                    OLogManager.instance().info(this, "Rebuild of %s index was successfully finished.", indexName);
                  } else {
                    errors++;
                    OLogManager.instance().error(
                        this,
                        "Information about index was restored incorrectly, following data were loaded : "
                            + "index name - %s, index definition %s, clusters %s, type %s.", indexName, indexDefinition, clusters,
                        type);
                  }
                }

              } catch (Exception e) {
                OLogManager.instance().error(this, "Error during addition of index %s", idx);
                errors++;
              }
            }

            newDb.close();

            OLogManager.instance().info(this, "%d indexes were restored successfully, %d errors", ok, errors);
          } catch (Exception e) {
            OLogManager.instance().error(this, "Error when attempt to restore indexes after crash was performed.");
          }
        }
      };

      recreateIndexesThread = new Thread(recreateIndexesTask);
      recreateIndexesThread.start();
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public void waitTillIndexRestore() {
    if (recreateIndexesThread != null && recreateIndexesThread.isAlive()) {
      if (Thread.currentThread().equals(recreateIndexesThread))
        return;

      OLogManager.instance().info(this, "Wait till indexes restore after crash was finished.");
      while (recreateIndexesThread.isAlive())
        try {
          recreateIndexesThread.join();
          OLogManager.instance().info(this, "Indexes restore after crash was finished.");
        } catch (InterruptedException e) {
          OLogManager.instance().info(this, "Index rebuild task was interrupted.");
        }
    }
  }
}
