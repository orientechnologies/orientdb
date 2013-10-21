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
import java.util.*;

import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OMultiKey;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.db.record.ORecordTrackedSet;
import com.orientechnologies.orient.core.engine.local.OEngineLocal;
import com.orientechnologies.orient.core.engine.local.OEngineLocalPaginated;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.OMetadataDefault;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchemaShared;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.OCluster.ATTRIBUTES;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.OClusterLocal;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocal;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OLocalPaginatedStorage;

/**
 * Manages indexes at database level. A single instance is shared among multiple databases. Contentions are managed by r/w locks.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * @author Artem Orobets added composite index managemement
 * 
 */
public class OIndexManagerShared extends OIndexManagerAbstract implements OIndexManager {
  private static final boolean useSBTree             = OGlobalConfiguration.INDEX_USE_SBTREE_BY_DEFAULT.getValueAsBoolean();

  private static final long    serialVersionUID      = 1L;

  protected volatile Thread    recreateIndexesThread = null;
  private volatile boolean     rebuildCompleted      = false;

  public OIndexManagerShared(final ODatabaseRecord iDatabase) {
    super(iDatabase);
  }

  public OIndex<?> getIndexInternal(final String name) {
    acquireSharedLock();
    try {
      return indexes.get(name.toLowerCase());
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
   * @param clusterIdsToIndex
   * @param iProgressListener
   */
  public OIndex<?> createIndex(final String iName, final String iType, final OIndexDefinition indexDefinition,
      final int[] clusterIdsToIndex, OProgressListener iProgressListener) {
    if (getDatabase().getTransaction().isActive())
      throw new IllegalStateException("Cannot create a new index inside a transaction");

    final Character c = OSchemaShared.checkNameIfValid(iName);
    if (c != null)
      throw new IllegalArgumentException("Invalid index name '" + iName + "'. Character '" + c + "' is invalid");

    ODatabase database = getDatabase();
    OStorage storage = database.getStorage();
    final String alghorithm;
    if ((storage.getType().equals(OEngineLocal.NAME) || storage.getType().equals(OEngineLocalPaginated.NAME)) && useSBTree)
      alghorithm = ODefaultIndexFactory.SBTREE_ALGORITHM;
    else
      alghorithm = ODefaultIndexFactory.MVRBTREE_ALGORITHM;

    final String valueContainerAlgorithm;
    if (OClass.INDEX_TYPE.NOTUNIQUE.toString().equals(iType) || OClass.INDEX_TYPE.NOTUNIQUE_HASH_INDEX.toString().equals(iType)
        || OClass.INDEX_TYPE.FULLTEXT_HASH_INDEX.toString().equals(iType) || OClass.INDEX_TYPE.FULLTEXT.toString().equals(iType)) {
      if ((storage.getType().equals(OEngineLocalPaginated.NAME) || storage.getType().equals(OEngineLocal.NAME))
          && OGlobalConfiguration.INDEX_NOTUNIQUE_USE_SBTREE_CONTAINER_BY_DEFAULT.getValueAsBoolean()) {
        valueContainerAlgorithm = ODefaultIndexFactory.SBTREEBONSAI_VALUE_CONTAINER;
      } else {
        valueContainerAlgorithm = ODefaultIndexFactory.MVRBTREE_VALUE_CONTAINER;
      }
    } else {
      valueContainerAlgorithm = ODefaultIndexFactory.NONE_VALUE_CONTAINER;
    }

    acquireExclusiveLock();
    try {
      final OIndexInternal<?> index = OIndexes.createIndex(getDatabase(), iType, alghorithm, valueContainerAlgorithm);

      // decide which cluster to use ("index" - for automatic and "manindex" for manual)
      final String clusterName = indexDefinition != null && indexDefinition.getClassName() != null ? defaultClusterName
          : manualClusterName;

      if (iProgressListener == null)
        // ASSIGN DEFAULT PROGRESS LISTENER
        iProgressListener = new OIndexRebuildOutputListener(index);

      Set<String> clustersToIndex = new HashSet<String>();
      if (clusterIdsToIndex != null) {
        for (int clusterId : clusterIdsToIndex) {
          final String clusterNameToIndex = database.getClusterNameById(clusterId);
          if (clusterNameToIndex == null)
            throw new OIndexException("Cluster with id " + clusterId + " does not exist.");

          clustersToIndex.add(clusterNameToIndex);
        }
      }

      index.create(iName, indexDefinition, clusterName, clustersToIndex, true, iProgressListener);
      addIndexInternal(index);

      setDirty();
      save();

      return index;
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

        getDatabase().unregisterListener(idx.getInternal());
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
      final Map<String, OIndex<?>> oldIndexes = new HashMap<String, OIndex<?>>(indexes);

      clearMetadata();
      final Collection<ODocument> idxs = document.field(CONFIG_INDEXES);

      if (idxs != null) {
        OIndexInternal<?> index;
        boolean configUpdated = false;
        Iterator<ODocument> indexConfigurationIterator = idxs.iterator();
        while (indexConfigurationIterator.hasNext()) {
          final ODocument d = indexConfigurationIterator.next();
          try {
            index = OIndexes.createIndex(getDatabase(), (String) d.field(OIndexInternal.CONFIG_TYPE),
                (String) d.field(OIndexInternal.ALGORITHM), d.<String> field(OIndexInternal.VALUE_CONTAINER_ALGORITHM));

            OIndexInternal.IndexMetadata newIndexMetadata = index.loadMetadata(d);
            final String normalizedName = newIndexMetadata.getName().toLowerCase();

            OIndex<?> oldIndex = oldIndexes.get(normalizedName);
            if (oldIndex != null) {
              OIndexInternal.IndexMetadata oldIndexMetadata = oldIndex.getInternal().loadMetadata(oldIndex.getConfiguration());
              if (oldIndexMetadata.equals(newIndexMetadata)) {
                addIndexInternal(oldIndex.getInternal());
                oldIndexes.remove(normalizedName);
              } else if (newIndexMetadata.getIndexDefinition() == null
                  && d.field(OIndexAbstract.CONFIG_MAP_RID)
                      .equals(oldIndex.getConfiguration().field(OIndexAbstract.CONFIG_MAP_RID))) {
                // index is manual and index definition was just detected
                addIndexInternal(oldIndex.getInternal());
                oldIndexes.remove(normalizedName);
              }
            } else {
              if (((OIndexInternal<?>) index).loadFromConfiguration(d)) {
                addIndexInternal(index);
              } else {
                indexConfigurationIterator.remove();
                configUpdated = true;
              }
            }
          } catch (Exception e) {
            indexConfigurationIterator.remove();
            configUpdated = true;
            OLogManager.instance().error(this, "Error on loading index by configuration: %s", e, d);
          }
        }

        for (OIndex<?> oldIndex : oldIndexes.values())
          try {
            OLogManager.instance().warn(this, "Index %s was not found after reload and will be removed", oldIndex.getName());

            getDatabase().unregisterListener(oldIndex.getInternal());
            oldIndex.delete();
          } catch (Exception e) {
            OLogManager.instance().error(this, "Error on deletion of index %s", e, oldIndex.getName());
          }

        if (configUpdated) {
          document.field(CONFIG_INDEXES, idxs);
          save();
        }

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
              final int dataId = newDb.getStorage().getDataSegmentIdByName(OMetadataDefault.DATASEGMENT_INDEX_NAME);
              if (dataId > -1)
                newDb.getStorage().dropDataSegment(OMetadataDefault.DATASEGMENT_INDEX_NAME);

              final int clusterId = newDb.getStorage().getClusterIdByName(OMetadataDefault.CLUSTER_INDEX_NAME);
              if (clusterId > -1)
                newDb.dropCluster(clusterId, false);

              newDb.addDataSegment(OMetadataDefault.DATASEGMENT_INDEX_NAME, null);
              newDb.getStorage().addCluster(OClusterLocal.TYPE, OMetadataDefault.CLUSTER_INDEX_NAME, null,
                  OMetadataDefault.DATASEGMENT_INDEX_NAME, true);

            } catch (IllegalArgumentException ex) {
              // OLD DATABASE: CREATE SEPARATE DATASEGMENT AND LET THE INDEX CLUSTER TO POINT TO IT
              OLogManager.instance().info(this, "Creating 'index' data-segment to store all the index content...");

              newDb.addDataSegment(OMetadataDefault.DATASEGMENT_INDEX_NAME, null);
              final OCluster indexCluster = newDb.getStorage().getClusterById(
                  newDb.getStorage().getClusterIdByName(OMetadataDefault.CLUSTER_INDEX_NAME));
              try {
                indexCluster.set(ATTRIBUTES.DATASEGMENT, OMetadataDefault.DATASEGMENT_INDEX_NAME);
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
                String algorithm = idx.field(OIndexInternal.ALGORITHM);
                String valueContainerAlgorithm = idx.field(OIndexInternal.VALUE_CONTAINER_ALGORITHM);

                if (indexType == null) {
                  OLogManager.instance().error(this, "Index type is null, will process other record.");
                  errors++;
                  continue;
                }

                final OIndexInternal<?> index = OIndexes.createIndex(newDb, indexType, algorithm, valueContainerAlgorithm);
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
                    getDatabase().unregisterListener(index.getInternal());
                    index.delete();
                    errors++;
                  }

                  OLogManager.instance().info(this, "Index %s was added in DB index list.", index.getName());
                } else {
                  String indexName = indexMetadata.getName();
                  Set<String> clusters = indexMetadata.getClustersToIndex();
                  String type = indexMetadata.getType();

                  if (indexName != null && indexDefinition != null && clusters != null && !clusters.isEmpty() && type != null) {
                    OLogManager.instance().info(this, "Start creation of index %s", indexName);

                    index.create(indexName, indexDefinition, defaultClusterName, clusters, false, new OIndexRebuildOutputListener(
                        index));

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

            rebuildCompleted = true;

            newDb.close();

            OLogManager.instance().info(this, "%d indexes were restored successfully, %d errors", ok, errors);
          } catch (Exception e) {
            OLogManager.instance().error(this, "Error when attempt to restore indexes after crash was performed.", e);
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

  public boolean autoRecreateIndexesAfterCrash() {
    if (rebuildCompleted)
      return false;

    final ODatabaseRecord database = ODatabaseRecordThreadLocal.INSTANCE.get();
    if (!OGlobalConfiguration.INDEX_AUTO_REBUILD_AFTER_NOTSOFTCLOSE.getValueAsBoolean())
      return false;

    OStorage storage = database.getStorage();

    if (storage instanceof OStorageLocal)
      return !((OStorageLocal) storage).wasClusterSoftlyClosed(OMetadataDefault.CLUSTER_INDEX_NAME);
    else if (storage instanceof OLocalPaginatedStorage) {
      return ((OLocalPaginatedStorage) storage).wereDataRestoredAfterOpen();
    }

    return false;
  }
}
