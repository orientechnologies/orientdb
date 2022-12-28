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
package com.orientechnologies.orient.core.index;

import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OMultiKey;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.OMetadataUpdateListener;
import com.orientechnologies.orient.core.dictionary.ODictionary;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.OMetadata;
import com.orientechnologies.orient.core.metadata.OMetadataDefault;
import com.orientechnologies.orient.core.metadata.OMetadataInternal;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sharding.auto.OAutoShardingIndexFactory;
import com.orientechnologies.orient.core.sql.OCommandExecutorSQLCreateIndex;
import com.orientechnologies.orient.core.storage.OStorageInfo;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class OIndexManagerRemote implements OIndexManagerAbstract {
  private final AtomicBoolean skipPush = new AtomicBoolean(false);
  private static final String QUERY_DROP = "drop index `%s` if exists";
  private final OStorageInfo storage;
  // values of this Map should be IMMUTABLE !! for thread safety reasons.
  protected final Map<String, Map<OMultiKey, Set<OIndex>>> classPropertyIndex =
      new ConcurrentHashMap<>();
  protected Map<String, OIndex> indexes = new ConcurrentHashMap<>();
  protected String defaultClusterName = OMetadataDefault.CLUSTER_INDEX_NAME;
  protected final AtomicInteger writeLockNesting = new AtomicInteger();
  protected final ReadWriteLock lock = new ReentrantReadWriteLock();
  protected ODocument document;

  public OIndexManagerRemote(OStorageInfo storage) {
    super();
    this.storage = storage;
    this.document = new ODocument().setTrackingChanges(false);
  }

  public void load(ODatabaseDocumentInternal database) {
    if (!autoRecreateIndexesAfterCrash(database)) {
      acquireExclusiveLock();
      try {
        if (database.getStorageInfo().getConfiguration().getIndexMgrRecordId() == null)
          // @COMPATIBILITY: CREATE THE INDEX MGR
          create(database);

        // RELOAD IT
        ((ORecordId) document.getIdentity())
            .fromString(database.getStorageInfo().getConfiguration().getIndexMgrRecordId());
        database.reload(document, "*:-1 index:0", true);
        fromStream();
      } finally {
        releaseExclusiveLock();
      }
    }
  }

  public void reload() {
    acquireExclusiveLock();
    try {
      ((ORecordId) document.getIdentity())
          .fromString(getStorage().getConfiguration().getIndexMgrRecordId());
      document.reload();
      fromStream();
    } finally {
      releaseExclusiveLock();
    }
  }

  public void save() {
    throw new UnsupportedOperationException();
  }

  public void addClusterToIndex(final String clusterName, final String indexName) {
    throw new UnsupportedOperationException();
  }

  public void removeClusterFromIndex(final String clusterName, final String indexName) {
    throw new UnsupportedOperationException();
  }

  public void create(ODatabaseDocumentInternal database) {
    throw new UnsupportedOperationException();
  }

  public Collection<? extends OIndex> getIndexes(ODatabaseDocumentInternal database) {
    final Collection<OIndex> rawResult = indexes.values();
    final List<OIndex> result = new ArrayList<>(rawResult.size());
    for (final OIndex index : rawResult) {
      result.add(index);
    }
    return result;
  }

  public OIndex getRawIndex(final String iName) {
    final OIndex index = indexes.get(iName);
    return index;
  }

  public OIndex getIndex(ODatabaseDocumentInternal database, final String iName) {
    final OIndex index = indexes.get(iName);
    if (index == null) return null;
    return index;
  }

  public boolean existsIndex(final String iName) {
    return indexes.containsKey(iName);
  }

  public String getDefaultClusterName() {
    acquireSharedLock();
    try {
      return defaultClusterName;
    } finally {
      releaseSharedLock();
    }
  }

  public void setDefaultClusterName(
      ODatabaseDocumentInternal database, final String defaultClusterName) {
    acquireExclusiveLock();
    try {
      this.defaultClusterName = defaultClusterName;
    } finally {
      releaseExclusiveLock();
    }
  }

  public ODictionary<ORecord> getDictionary(ODatabaseDocumentInternal database) {
    OIndex idx;
    acquireSharedLock();
    try {
      idx = getIndex(database, DICTIONARY_NAME);
    } finally {
      releaseSharedLock();
    }
    assert idx != null;
    return new ODictionary<>(idx);
  }

  public ODocument getConfiguration() {
    acquireSharedLock();

    try {
      return getDocument();
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public void close() {
    indexes.clear();
    classPropertyIndex.clear();
  }

  void setDirty() {
    acquireExclusiveLock();
    try {
      document.setDirty();
    } finally {
      releaseExclusiveLock();
    }
  }

  public Set<OIndex> getClassInvolvedIndexes(
      ODatabaseDocumentInternal database, final String className, Collection<String> fields) {
    final OMultiKey multiKey = new OMultiKey(fields);

    final Map<OMultiKey, Set<OIndex>> propertyIndex = getIndexOnProperty(className);

    if (propertyIndex == null || !propertyIndex.containsKey(multiKey))
      return Collections.emptySet();

    final Set<OIndex> rawResult = propertyIndex.get(multiKey);
    final Set<OIndex> transactionalResult = new HashSet<>(rawResult.size());
    for (final OIndex index : rawResult) {
      // ignore indexes that ignore null values on partial match
      if (fields.size() == index.getDefinition().getFields().size()
          || !index.getDefinition().isNullValuesIgnored()) {
        transactionalResult.add(index);
      }
    }

    return transactionalResult;
  }

  public Set<OIndex> getClassInvolvedIndexes(
      ODatabaseDocumentInternal database, final String className, final String... fields) {
    return getClassInvolvedIndexes(database, className, Arrays.asList(fields));
  }

  public boolean areIndexed(final String className, Collection<String> fields) {
    final OMultiKey multiKey = new OMultiKey(fields);

    final Map<OMultiKey, Set<OIndex>> propertyIndex = getIndexOnProperty(className);

    if (propertyIndex == null) return false;

    return propertyIndex.containsKey(multiKey) && !propertyIndex.get(multiKey).isEmpty();
  }

  public boolean areIndexed(final String className, final String... fields) {
    return areIndexed(className, Arrays.asList(fields));
  }

  public Set<OIndex> getClassIndexes(ODatabaseDocumentInternal database, final String className) {
    final HashSet<OIndex> coll = new HashSet<OIndex>(4);
    getClassIndexes(database, className, coll);
    return coll;
  }

  public void getClassIndexes(
      ODatabaseDocumentInternal database,
      final String className,
      final Collection<OIndex> indexes) {
    getClassRawIndexes(className, indexes);
  }

  public void getClassRawIndexes(final String className, final Collection<OIndex> indexes) {
    final Map<OMultiKey, Set<OIndex>> propertyIndex = getIndexOnProperty(className);

    if (propertyIndex == null) return;

    for (final Set<OIndex> propertyIndexes : propertyIndex.values())
      indexes.addAll(propertyIndexes);
  }

  public OIndexUnique getClassUniqueIndex(final String className) {
    final Map<OMultiKey, Set<OIndex>> propertyIndex = getIndexOnProperty(className);

    if (propertyIndex != null)
      for (final Set<OIndex> propertyIndexes : propertyIndex.values())
        for (final OIndex index : propertyIndexes)
          if (index instanceof OIndexUnique) return (OIndexUnique) index;

    return null;
  }

  public OIndex getClassIndex(
      ODatabaseDocumentInternal database, String className, String indexName) {
    className = className.toLowerCase();

    final OIndex index = indexes.get(indexName);
    if (index != null
        && index.getDefinition() != null
        && index.getDefinition().getClassName() != null
        && className.equals(index.getDefinition().getClassName().toLowerCase())) return index;
    return null;
  }

  public OIndex getClassAutoShardingIndex(ODatabaseDocumentInternal database, String className) {
    className = className.toLowerCase();

    // LOOK FOR INDEX
    for (OIndex index : indexes.values()) {
      if (index != null
          && OAutoShardingIndexFactory.AUTOSHARDING_ALGORITHM.equals(index.getAlgorithm())
          && index.getDefinition() != null
          && index.getDefinition().getClassName() != null
          && className.equals(index.getDefinition().getClassName().toLowerCase())) return index;
    }
    return null;
  }

  private void acquireSharedLock() {
    lock.readLock().lock();
  }

  private void releaseSharedLock() {
    lock.readLock().unlock();
  }

  void internalAcquireExclusiveLock() {
    final ODatabaseDocumentInternal databaseRecord = getDatabaseIfDefined();
    if (databaseRecord != null && !databaseRecord.isClosed()) {
      final OMetadataInternal metadata = (OMetadataInternal) databaseRecord.getMetadata();
      if (metadata != null) metadata.makeThreadLocalSchemaSnapshot();
    }
    lock.writeLock().lock();
  }

  void internalReleaseExclusiveLock() {
    lock.writeLock().unlock();

    final ODatabaseDocumentInternal databaseRecord = getDatabaseIfDefined();
    if (databaseRecord != null && !databaseRecord.isClosed()) {
      final OMetadata metadata = databaseRecord.getMetadata();
      if (metadata != null) ((OMetadataInternal) metadata).clearThreadLocalSchemaSnapshot();
    }
  }

  void clearMetadata() {
    acquireExclusiveLock();
    try {
      indexes.clear();
      classPropertyIndex.clear();
    } finally {
      releaseExclusiveLock();
    }
  }

  protected static ODatabaseDocumentInternal getDatabase() {
    return ODatabaseRecordThreadLocal.instance().get();
  }

  private static ODatabaseDocumentInternal getDatabaseIfDefined() {
    return ODatabaseRecordThreadLocal.instance().getIfDefined();
  }

  void addIndexInternal(final OIndex index) {
    acquireExclusiveLock();
    try {
      indexes.put(index.getName(), index);

      final OIndexDefinition indexDefinition = index.getDefinition();
      if (indexDefinition == null || indexDefinition.getClassName() == null) return;

      Map<OMultiKey, Set<OIndex>> propertyIndex =
          getIndexOnProperty(indexDefinition.getClassName());

      if (propertyIndex == null) {
        propertyIndex = new HashMap<>();
      } else {
        propertyIndex = new HashMap<>(propertyIndex);
      }

      final int paramCount = indexDefinition.getParamCount();

      for (int i = 1; i <= paramCount; i++) {
        final List<String> fields = indexDefinition.getFields().subList(0, i);
        final OMultiKey multiKey = new OMultiKey(fields);
        Set<OIndex> indexSet = propertyIndex.get(multiKey);

        if (indexSet == null) indexSet = new HashSet<>();
        else indexSet = new HashSet<>(indexSet);

        indexSet.add(index);
        propertyIndex.put(multiKey, indexSet);
      }

      classPropertyIndex.put(
          indexDefinition.getClassName().toLowerCase(), copyPropertyMap(propertyIndex));
    } finally {
      releaseExclusiveLock();
    }
  }

  static Map<OMultiKey, Set<OIndex>> copyPropertyMap(Map<OMultiKey, Set<OIndex>> original) {
    final Map<OMultiKey, Set<OIndex>> result = new HashMap<>();

    for (Map.Entry<OMultiKey, Set<OIndex>> entry : original.entrySet()) {
      Set<OIndex> indexes = new HashSet<>(entry.getValue());
      assert indexes.equals(entry.getValue());

      result.put(entry.getKey(), Collections.unmodifiableSet(indexes));
    }

    assert result.equals(original);

    return Collections.unmodifiableMap(result);
  }

  private Map<OMultiKey, Set<OIndex>> getIndexOnProperty(final String className) {
    acquireSharedLock();
    try {

      return classPropertyIndex.get(className.toLowerCase());

    } finally {
      releaseSharedLock();
    }
  }

  public OIndex createIndex(
      ODatabaseDocumentInternal database,
      final String iName,
      final String iType,
      final OIndexDefinition iIndexDefinition,
      final int[] iClusterIdsToIndex,
      final OProgressListener progressListener,
      ODocument metadata,
      String engine) {

    String createIndexDDL;
    if (iIndexDefinition != null)
      createIndexDDL = iIndexDefinition.toCreateIndexDDL(iName, iType, engine);
    else createIndexDDL = new OSimpleKeyIndexDefinition().toCreateIndexDDL(iName, iType, engine);

    if (metadata != null)
      createIndexDDL +=
          " " + OCommandExecutorSQLCreateIndex.KEYWORD_METADATA + " " + metadata.toJSON();

    acquireExclusiveLock();
    try {
      if (progressListener != null) progressListener.onBegin(this, 0, false);

      database.command(createIndexDDL).close();

      ORecordInternal.setIdentity(
          document,
          new ORecordId(database.getStorageInfo().getConfiguration().getIndexMgrRecordId()));

      if (progressListener != null) progressListener.onCompletition(this, true);

      reload();

      return indexes.get(iName);
    } catch (OCommandExecutionException x) {
      throw new OIndexException(x.getMessage());
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public OIndex createIndex(
      ODatabaseDocumentInternal database,
      String iName,
      String iType,
      OIndexDefinition indexDefinition,
      int[] clusterIdsToIndex,
      OProgressListener progressListener,
      ODocument metadata) {
    return createIndex(
        database,
        iName,
        iType,
        indexDefinition,
        clusterIdsToIndex,
        progressListener,
        metadata,
        null);
  }

  public void dropIndex(ODatabaseDocumentInternal database, final String iIndexName) {
    acquireExclusiveLock();
    try {
      final String text = String.format(QUERY_DROP, iIndexName);
      database.command(text).close();

      // REMOVE THE INDEX LOCALLY
      indexes.remove(iIndexName);
      reload();

    } finally {
      releaseExclusiveLock();
    }
  }

  public ODocument toStream() {
    throw new UnsupportedOperationException("Remote index cannot be streamed");
  }

  public void recreateIndexes() {
    throw new UnsupportedOperationException("recreateIndexes()");
  }

  @Override
  public void recreateIndexes(ODatabaseDocumentInternal database) {
    throw new UnsupportedOperationException("recreateIndexes(ODatabaseDocumentInternal)");
  }

  public void waitTillIndexRestore() {}

  @Override
  public boolean autoRecreateIndexesAfterCrash(ODatabaseDocumentInternal database) {
    return false;
  }

  public boolean autoRecreateIndexesAfterCrash() {
    return false;
  }

  public void removeClassPropertyIndex(OIndex idx) {}

  protected OIndex getRemoteIndexInstance(
      boolean isMultiValueIndex,
      String type,
      String name,
      String algorithm,
      Set<String> clustersToIndex,
      OIndexDefinition indexDefinition,
      ORID identity,
      ODocument configuration) {
    if (isMultiValueIndex)
      return new OIndexRemoteMultiValue(
          name,
          type,
          algorithm,
          identity,
          indexDefinition,
          configuration,
          clustersToIndex,
          getStorage().getName());

    return new OIndexRemoteOneValue(
        name,
        type,
        algorithm,
        identity,
        indexDefinition,
        configuration,
        clustersToIndex,
        getStorage().getName());
  }

  protected void fromStream() {
    acquireExclusiveLock();
    try {
      clearMetadata();

      final Collection<ODocument> idxs = document.field(CONFIG_INDEXES);
      if (idxs != null) {
        for (ODocument d : idxs) {
          d.setLazyLoad(false);
          try {
            final boolean isMultiValue =
                ODefaultIndexFactory.isMultiValueIndex(d.field(OIndexInternal.CONFIG_TYPE));

            final OIndexMetadata newIndexMetadata =
                OIndexAbstract.loadMetadataInternal(
                    d,
                    d.field(OIndexInternal.CONFIG_TYPE),
                    d.field(OIndexInternal.ALGORITHM),
                    d.field(OIndexInternal.VALUE_CONTAINER_ALGORITHM));

            addIndexInternal(
                getRemoteIndexInstance(
                    isMultiValue,
                    newIndexMetadata.getType(),
                    newIndexMetadata.getName(),
                    newIndexMetadata.getAlgorithm(),
                    newIndexMetadata.getClustersToIndex(),
                    newIndexMetadata.getIndexDefinition(),
                    d.field(OIndexAbstract.CONFIG_MAP_RID),
                    d));
          } catch (Exception e) {
            OLogManager.instance()
                .error(this, "Error on loading of index by configuration: %s", e, d);
          }
        }
      }
    } finally {
      releaseExclusiveLock();
    }
  }

  protected void acquireExclusiveLock() {
    skipPush.set(true);
  }

  protected void releaseExclusiveLock() {
    skipPush.set(false);
  }

  protected void realAcquireExclusiveLock() {
    internalAcquireExclusiveLock();
    writeLockNesting.incrementAndGet();
  }

  protected void realReleaseExclusiveLock() {
    int val = writeLockNesting.decrementAndGet();
    ODatabaseDocumentInternal database = getDatabaseIfDefined();
    if (database != null) {
      database
          .getSharedContext()
          .getSchema()
          .forceSnapshot(ODatabaseRecordThreadLocal.instance().get());
    }
    internalReleaseExclusiveLock();
    if (val == 0 && database != null) {
      for (OMetadataUpdateListener listener : database.getSharedContext().browseListeners()) {
        listener.onIndexManagerUpdate(database.getName(), this);
      }
    }
  }

  public void update(ODocument indexManager) {
    if (!skipPush.get()) {
      realAcquireExclusiveLock();
      try {
        this.document = indexManager;
        fromStream();
      } finally {
        realReleaseExclusiveLock();
      }
    }
  }

  protected OStorageInfo getStorage() {
    return storage;
  }

  public ODocument getDocument() {
    return document;
  }

  public OIndex preProcessBeforeReturn(ODatabaseDocumentInternal database, OIndex index) {
    return index;
  }
}
