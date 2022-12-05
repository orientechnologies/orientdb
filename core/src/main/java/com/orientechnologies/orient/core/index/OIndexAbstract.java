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

import com.orientechnologies.common.concur.lock.OOneEntryPerKeyLockManager;
import com.orientechnologies.common.concur.lock.OPartitionedLockManager;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.exception.OInvalidIndexEngineIdException;
import com.orientechnologies.orient.core.exception.OManualIndexesAreProhibited;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.engine.OBaseIndexEngine;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.cache.OReadCache;
import com.orientechnologies.orient.core.storage.cache.OWriteCache;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OIndexRIDContainer;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges.OPERATION;
import com.orientechnologies.orient.core.tx.OTransactionIndexChangesPerKey;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

/**
 * Handles indexing when records change. The underlying lock manager for keys can be the {@link
 * OPartitionedLockManager}, the default one, or the {@link OOneEntryPerKeyLockManager} in case of
 * distributed. This is to avoid deadlock situation between nodes where keys have the same hash
 * code.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public abstract class OIndexAbstract implements OIndexInternal {

  private static final OAlwaysLessKey ALWAYS_LESS_KEY = new OAlwaysLessKey();
  private static final OAlwaysGreaterKey ALWAYS_GREATER_KEY = new OAlwaysGreaterKey();
  protected static final String CONFIG_MAP_RID = "mapRid";
  private static final String CONFIG_CLUSTERS = "clusters";
  protected final String type;
  protected final ODocument metadata;
  protected final OAbstractPaginatedStorage storage;
  private final String databaseName;
  private final String name;
  private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
  private final AtomicLong rebuildVersion = new AtomicLong();
  private final int version;
  protected volatile IndexConfiguration configuration;
  protected volatile String valueContainerAlgorithm;

  protected volatile int indexId = -1;
  protected volatile int apiVersion = -1;

  protected Set<String> clustersToIndex = new HashSet<>();
  private String algorithm;
  private volatile OIndexDefinition indexDefinition;
  private final Map<String, String> engineProperties = new HashMap<>();
  protected final int binaryFormatVersion;

  public OIndexAbstract(
      String name,
      final String type,
      final String algorithm,
      final String valueContainerAlgorithm,
      final ODocument metadata,
      final int version,
      final OStorage storage,
      int binaryFormatVersion) {
    this.binaryFormatVersion = binaryFormatVersion;
    acquireExclusiveLock();
    try {
      databaseName = storage.getName();

      this.version = version;
      this.name = name;
      this.type = type;
      this.algorithm = algorithm;
      this.metadata = metadata;
      this.valueContainerAlgorithm = valueContainerAlgorithm;
      this.storage = (OAbstractPaginatedStorage) storage;
    } finally {
      releaseExclusiveLock();
    }
  }

  public static OIndexMetadata loadMetadataInternal(
      final ODocument config,
      final String type,
      final String algorithm,
      final String valueContainerAlgorithm) {
    final String indexName = config.field(OIndexInternal.CONFIG_NAME);

    final ODocument indexDefinitionDoc = config.field(OIndexInternal.INDEX_DEFINITION);
    OIndexDefinition loadedIndexDefinition = null;
    if (indexDefinitionDoc != null) {
      try {
        final String indexDefClassName = config.field(OIndexInternal.INDEX_DEFINITION_CLASS);
        final Class<?> indexDefClass = Class.forName(indexDefClassName);
        loadedIndexDefinition =
            (OIndexDefinition) indexDefClass.getDeclaredConstructor().newInstance();
        loadedIndexDefinition.fromStream(indexDefinitionDoc);

      } catch (final ClassNotFoundException
          | IllegalAccessException
          | InstantiationException
          | InvocationTargetException
          | NoSuchMethodException e) {
        throw OException.wrapException(
            new OIndexException("Error during deserialization of index definition"), e);
      }
    } else {
      // @COMPATIBILITY 1.0rc6 new index model was implemented
      final Boolean isAutomatic = config.field(OIndexInternal.CONFIG_AUTOMATIC);
      OIndexFactory factory = OIndexes.getFactory(type, algorithm);
      if (Boolean.TRUE.equals(isAutomatic)) {
        final int pos = indexName.lastIndexOf('.');
        if (pos < 0)
          throw new OIndexException(
              "Cannot convert from old index model to new one. "
                  + "Invalid index name. Dot (.) separator should be present");
        final String className = indexName.substring(0, pos);
        final String propertyName = indexName.substring(pos + 1);

        final String keyTypeStr = config.field(OIndexInternal.CONFIG_KEYTYPE);
        if (keyTypeStr == null)
          throw new OIndexException(
              "Cannot convert from old index model to new one. " + "Index key type is absent");
        final OType keyType = OType.valueOf(keyTypeStr.toUpperCase(Locale.ENGLISH));

        loadedIndexDefinition = new OPropertyIndexDefinition(className, propertyName, keyType);

        config.removeField(OIndexInternal.CONFIG_AUTOMATIC);
        config.removeField(OIndexInternal.CONFIG_KEYTYPE);
      } else if (config.field(OIndexInternal.CONFIG_KEYTYPE) != null) {
        final String keyTypeStr = config.field(OIndexInternal.CONFIG_KEYTYPE);
        final OType keyType = OType.valueOf(keyTypeStr.toUpperCase(Locale.ENGLISH));

        loadedIndexDefinition = new OSimpleKeyIndexDefinition(keyType);

        config.removeField(OIndexInternal.CONFIG_KEYTYPE);
      }
    }

    final Set<String> clusters = new HashSet<>(config.field(CONFIG_CLUSTERS, OType.EMBEDDEDSET));

    return new OIndexMetadata(
        indexName,
        loadedIndexDefinition,
        clusters,
        type,
        algorithm,
        valueContainerAlgorithm,
        config.field(OIndexInternal.METADATA));
  }

  @Override
  public boolean hasRangeQuerySupport() {

    acquireSharedLock();
    try {
      while (true)
        try {
          return storage.hasIndexRangeQuerySupport(indexId);
        } catch (OInvalidIndexEngineIdException ignore) {
          doReloadIndexEngine();
        }
    } finally {
      releaseSharedLock();
    }
  }

  /** Creates the index. */
  public OIndexInternal create(
      final OIndexMetadata indexMetadata,
      boolean rebuild,
      final OProgressListener progressListener) {
    final OBinarySerializer valueSerializer = determineValueSerializer();
    acquireExclusiveLock();
    try {
      configuration = indexConfigurationInstance(new ODocument().setTrackingChanges(false));
      Set<String> clustersToIndex = indexMetadata.getClustersToIndex();
      this.indexDefinition = indexMetadata.getIndexDefinition();

      if (clustersToIndex != null) this.clustersToIndex = new HashSet<>(clustersToIndex);
      else this.clustersToIndex = new HashSet<>();

      // do not remove this, it is needed to remove index garbage if such one exists
      try {
        if (apiVersion == 0) {
          removeValuesContainer();
        }
      } catch (Exception e) {
        OLogManager.instance().error(this, "Error during deletion of index '%s'", e, name);
      }
      // this property is used for autosharded index
      if (metadata != null && metadata.containsField("partitions")) {
        engineProperties.put("partitions", metadata.field("partitions"));
      } else {
        engineProperties.put("partitions", Integer.toString(clustersToIndex.size()));
      }

      indexId =
          storage.addIndexEngine(
              name,
              algorithm,
              type,
              indexDefinition,
              valueSerializer,
              isAutomatic(),
              version,
              this instanceof OIndexMultiValues,
              engineProperties,
              metadata);
      apiVersion = OAbstractPaginatedStorage.extractEngineAPIVersion(indexId);

      assert indexId >= 0;
      assert apiVersion >= 0;

      onIndexEngineChange(indexId);

      if (rebuild) fillIndex(progressListener, false);

      updateConfiguration();
    } catch (Exception e) {
      OLogManager.instance().error(this, "Exception during index '%s' creation", e, name);
      // index is created inside of storage
      if (indexId >= 0) {
        doDelete();
      }
      throw OException.wrapException(
          new OIndexException("Cannot create the index '" + name + "'"), e);
    } finally {
      releaseExclusiveLock();
    }

    return this;
  }

  protected void doReloadIndexEngine() {
    indexId = storage.loadIndexEngine(name);
    apiVersion = OAbstractPaginatedStorage.extractEngineAPIVersion(indexId);

    if (indexId < 0) {
      throw new IllegalStateException("Index " + name + " can not be loaded");
    }
  }

  public boolean loadFromConfiguration(final ODocument config) {
    acquireExclusiveLock();
    try {
      configuration = indexConfigurationInstance(config);
      clustersToIndex.clear();

      final OIndexMetadata indexMetadata = loadMetadata(config);
      indexDefinition = indexMetadata.getIndexDefinition();
      clustersToIndex.addAll(indexMetadata.getClustersToIndex());
      algorithm = indexMetadata.getAlgorithm();
      valueContainerAlgorithm = indexMetadata.getValueContainerAlgorithm();

      try {
        indexId = storage.loadIndexEngine(name);
        apiVersion = OAbstractPaginatedStorage.extractEngineAPIVersion(indexId);

        if (indexId == -1) {
          indexId =
              storage.loadExternalIndexEngine(
                  name,
                  algorithm,
                  type,
                  indexDefinition,
                  determineValueSerializer(),
                  isAutomatic(),
                  version,
                  1,
                  this instanceof OIndexMultiValues,
                  engineProperties);
          apiVersion = OAbstractPaginatedStorage.extractEngineAPIVersion(indexId);
        }

        if (indexId == -1) {
          return false;
        }

        onIndexEngineChange(indexId);

      } catch (Exception e) {
        OLogManager.instance()
            .error(
                this,
                "Error during load of index '%s'",
                e,
                Optional.ofNullable(name).orElse("null"));

        if (isAutomatic()) {
          // AUTOMATIC REBUILD IT
          OLogManager.instance().warn(this, "Cannot load index '%s' rebuilt it from scratch", name);
          try {
            rebuild();
          } catch (Exception t) {
            OLogManager.instance()
                .error(
                    this,
                    "Cannot rebuild index '%s' because '"
                        + t
                        + "'. The index will be removed in configuration",
                    e,
                    name);
            // REMOVE IT
            return false;
          }
        }
      }

      return true;
    } finally {
      releaseExclusiveLock();
    }
  }

  private Map<String, String> getEngineProperties() {
    return engineProperties;
  }

  @Override
  public OIndexMetadata loadMetadata(final ODocument config) {
    return loadMetadataInternal(config, type, algorithm, valueContainerAlgorithm);
  }

  /** {@inheritDoc} */
  public long rebuild() {
    return rebuild(new OIndexRebuildOutputListener(this));
  }

  @Override
  public void close() {}

  /** @return number of entries in the index. */
  @Deprecated
  public long getSize() {
    return size();
  }

  /** Counts the entries for the key. */
  @Deprecated
  public long count(Object iKey) {
    try (Stream<ORawPair<Object, ORID>> stream =
        streamEntriesBetween(iKey, true, iKey, true, true)) {
      return stream.count();
    }
  }

  /** @return Number of keys in index */
  @Deprecated
  public long getKeySize() {
    try (Stream<Object> stream = keyStream()) {
      return stream.distinct().count();
    }
  }

  /** Flushes in-memory changes to disk. */
  @Deprecated
  public void flush() {
    // do nothing
  }

  @Deprecated
  public long getRebuildVersion() {
    return 0;
  }

  /**
   * @return Indicates whether index is rebuilding at the moment.
   * @see #getRebuildVersion()
   */
  @Deprecated
  public boolean isRebuilding() {
    return false;
  }

  @Deprecated
  public Object getFirstKey() {
    try (final Stream<Object> stream = keyStream()) {
      final Iterator<Object> iterator = stream.iterator();
      if (iterator.hasNext()) {
        return iterator.next();
      }

      return null;
    }
  }

  @Deprecated
  public Object getLastKey() {
    try (final Stream<ORawPair<Object, ORID>> stream = descStream()) {
      final Iterator<ORawPair<Object, ORID>> iterator = stream.iterator();
      if (iterator.hasNext()) {
        return iterator.next().first;
      }

      return null;
    }
  }

  @Deprecated
  public OIndexCursor cursor() {
    return new StreamWrapper(stream());
  }

  @Deprecated
  @Override
  public OIndexCursor descCursor() {
    return new StreamWrapper(descStream());
  }

  @Deprecated
  @Override
  public OIndexKeyCursor keyCursor() {
    return new OIndexKeyCursor() {
      private final Iterator<Object> keyIterator = keyStream().iterator();

      @Override
      public Object next(int prefetchSize) {
        if (keyIterator.hasNext()) {
          return keyIterator.next();
        }

        return null;
      }
    };
  }

  @Deprecated
  @Override
  public OIndexCursor iterateEntries(Collection<?> keys, boolean ascSortOrder) {
    return new StreamWrapper(streamEntries(keys, ascSortOrder));
  }

  @Deprecated
  @Override
  public OIndexCursor iterateEntriesBetween(
      Object fromKey, boolean fromInclusive, Object toKey, boolean toInclusive, boolean ascOrder) {
    return new StreamWrapper(
        streamEntriesBetween(fromKey, fromInclusive, toKey, toInclusive, ascOrder));
  }

  @Deprecated
  @Override
  public OIndexCursor iterateEntriesMajor(Object fromKey, boolean fromInclusive, boolean ascOrder) {
    return new StreamWrapper(streamEntriesMajor(fromKey, fromInclusive, ascOrder));
  }

  @Deprecated
  @Override
  public OIndexCursor iterateEntriesMinor(Object toKey, boolean toInclusive, boolean ascOrder) {
    return new StreamWrapper(streamEntriesMajor(toKey, toInclusive, ascOrder));
  }

  /** {@inheritDoc} */
  public long rebuild(final OProgressListener iProgressListener) {
    long documentIndexed;

    final boolean intentInstalled = getDatabase().declareIntent(new OIntentMassiveInsert());

    acquireExclusiveLock();
    try {
      try {
        if (indexId >= 0) {
          doDelete();
        }
      } catch (Exception e) {
        OLogManager.instance().error(this, "Error during index '%s' delete", e, name);
      }

      indexId =
          storage.addIndexEngine(
              name,
              algorithm,
              type,
              indexDefinition,
              determineValueSerializer(),
              isAutomatic(),
              version,
              this instanceof OIndexMultiValues,
              engineProperties,
              metadata);
      apiVersion = OAbstractPaginatedStorage.extractEngineAPIVersion(indexId);

      onIndexEngineChange(indexId);
    } catch (Exception e) {
      try {
        if (indexId >= 0) storage.clearIndex(indexId);
      } catch (Exception e2) {
        OLogManager.instance().error(this, "Error during index rebuild", e2);
        // IGNORE EXCEPTION: IF THE REBUILD WAS LAUNCHED IN CASE OF RID INVALID CLEAR ALWAYS GOES IN
        // ERROR
      }

      throw OException.wrapException(
          new OIndexException("Error on rebuilding the index for clusters: " + clustersToIndex), e);
    } finally {
      releaseExclusiveLock();
    }

    acquireSharedLock();
    try {
      documentIndexed = fillIndex(iProgressListener, true);
    } catch (final Exception e) {
      OLogManager.instance().error(this, "Error during index rebuild", e);
      try {
        if (indexId >= 0) storage.clearIndex(indexId);
      } catch (Exception e2) {
        OLogManager.instance().error(this, "Error during index rebuild", e2);
        // IGNORE EXCEPTION: IF THE REBUILD WAS LAUNCHED IN CASE OF RID INVALID CLEAR ALWAYS GOES IN
        // ERROR
      }

      throw OException.wrapException(
          new OIndexException("Error on rebuilding the index for clusters: " + clustersToIndex), e);
    } finally {
      if (intentInstalled) getDatabase().declareIntent(null);

      releaseSharedLock();
    }

    return documentIndexed;
  }

  private long fillIndex(final OProgressListener iProgressListener, final boolean rebuild) {
    long documentIndexed = 0;
    try {
      long documentNum = 0;
      long documentTotal = 0;

      for (final String cluster : clustersToIndex)
        documentTotal += storage.count(storage.getClusterIdByName(cluster));

      if (iProgressListener != null) iProgressListener.onBegin(this, documentTotal, rebuild);

      // INDEX ALL CLUSTERS
      for (final String clusterName : clustersToIndex) {
        final long[] metrics =
            indexCluster(
                clusterName, iProgressListener, documentNum, documentIndexed, documentTotal);
        documentNum = metrics[0];
        documentIndexed = metrics[1];
      }

      if (iProgressListener != null) iProgressListener.onCompletition(this, true);
    } catch (final RuntimeException e) {
      if (iProgressListener != null) iProgressListener.onCompletition(this, false);
      throw e;
    }
    return documentIndexed;
  }

  @Override
  public boolean doRemove(OAbstractPaginatedStorage storage, Object key, ORID rid)
      throws OInvalidIndexEngineIdException {
    return doRemove(storage, key);
  }

  public boolean remove(Object key, final OIdentifiable rid) {
    key = getCollatingValue(key);

    ODatabaseDocumentInternal database = getDatabase();
    if (database.getTransaction().isActive()) {
      database.getTransaction().addIndexEntry(this, getName(), OPERATION.REMOVE, key, rid);
    } else {
      database.begin();
      database.getTransaction().addIndexEntry(this, getName(), OPERATION.REMOVE, key, rid);
      database.commit();
    }
    return true;
  }

  public boolean remove(Object key) {
    key = getCollatingValue(key);

    ODatabaseDocumentInternal database = getDatabase();
    if (database.getTransaction().isActive()) {
      database.getTransaction().addIndexEntry(this, getName(), OPERATION.REMOVE, key, null);
    } else {
      database.begin();
      database.getTransaction().addIndexEntry(this, getName(), OPERATION.REMOVE, key, null);
      database.commit();
    }
    return true;
  }

  @Override
  public boolean doRemove(OAbstractPaginatedStorage storage, Object key)
      throws OInvalidIndexEngineIdException {
    return storage.removeKeyFromIndex(indexId, key);
  }

  /**
   * {@inheritDoc}
   *
   * @deprecated Manual indexes are deprecated and will be removed
   */
  @Override
  @Deprecated
  public OIndex clear() {
    ODatabaseDocumentInternal database = getDatabase();
    if (database.getTransaction().isActive()) {
      database.getTransaction().addIndexEntry(this, this.getName(), OPERATION.CLEAR, null, null);
    } else {
      database.begin();
      database.getTransaction().addIndexEntry(this, this.getName(), OPERATION.CLEAR, null, null);
      database.commit();
    }
    return this;
  }

  public OIndexInternal delete() {
    acquireExclusiveLock();

    try {
      doDelete();
      // REMOVE THE INDEX ALSO FROM CLASS MAP
      if (getDatabase().getMetadata() != null)
        getDatabase().getMetadata().getIndexManagerInternal().removeClassPropertyIndex(this);

      return this;
    } finally {
      releaseExclusiveLock();
    }
  }

  protected void doDelete() {
    while (true)
      try {
        //noinspection ObjectAllocationInLoop
        try {
          try (final Stream<ORawPair<Object, ORID>> stream = stream()) {
            stream.forEach((pair) -> remove(pair.first, pair.second));
          }
        } catch (OIndexEngineException e) {
          throw e;
        } catch (RuntimeException e) {
          OLogManager.instance().error(this, "Error Dropping Index %s", e, getName());
          // Just log errors of removing keys while dropping and keep dropping
        }

        try {
          try (Stream<ORID> stream = getRids(null)) {
            stream.forEach((rid) -> remove(null, rid));
          }
        } catch (OIndexEngineException e) {
          throw e;
        } catch (RuntimeException e) {
          OLogManager.instance().error(this, "Error Dropping Index %s", e, getName());
          // Just log errors of removing keys while dropping and keep dropping
        }

        storage.deleteIndexEngine(indexId);
        break;
      } catch (OInvalidIndexEngineIdException ignore) {
        doReloadIndexEngine();
      }

    removeValuesContainer();
  }

  public String getName() {
    return name;
  }

  public String getType() {
    return type;
  }

  @Override
  public void setType(OType type) {
    indexDefinition = new OSimpleKeyIndexDefinition(type);
    updateConfiguration();
  }

  @Override
  public String getAlgorithm() {
    acquireSharedLock();
    try {
      return algorithm;
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public String toString() {
    acquireSharedLock();
    try {
      return name;
    } finally {
      releaseSharedLock();
    }
  }

  public OIndexInternal getInternal() {
    return this;
  }

  public Set<String> getClusters() {
    acquireSharedLock();
    try {
      return Collections.unmodifiableSet(clustersToIndex);
    } finally {
      releaseSharedLock();
    }
  }

  public OIndexAbstract addCluster(final String clusterName) {
    acquireExclusiveLock();
    try {
      if (clustersToIndex.add(clusterName)) {
        updateConfiguration();

        // INDEX SINGLE CLUSTER
        indexCluster(clusterName, null, 0, 0, 0);
      }

      return this;
    } finally {
      releaseExclusiveLock();
    }
  }

  public OIndexAbstract removeCluster(String iClusterName) {
    acquireExclusiveLock();
    try {
      if (clustersToIndex.remove(iClusterName)) {
        updateConfiguration();
        rebuild();
      }

      return this;
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public int getVersion() {
    return version;
  }

  public ODocument updateConfiguration() {
    configuration.updateConfiguration(
        type, name, version, indexDefinition, clustersToIndex, algorithm, valueContainerAlgorithm);
    if (metadata != null)
      configuration.document.field(OIndexInternal.METADATA, metadata, OType.EMBEDDED);
    return configuration.getDocument();
  }

  public void addTxOperation(IndexTxSnapshot snapshots, final OTransactionIndexChanges changes) {
    acquireSharedLock();
    try {
      if (changes.cleared) clearSnapshot(snapshots);
      final Map<Object, Object> snapshot = snapshots.indexSnapshot;
      for (final OTransactionIndexChangesPerKey entry : changes.changesPerKey.values()) {
        applyIndexTxEntry(snapshot, entry);
      }
      applyIndexTxEntry(snapshot, changes.nullKeyChanges);

    } finally {
      releaseSharedLock();
    }
  }

  /**
   * Interprets transaction index changes for a certain key. Override it to customize index
   * behaviour on interpreting index changes. This may be viewed as an optimization, but in some
   * cases this is a requirement. For example, if you put multiple values under the same key during
   * the transaction for single-valued/unique index, but remove all of them except one before
   * commit, there is no point in throwing {@link
   * com.orientechnologies.orient.core.storage.ORecordDuplicatedException} while applying index
   * changes.
   *
   * @param changes the changes to interpret.
   * @return the interpreted index key changes.
   */
  public Iterable<OTransactionIndexChangesPerKey.OTransactionIndexEntry> interpretTxKeyChanges(
      OTransactionIndexChangesPerKey changes) {
    return changes.getEntriesAsList();
  }

  private void applyIndexTxEntry(
      Map<Object, Object> snapshot, OTransactionIndexChangesPerKey entry) {
    for (OTransactionIndexChangesPerKey.OTransactionIndexEntry op : interpretTxKeyChanges(entry)) {
      switch (op.getOperation()) {
        case PUT:
          putInSnapshot(getCollatingValue(entry.key), op.getValue(), snapshot);
          break;
        case REMOVE:
          if (op.getValue() != null)
            removeFromSnapshot(getCollatingValue(entry.key), op.getValue(), snapshot);
          else removeFromSnapshot(getCollatingValue(entry.key), snapshot);
          break;
        case CLEAR:
          // SHOULD NEVER BE THE CASE HANDLE BY cleared FLAG
          break;
      }
    }
  }

  public void commit(IndexTxSnapshot snapshots) {
    acquireSharedLock();
    try {
      if (snapshots.clear) clear();

      commitSnapshot(snapshots.indexSnapshot);
    } finally {
      releaseSharedLock();
    }
  }

  public void preCommit(IndexTxSnapshot snapshots) {}

  public void postCommit(IndexTxSnapshot snapshots) {}

  public ODocument getConfiguration() {
    return configuration.getDocument();
  }

  @Override
  public ODocument getMetadata() {
    return metadata;
  }

  @Override
  public boolean isUnique() {
    return false;
  }

  public boolean isAutomatic() {
    acquireSharedLock();
    try {
      return indexDefinition != null && indexDefinition.getClassName() != null;
    } finally {
      releaseSharedLock();
    }
  }

  public OType[] getKeyTypes() {
    acquireSharedLock();
    try {
      if (indexDefinition == null) return null;

      return indexDefinition.getTypes();
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public Stream<Object> keyStream() {
    acquireSharedLock();
    try {
      while (true)
        try {
          return storage.getIndexKeyStream(indexId);
        } catch (OInvalidIndexEngineIdException ignore) {
          doReloadIndexEngine();
        }
    } finally {
      releaseSharedLock();
    }
  }

  public OIndexDefinition getDefinition() {
    return indexDefinition;
  }

  @Override
  public boolean equals(final Object o) {
    acquireSharedLock();
    try {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      final OIndexAbstract that = (OIndexAbstract) o;

      return name.equals(that.name);
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public int hashCode() {
    acquireSharedLock();
    try {
      return name.hashCode();
    } finally {
      releaseSharedLock();
    }
  }

  public int getIndexId() {
    return indexId;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  protected abstract OBinarySerializer determineValueSerializer();

  public Object getCollatingValue(final Object key) {
    if (key != null && indexDefinition != null) return indexDefinition.getCollate().transform(key);
    return key;
  }

  protected void commitSnapshot(Map<Object, Object> snapshot) {
    throw new UnsupportedOperationException();
  }

  protected void putInSnapshot(Object key, OIdentifiable value, Map<Object, Object> snapshot) {
    throw new UnsupportedOperationException();
  }

  protected void removeFromSnapshot(Object key, OIdentifiable value, Map<Object, Object> snapshot) {
    throw new UnsupportedOperationException();
  }

  protected void removeFromSnapshot(Object key, Map<Object, Object> snapshot) {
    throw new UnsupportedOperationException();
  }

  protected void clearSnapshot(IndexTxSnapshot indexTxSnapshot) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int compareTo(OIndex index) {
    acquireSharedLock();
    try {
      final String name = index.getName();
      return this.name.compareTo(name);
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public String getIndexNameByKey(final Object key) {
    OBaseIndexEngine engine;

    while (true) {
      try {
        engine = storage.getIndexEngine(indexId);
        break;
      } catch (OInvalidIndexEngineIdException ignore) {
        doReloadIndexEngine();
      }
    }
    return engine.getIndexNameByKey(key);
  }

  @Override
  public boolean acquireAtomicExclusiveLock(Object key) {
    OBaseIndexEngine engine;

    while (true) {
      try {
        engine = storage.getIndexEngine(indexId);
        break;
      } catch (OInvalidIndexEngineIdException ignore) {
        doReloadIndexEngine();
      }
    }

    return engine.acquireAtomicExclusiveLock(key);
  }

  protected static ODatabaseDocumentInternal getDatabase() {
    return ODatabaseRecordThreadLocal.instance().get();
  }

  private long[] indexCluster(
      final String clusterName,
      final OProgressListener iProgressListener,
      long documentNum,
      long documentIndexed,
      long documentTotal) {
    if (indexDefinition == null)
      throw new OConfigurationException(
          "Index '"
              + name
              + "' cannot be rebuilt because has no a valid definition ("
              + indexDefinition
              + ")");
    ODatabaseDocumentInternal database = getDatabase();
    database.begin();
    for (final ORecord record : database.browseCluster(clusterName)) {
      if (Thread.interrupted())
        throw new OCommandExecutionException("The index rebuild has been interrupted");

      if (record instanceof ODocument) {
        final ODocument doc = (ODocument) record;
        OClassIndexManager.reIndex(doc, database, this);
        ++documentIndexed;
      }
      if (documentIndexed % 1000 == 0) {
        database.commit();
      }
      documentNum++;

      if (iProgressListener != null)
        iProgressListener.onProgress(
            this, documentNum, (float) (documentNum * 100.0 / documentTotal));
    }
    database.commit();

    return new long[] {documentNum, documentIndexed};
  }

  protected void releaseExclusiveLock() {
    rwLock.writeLock().unlock();
  }

  protected void acquireExclusiveLock() {
    rwLock.writeLock().lock();
  }

  protected void releaseSharedLock() {
    rwLock.readLock().unlock();
  }

  protected void acquireSharedLock() {
    rwLock.readLock().lock();
  }

  private void removeValuesContainer() {
    if (valueContainerAlgorithm.equals(ODefaultIndexFactory.SBTREE_BONSAI_VALUE_CONTAINER)) {

      final OAtomicOperation atomicOperation =
          storage.getAtomicOperationsManager().getCurrentOperation();

      final OReadCache readCache = storage.getReadCache();
      final OWriteCache writeCache = storage.getWriteCache();

      if (atomicOperation == null) {
        try {
          final String fileName = name + OIndexRIDContainer.INDEX_FILE_EXTENSION;
          if (writeCache.exists(fileName)) {
            final long fileId = writeCache.loadFile(fileName);
            readCache.deleteFile(fileId, writeCache);
          }
        } catch (IOException e) {
          OLogManager.instance().error(this, "Cannot delete file for value containers", e);
        }
      } else {
        try {
          final String fileName = name + OIndexRIDContainer.INDEX_FILE_EXTENSION;
          if (atomicOperation.isFileExists(fileName)) {
            final long fileId = atomicOperation.loadFile(fileName);
            atomicOperation.deleteFile(fileId);
          }
        } catch (IOException e) {
          OLogManager.instance().error(this, "Cannot delete file for value containers", e);
        }
      }
    }
  }

  protected void onIndexEngineChange(final int indexId) {
    while (true)
      try {
        storage.callIndexEngine(
            false,
            indexId,
            engine -> {
              engine.init(name, type, indexDefinition, isAutomatic(), metadata);
              return null;
            });
        break;
      } catch (OInvalidIndexEngineIdException ignore) {
        doReloadIndexEngine();
      }
  }

  IndexConfiguration indexConfigurationInstance(final ODocument document) {
    return new IndexConfiguration(document);
  }

  public static final class IndexTxSnapshot {
    public Map<Object, Object> indexSnapshot = new HashMap<>();
    public boolean clear = false;
  }

  protected static class IndexConfiguration {
    protected final ODocument document;

    public IndexConfiguration(ODocument document) {
      this.document = document;
    }

    private ODocument getDocument() {
      return document;
    }

    private synchronized void updateConfiguration(
        String type,
        String name,
        int version,
        OIndexDefinition indexDefinition,
        Set<String> clustersToIndex,
        String algorithm,
        String valueContainerAlgorithm) {
      document.field(OIndexInternal.CONFIG_TYPE, type);
      document.field(OIndexInternal.CONFIG_NAME, name);
      document.field(OIndexInternal.INDEX_VERSION, version);

      if (indexDefinition != null) {

        final ODocument indexDefDocument = indexDefinition.toStream();
        if (!indexDefDocument.hasOwners()) ODocumentInternal.addOwner(indexDefDocument, document);

        document.field(OIndexInternal.INDEX_DEFINITION, indexDefDocument, OType.EMBEDDED);
        document.field(OIndexInternal.INDEX_DEFINITION_CLASS, indexDefinition.getClass().getName());
      } else {
        document.removeField(OIndexInternal.INDEX_DEFINITION);
        document.removeField(OIndexInternal.INDEX_DEFINITION_CLASS);
      }

      document.field(CONFIG_CLUSTERS, clustersToIndex, OType.EMBEDDEDSET);
      document.field(ALGORITHM, algorithm);
      document.field(VALUE_CONTAINER_ALGORITHM, valueContainerAlgorithm);
    }
  }

  public static void manualIndexesWarning() {
    if (!OGlobalConfiguration.INDEX_ALLOW_MANUAL_INDEXES.getValueAsBoolean()) {
      throw new OManualIndexesAreProhibited(
          "Manual indexes are deprecated, not supported any more and will be removed in next versions if you still want to use them, "
              + "please set global property `"
              + OGlobalConfiguration.INDEX_ALLOW_MANUAL_INDEXES.getKey()
              + "` to `true`");
    }

    if (OGlobalConfiguration.INDEX_ALLOW_MANUAL_INDEXES_WARNING.getValueAsBoolean()) {
      OLogManager.instance()
          .warn(
              OIndexAbstract.class,
              "Seems you use manual indexes. "
                  + "Manual indexes are deprecated, not supported any more and will be removed in next versions if you do not want "
                  + "to see warning, please set global property `"
                  + OGlobalConfiguration.INDEX_ALLOW_MANUAL_INDEXES_WARNING.getKey()
                  + "` to `false`");
    }
  }

  private static class StreamWrapper extends OIndexAbstractCursor {
    private final Iterator<ORawPair<Object, ORID>> iterator;

    private StreamWrapper(final Stream<ORawPair<Object, ORID>> stream) {
      iterator = stream.iterator();
    }

    @Override
    public Map.Entry<Object, OIdentifiable> nextEntry() {
      if (iterator.hasNext()) {
        final ORawPair<Object, ORID> pair = iterator.next();

        return new Map.Entry<Object, OIdentifiable>() {
          @Override
          public Object getKey() {
            return pair.first;
          }

          @Override
          public OIdentifiable getValue() {
            return pair.second;
          }

          @Override
          public OIdentifiable setValue(OIdentifiable value) {
            throw new UnsupportedOperationException();
          }
        };
      }

      return null;
    }
  }

  /**
   * Indicates search behavior in case of {@link
   * com.orientechnologies.orient.core.index.OCompositeKey} keys that have less amount of internal
   * keys are used, whether lowest or highest partially matched key should be used. Such keys is
   * allowed to use only in
   */
  public enum PartialSearchMode {
    /** Any partially matched key will be used as search result. */
    NONE,
    /** The biggest partially matched key will be used as search result. */
    HIGHEST_BOUNDARY,

    /** The smallest partially matched key will be used as search result. */
    LOWEST_BOUNDARY
  }

  private static Object enhanceCompositeKey(
      Object key, OIndexAbstract.PartialSearchMode partialSearchMode, OIndexDefinition definition) {
    if (!(key instanceof OCompositeKey)) return key;

    final OCompositeKey compositeKey = (OCompositeKey) key;
    final int keySize = definition.getParamCount();

    if (!(keySize == 1
        || compositeKey.getKeys().size() == keySize
        || partialSearchMode.equals(OIndexAbstract.PartialSearchMode.NONE))) {
      final OCompositeKey fullKey = new OCompositeKey(compositeKey);
      int itemsToAdd = keySize - fullKey.getKeys().size();

      final Comparable<?> keyItem;
      if (partialSearchMode.equals(OIndexAbstract.PartialSearchMode.HIGHEST_BOUNDARY))
        keyItem = ALWAYS_GREATER_KEY;
      else keyItem = ALWAYS_LESS_KEY;

      for (int i = 0; i < itemsToAdd; i++) fullKey.addKey(keyItem);

      return fullKey;
    }

    return key;
  }

  protected Object enhanceToCompositeKeyBetweenAsc(Object keyTo, boolean toInclusive) {
    OIndexAbstract.PartialSearchMode partialSearchModeTo;
    if (toInclusive) partialSearchModeTo = OIndexAbstract.PartialSearchMode.HIGHEST_BOUNDARY;
    else partialSearchModeTo = OIndexAbstract.PartialSearchMode.LOWEST_BOUNDARY;

    keyTo = enhanceCompositeKey(keyTo, partialSearchModeTo, getDefinition());
    return keyTo;
  }

  protected Object enhanceFromCompositeKeyBetweenAsc(Object keyFrom, boolean fromInclusive) {
    OIndexAbstract.PartialSearchMode partialSearchModeFrom;
    if (fromInclusive) partialSearchModeFrom = OIndexAbstract.PartialSearchMode.LOWEST_BOUNDARY;
    else partialSearchModeFrom = OIndexAbstract.PartialSearchMode.HIGHEST_BOUNDARY;

    keyFrom = enhanceCompositeKey(keyFrom, partialSearchModeFrom, getDefinition());
    return keyFrom;
  }

  protected Object enhanceToCompositeKeyBetweenDesc(Object keyTo, boolean toInclusive) {
    OIndexAbstract.PartialSearchMode partialSearchModeTo;
    if (toInclusive) partialSearchModeTo = OIndexAbstract.PartialSearchMode.HIGHEST_BOUNDARY;
    else partialSearchModeTo = OIndexAbstract.PartialSearchMode.LOWEST_BOUNDARY;

    keyTo = enhanceCompositeKey(keyTo, partialSearchModeTo, getDefinition());
    return keyTo;
  }

  protected Object enhanceFromCompositeKeyBetweenDesc(Object keyFrom, boolean fromInclusive) {
    OIndexAbstract.PartialSearchMode partialSearchModeFrom;
    if (fromInclusive) partialSearchModeFrom = OIndexAbstract.PartialSearchMode.LOWEST_BOUNDARY;
    else partialSearchModeFrom = OIndexAbstract.PartialSearchMode.HIGHEST_BOUNDARY;

    keyFrom = enhanceCompositeKey(keyFrom, partialSearchModeFrom, getDefinition());
    return keyFrom;
  }
}
