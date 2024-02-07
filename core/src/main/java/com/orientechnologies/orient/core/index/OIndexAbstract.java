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
import com.orientechnologies.orient.core.index.comparator.OAlwaysGreaterKey;
import com.orientechnologies.orient.core.index.comparator.OAlwaysLessKey;
import com.orientechnologies.orient.core.index.engine.OBaseIndexEngine;
import com.orientechnologies.orient.core.index.iterator.OIndexCursorStream;
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
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges.OPERATION;
import com.orientechnologies.orient.core.tx.OTransactionIndexChangesPerKey;
import com.orientechnologies.orient.core.tx.OTransactionIndexChangesPerKey.OTransactionIndexEntry;
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
  protected final OAbstractPaginatedStorage storage;
  private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();

  protected volatile int indexId = -1;
  protected volatile int apiVersion = -1;

  protected Set<String> clustersToIndex = new HashSet<>();
  protected OIndexMetadata im;

  public OIndexAbstract(OIndexMetadata im, final OStorage storage) {
    acquireExclusiveLock();
    try {
      this.im = im;
      this.storage = (OAbstractPaginatedStorage) storage;
    } finally {
      releaseExclusiveLock();
    }
  }

  public static OIndexMetadata loadMetadataFromDoc(final ODocument config) {
    return loadMetadataInternal(
        config,
        config.field(OIndexInternal.CONFIG_TYPE),
        config.field(OIndexInternal.ALGORITHM),
        config.field(OIndexInternal.VALUE_CONTAINER_ALGORITHM));
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

    final int indexVersion =
        config.field(OIndexInternal.INDEX_VERSION) == null
            ? 1
            : (Integer) config.field(OIndexInternal.INDEX_VERSION);

    return new OIndexMetadata(
        indexName,
        loadedIndexDefinition,
        clusters,
        type,
        algorithm,
        valueContainerAlgorithm,
        indexVersion,
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
    acquireExclusiveLock();
    try {
      Set<String> clustersToIndex = indexMetadata.getClustersToIndex();

      if (clustersToIndex != null) this.clustersToIndex = new HashSet<>(clustersToIndex);
      else this.clustersToIndex = new HashSet<>();

      // do not remove this, it is needed to remove index garbage if such one exists
      try {
        if (apiVersion == 0) {
          removeValuesContainer();
        }
      } catch (Exception e) {
        OLogManager.instance().error(this, "Error during deletion of index '%s'", e, im.getName());
      }
      Map<String, String> engineProperties = new HashMap<>();
      // this property is used for autosharded index
      if (im.getMetadata() != null && im.getMetadata().containsField("partitions")) {
        engineProperties.put("partitions", im.getMetadata().field("partitions"));
      } else {
        engineProperties.put("partitions", Integer.toString(clustersToIndex.size()));
      }
      indexMetadata.setVersion(im.getVersion());
      indexId = storage.addIndexEngine(indexMetadata, engineProperties);
      apiVersion = OAbstractPaginatedStorage.extractEngineAPIVersion(indexId);

      assert indexId >= 0;
      assert apiVersion >= 0;

      onIndexEngineChange(indexId);

      if (rebuild) fillIndex(progressListener, false);

      updateConfiguration();
    } catch (Exception e) {
      OLogManager.instance().error(this, "Exception during index '%s' creation", e, im.getName());
      // index is created inside of storage
      if (indexId >= 0) {
        doDelete();
      }
      throw OException.wrapException(
          new OIndexException("Cannot create the index '" + im.getName() + "'"), e);
    } finally {
      releaseExclusiveLock();
    }

    return this;
  }

  protected void doReloadIndexEngine() {
    indexId = storage.loadIndexEngine(im.getName());
    apiVersion = OAbstractPaginatedStorage.extractEngineAPIVersion(indexId);

    if (indexId < 0) {
      throw new IllegalStateException("Index " + im.getName() + " can not be loaded");
    }
  }

  public boolean loadFromConfiguration(final ODocument config) {
    acquireExclusiveLock();
    try {
      clustersToIndex.clear();

      final OIndexMetadata indexMetadata = loadMetadata(config);
      this.im = indexMetadata;
      clustersToIndex.addAll(indexMetadata.getClustersToIndex());

      try {
        indexId = storage.loadIndexEngine(im.getName());
        apiVersion = OAbstractPaginatedStorage.extractEngineAPIVersion(indexId);

        if (indexId == -1) {
          Map<String, String> engineProperties = new HashMap<>();
          // this property is used for autosharded index
          if (im.getMetadata() != null && im.getMetadata().containsField("partitions")) {
            engineProperties.put("partitions", im.getMetadata().field("partitions"));
          } else {
            engineProperties.put("partitions", Integer.toString(clustersToIndex.size()));
          }
          indexId = storage.loadExternalIndexEngine(indexMetadata, engineProperties);
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
                Optional.ofNullable(im.getName()).orElse("null"));

        if (isAutomatic()) {
          // AUTOMATIC REBUILD IT
          OLogManager.instance()
              .warn(this, "Cannot load index '%s' rebuilt it from scratch", im.getName());
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
                    im.getName());
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

  @Override
  public OIndexMetadata loadMetadata(final ODocument config) {
    return loadMetadataInternal(
        config, im.getType(), im.getAlgorithm(), im.getValueContainerAlgorithm());
  }

  /** {@inheritDoc} */
  public long rebuild() {
    return rebuild(new OIndexRebuildOutputListener(this));
  }

  @Override
  public void close() {}

  /**
   * @return number of entries in the index.
   */
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

  /**
   * @return Number of keys in index
   */
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
    return new OIndexCursorStream(stream());
  }

  @Deprecated
  @Override
  public OIndexCursor descCursor() {
    return new OIndexCursorStream(descStream());
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
    return new OIndexCursorStream(streamEntries(keys, ascSortOrder));
  }

  @Deprecated
  @Override
  public OIndexCursor iterateEntriesBetween(
      Object fromKey, boolean fromInclusive, Object toKey, boolean toInclusive, boolean ascOrder) {
    return new OIndexCursorStream(
        streamEntriesBetween(fromKey, fromInclusive, toKey, toInclusive, ascOrder));
  }

  @Deprecated
  @Override
  public OIndexCursor iterateEntriesMajor(Object fromKey, boolean fromInclusive, boolean ascOrder) {
    return new OIndexCursorStream(streamEntriesMajor(fromKey, fromInclusive, ascOrder));
  }

  @Deprecated
  @Override
  public OIndexCursor iterateEntriesMinor(Object toKey, boolean toInclusive, boolean ascOrder) {
    return new OIndexCursorStream(streamEntriesMajor(toKey, toInclusive, ascOrder));
  }

  /** {@inheritDoc} */
  public long rebuild(final OProgressListener iProgressListener) {
    long documentIndexed;

    acquireExclusiveLock();
    try {
      try {
        if (indexId >= 0) {
          doDelete();
        }
      } catch (Exception e) {
        OLogManager.instance().error(this, "Error during index '%s' delete", e, im.getName());
      }

      OIndexMetadata indexMetadata = this.loadMetadata(updateConfiguration());
      Map<String, String> engineProperties = new HashMap<>();
      // this property is used for autosharded index
      if (im.getMetadata() != null && im.getMetadata().containsField("partitions")) {
        engineProperties.put("partitions", im.getMetadata().field("partitions"));
      } else {
        engineProperties.put("partitions", Integer.toString(clustersToIndex.size()));
      }
      indexId = storage.addIndexEngine(indexMetadata, engineProperties);
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
            ODatabaseDocumentInternal database = getDatabase();
            Iterator<ORawPair<Object, ORID>> iterator = stream.iterator();
            long count = 0;
            database.begin();
            try {
              while (iterator.hasNext()) {
                ORawPair<Object, ORID> pair = iterator.next();
                remove(pair.first, pair.second);
                if (count % 1000 == 0) {
                  database.commit();
                  database.begin();
                }
              }
            } finally {
              database.commit();
            }
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
    return im.getName();
  }

  public String getType() {
    return im.getType();
  }

  @Override
  public String getAlgorithm() {
    acquireSharedLock();
    try {
      return im.getAlgorithm();
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public String toString() {
    acquireSharedLock();
    try {
      return im.getName();
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
    return im.getVersion();
  }

  public ODocument updateConfiguration() {
    ODocument document = new ODocument();
    document.field(OIndexInternal.CONFIG_TYPE, im.getType());
    document.field(OIndexInternal.CONFIG_NAME, im.getName());
    document.field(OIndexInternal.INDEX_VERSION, im.getVersion());

    if (im.getIndexDefinition() != null) {

      final ODocument indexDefDocument = im.getIndexDefinition().toStream();
      if (!indexDefDocument.hasOwners()) ODocumentInternal.addOwner(indexDefDocument, document);

      document.field(OIndexInternal.INDEX_DEFINITION, indexDefDocument, OType.EMBEDDED);
      document.field(
          OIndexInternal.INDEX_DEFINITION_CLASS, im.getIndexDefinition().getClass().getName());
    } else {
      document.removeField(OIndexInternal.INDEX_DEFINITION);
      document.removeField(OIndexInternal.INDEX_DEFINITION_CLASS);
    }

    document.field(CONFIG_CLUSTERS, clustersToIndex, OType.EMBEDDEDSET);
    document.field(ALGORITHM, im.getAlgorithm());
    document.field(VALUE_CONTAINER_ALGORITHM, im.getValueContainerAlgorithm());
    if (im.getMetadata() != null)
      document.field(OIndexInternal.METADATA, im.getMetadata(), OType.EMBEDDED);

    return document;
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
  public Iterable<OTransactionIndexEntry> interpretTxKeyChanges(
      OTransactionIndexChangesPerKey changes) {
    return changes.getEntriesAsList();
  }

  public ODocument getConfiguration() {
    return updateConfiguration();
  }

  @Override
  public ODocument getMetadata() {
    return im.getMetadata();
  }

  @Override
  public boolean isUnique() {
    return false;
  }

  public boolean isAutomatic() {
    acquireSharedLock();
    try {
      return im.getIndexDefinition() != null && im.getIndexDefinition().getClassName() != null;
    } finally {
      releaseSharedLock();
    }
  }

  public OType[] getKeyTypes() {
    acquireSharedLock();
    try {
      if (im.getIndexDefinition() == null) return null;

      return im.getIndexDefinition().getTypes();
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
    return im.getIndexDefinition();
  }

  @Override
  public boolean equals(final Object o) {
    acquireSharedLock();
    try {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      final OIndexAbstract that = (OIndexAbstract) o;

      return im.getName().equals(that.im.getName());
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public int hashCode() {
    acquireSharedLock();
    try {
      return im.getName().hashCode();
    } finally {
      releaseSharedLock();
    }
  }

  public int getIndexId() {
    return indexId;
  }

  public String getDatabaseName() {
    return storage.getName();
  }

  public Object getCollatingValue(final Object key) {
    if (key != null && im.getIndexDefinition() != null)
      return im.getIndexDefinition().getCollate().transform(key);
    return key;
  }

  @Override
  public int compareTo(OIndex index) {
    acquireSharedLock();
    try {
      final String name = index.getName();
      return this.im.getName().compareTo(name);
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
    if (im.getIndexDefinition() == null)
      throw new OConfigurationException(
          "Index '"
              + im.getName()
              + "' cannot be rebuilt because has no a valid definition ("
              + im.getIndexDefinition()
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
        database.begin();
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
    if (im.getAlgorithm().equals(ODefaultIndexFactory.SBTREE_BONSAI_VALUE_CONTAINER)) {

      final OAtomicOperation atomicOperation =
          storage.getAtomicOperationsManager().getCurrentOperation();

      final OReadCache readCache = storage.getReadCache();
      final OWriteCache writeCache = storage.getWriteCache();

      if (atomicOperation == null) {
        try {
          final String fileName = im.getName() + OIndexRIDContainer.INDEX_FILE_EXTENSION;
          if (writeCache.exists(fileName)) {
            final long fileId = writeCache.loadFile(fileName);
            readCache.deleteFile(fileId, writeCache);
          }
        } catch (IOException e) {
          OLogManager.instance().error(this, "Cannot delete file for value containers", e);
        }
      } else {
        try {
          final String fileName = im.getName() + OIndexRIDContainer.INDEX_FILE_EXTENSION;
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
              engine.init(im);
              return null;
            });
        break;
      } catch (OInvalidIndexEngineIdException ignore) {
        doReloadIndexEngine();
      }
  }

  public static void manualIndexesWarning() {
    if (!OGlobalConfiguration.INDEX_ALLOW_MANUAL_INDEXES.getValueAsBoolean()) {
      throw new OManualIndexesAreProhibited(
          "Manual indexes are deprecated, not supported any more and will be removed in next"
              + " versions if you still want to use them, please set global property `"
              + OGlobalConfiguration.INDEX_ALLOW_MANUAL_INDEXES.getKey()
              + "` to `true`");
    }

    if (OGlobalConfiguration.INDEX_ALLOW_MANUAL_INDEXES_WARNING.getValueAsBoolean()) {
      OLogManager.instance()
          .warn(
              OIndexAbstract.class,
              "Seems you use manual indexes. Manual indexes are deprecated, not supported any more"
                  + " and will be removed in next versions if you do not want to see warning,"
                  + " please set global property `"
                  + OGlobalConfiguration.INDEX_ALLOW_MANUAL_INDEXES_WARNING.getKey()
                  + "` to `false`");
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
      Object key, PartialSearchMode partialSearchMode, OIndexDefinition definition) {
    if (!(key instanceof OCompositeKey)) return key;

    final OCompositeKey compositeKey = (OCompositeKey) key;
    final int keySize = definition.getParamCount();

    if (!(keySize == 1
        || compositeKey.getKeys().size() == keySize
        || partialSearchMode.equals(PartialSearchMode.NONE))) {
      final OCompositeKey fullKey = new OCompositeKey(compositeKey);
      int itemsToAdd = keySize - fullKey.getKeys().size();

      final Comparable<?> keyItem;
      if (partialSearchMode.equals(PartialSearchMode.HIGHEST_BOUNDARY))
        keyItem = ALWAYS_GREATER_KEY;
      else keyItem = ALWAYS_LESS_KEY;

      for (int i = 0; i < itemsToAdd; i++) fullKey.addKey(keyItem);

      return fullKey;
    }

    return key;
  }

  public Object enhanceToCompositeKeyBetweenAsc(Object keyTo, boolean toInclusive) {
    PartialSearchMode partialSearchModeTo;
    if (toInclusive) partialSearchModeTo = PartialSearchMode.HIGHEST_BOUNDARY;
    else partialSearchModeTo = PartialSearchMode.LOWEST_BOUNDARY;

    keyTo = enhanceCompositeKey(keyTo, partialSearchModeTo, getDefinition());
    return keyTo;
  }

  public Object enhanceFromCompositeKeyBetweenAsc(Object keyFrom, boolean fromInclusive) {
    PartialSearchMode partialSearchModeFrom;
    if (fromInclusive) partialSearchModeFrom = PartialSearchMode.LOWEST_BOUNDARY;
    else partialSearchModeFrom = PartialSearchMode.HIGHEST_BOUNDARY;

    keyFrom = enhanceCompositeKey(keyFrom, partialSearchModeFrom, getDefinition());
    return keyFrom;
  }

  public Object enhanceToCompositeKeyBetweenDesc(Object keyTo, boolean toInclusive) {
    PartialSearchMode partialSearchModeTo;
    if (toInclusive) partialSearchModeTo = PartialSearchMode.HIGHEST_BOUNDARY;
    else partialSearchModeTo = PartialSearchMode.LOWEST_BOUNDARY;

    keyTo = enhanceCompositeKey(keyTo, partialSearchModeTo, getDefinition());
    return keyTo;
  }

  public Object enhanceFromCompositeKeyBetweenDesc(Object keyFrom, boolean fromInclusive) {
    PartialSearchMode partialSearchModeFrom;
    if (fromInclusive) partialSearchModeFrom = PartialSearchMode.LOWEST_BOUNDARY;
    else partialSearchModeFrom = PartialSearchMode.HIGHEST_BOUNDARY;

    keyFrom = enhanceCompositeKey(keyFrom, partialSearchModeFrom, getDefinition());
    return keyFrom;
  }
}
