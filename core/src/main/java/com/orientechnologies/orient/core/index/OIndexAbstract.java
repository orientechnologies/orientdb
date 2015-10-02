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
package com.orientechnologies.orient.core.index;

import com.orientechnologies.common.concur.lock.ONewLockManager;
import com.orientechnologies.common.concur.lock.OReadersWriterSpinLock;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.orient.core.OOrientShutdownListener;
import com.orientechnologies.orient.core.OOrientStartupListener;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.annotation.ODocumentInstance;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OIndexRIDContainer;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.exception.OTransactionException;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerSchemaAware2CSV;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerAnyStreamable;
import com.orientechnologies.orient.core.storage.cache.OReadCache;
import com.orientechnologies.orient.core.storage.cache.OWriteCache;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges.OPERATION;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Handles indexing when records change.
 *
 * @author Luca Garulli
 */
public abstract class OIndexAbstract<T> implements OIndexInternal<T>, OOrientStartupListener, OOrientShutdownListener {
  protected static final String                 CONFIG_MAP_RID  = "mapRid";
  protected static final String                 CONFIG_CLUSTERS = "clusters";
  protected final String                        type;
  protected final ONewLockManager<Object>       keyLockManager  = new ONewLockManager<Object>();
  @ODocumentInstance
  protected final AtomicReference<ODocument>    configuration   = new AtomicReference<ODocument>();
  protected final ODocument                     metadata;
  protected final OAbstractPaginatedStorage     storage;
  private final String                          databaseName;
  private final String                          name;
  private final OReadersWriterSpinLock          rwLock          = new OReadersWriterSpinLock();
  private final int                             version;
  protected String                              valueContainerAlgorithm;
  protected int                                 indexId         = -1;
  private String                                algorithm;
  private Set<String>                           clustersToIndex = new HashSet<String>();
  private volatile OIndexDefinition             indexDefinition;
  private volatile boolean                      rebuilding      = false;
  private Thread                                rebuildThread   = null;
  private volatile ThreadLocal<IndexTxSnapshot> txSnapshot      = new IndexTxSnapshotThreadLocal();

  public OIndexAbstract(String name, final String type, String algorithm, String valueContainerAlgorithm, ODocument metadata,
      int version, OAbstractPaginatedStorage storage) {
    acquireExclusiveLock();
    try {
      databaseName = ODatabaseRecordThreadLocal.INSTANCE.get().getName();

      this.version = version;
      this.name = name;
      this.type = type;
      this.algorithm = algorithm;
      this.metadata = metadata;
      this.valueContainerAlgorithm = valueContainerAlgorithm;
      this.storage = storage;

      Orient.instance().registerWeakOrientStartupListener(this);
      Orient.instance().registerWeakOrientShutdownListener(this);
    } finally {
      releaseExclusiveLock();
    }
  }

  public static IndexMetadata loadMetadataInternal(final ODocument config, final String type, final String algorithm,
      final String valueContainerAlgorithm) {
    String indexName = config.field(OIndexInternal.CONFIG_NAME);

    final ODocument indexDefinitionDoc = config.field(OIndexInternal.INDEX_DEFINITION);
    OIndexDefinition loadedIndexDefinition = null;
    if (indexDefinitionDoc != null) {
      try {
        final String indexDefClassName = config.field(OIndexInternal.INDEX_DEFINITION_CLASS);
        final Class<?> indexDefClass = Class.forName(indexDefClassName);
        loadedIndexDefinition = (OIndexDefinition) indexDefClass.getDeclaredConstructor().newInstance();
        loadedIndexDefinition.fromStream(indexDefinitionDoc);

      } catch (final ClassNotFoundException e) {
        throw OException.wrapException(new OIndexException("Error during deserialization of index definition"), e);
      } catch (final NoSuchMethodException e) {
        throw OException.wrapException(new OIndexException("Error during deserialization of index definition"), e);
      } catch (final InvocationTargetException e) {
        throw OException.wrapException(new OIndexException("Error during deserialization of index definition"), e);
      } catch (final InstantiationException e) {
        throw OException.wrapException(new OIndexException("Error during deserialization of index definition"), e);
      } catch (final IllegalAccessException e) {
        throw OException.wrapException(new OIndexException("Error during deserialization of index definition"), e);
      }
    } else {
      // @COMPATIBILITY 1.0rc6 new index model was implemented
      final Boolean isAutomatic = config.field(OIndexInternal.CONFIG_AUTOMATIC);
      OIndexFactory factory = OIndexes.getFactory(type, algorithm);
      if (Boolean.TRUE.equals(isAutomatic)) {
        final int pos = indexName.lastIndexOf('.');
        if (pos < 0)
          throw new OIndexException("Cannot convert from old index model to new one. "
              + "Invalid index name. Dot (.) separator should be present");
        final String className = indexName.substring(0, pos);
        final String propertyName = indexName.substring(pos + 1);

        final String keyTypeStr = config.field(OIndexInternal.CONFIG_KEYTYPE);
        if (keyTypeStr == null)
          throw new OIndexException("Cannot convert from old index model to new one. " + "Index key type is absent");
        final OType keyType = OType.valueOf(keyTypeStr.toUpperCase(Locale.ENGLISH));

        loadedIndexDefinition = new OPropertyIndexDefinition(className, propertyName, keyType);

        config.removeField(OIndexInternal.CONFIG_AUTOMATIC);
        config.removeField(OIndexInternal.CONFIG_KEYTYPE);
      } else if (config.field(OIndexInternal.CONFIG_KEYTYPE) != null) {
        final String keyTypeStr = config.field(OIndexInternal.CONFIG_KEYTYPE);
        final OType keyType = OType.valueOf(keyTypeStr.toUpperCase(Locale.ENGLISH));

        loadedIndexDefinition = new OSimpleKeyIndexDefinition(factory.getLastVersion(), keyType);

        config.removeField(OIndexInternal.CONFIG_KEYTYPE);
      }
    }

    final Set<String> clusters = new HashSet<String>((Collection<String>) config.field(CONFIG_CLUSTERS, OType.EMBEDDEDSET));

    return new IndexMetadata(indexName, loadedIndexDefinition, clusters, type, algorithm, valueContainerAlgorithm);
  }

  @Override
  public void onShutdown() {
    txSnapshot = null;
  }

  @Override
  public void onStartup() {
    if (txSnapshot == null)
      txSnapshot = new IndexTxSnapshotThreadLocal();
  }

  public void flush() {
  }

  @Override
  public boolean hasRangeQuerySupport() {
    acquireSharedLock();
    try {
      return storage.hasIndexRangeQuerySupport(indexId);
    } finally {
      releaseSharedLock();
    }
  }

  /**
   * Creates the index.
   *
   * @param clusterIndexName
   *          Cluster name where to place the TreeMap
   * @param clustersToIndex
   * @param rebuild
   * @param progressListener
   * @param valueSerializer
   */
  public OIndexInternal<?> create(final OIndexDefinition indexDefinition, final String clusterIndexName,
      final Set<String> clustersToIndex, boolean rebuild, final OProgressListener progressListener,
      final OBinarySerializer valueSerializer) {
    acquireExclusiveLock();
    try {
      configuration.set(new ODocument().setTrackingChanges(false));

      this.indexDefinition = indexDefinition;

      if (clustersToIndex != null)
        this.clustersToIndex = new HashSet<String>(clustersToIndex);
      else
        this.clustersToIndex = new HashSet<String>();

      // do not remove this, it is needed to remove index garbage if such one exists
      try {
        removeValuesContainer();
      } catch (Exception e) {
        OLogManager.instance().error(this, "Error during deletion of index '%s'", name);
      }

      final Boolean durableInNonTxMode = isDurableInNonTxMode();

      indexId = storage.addIndexEngine(name, algorithm, indexDefinition, valueSerializer, isAutomatic(), durableInNonTxMode,
          version, getEngineProperties());
      assert indexId >= 0;

      onIndexEngineChange(indexId);

      if (rebuild)
        rebuild(progressListener);

      updateConfiguration();
    } catch (Exception e) {
      OLogManager.instance().error(this, "Exception during index '%s' creation", e, name);

      try {
        if (indexId >= 0)
          storage.deleteIndexEngine(indexId);

      } catch (Exception ex) {
        OLogManager.instance().error(this, "Exception during index '%s' deletion", ex, name);
      }

      if (e instanceof OIndexException)
        throw (OIndexException) e;

      throw OException.wrapException(new OIndexException("Cannot create the index '" + name + "'"), e);

    } finally {
      releaseExclusiveLock();
    }

    return this;
  }

  private Boolean isDurableInNonTxMode() {
    Boolean durableInNonTxMode;

    Object durable = null;

    if (metadata != null) {
      durable = metadata.field("durableInNonTxMode");
    }

    if (durable instanceof Boolean)
      durableInNonTxMode = (Boolean) durable;
    else
      durableInNonTxMode = null;
    return durableInNonTxMode;
  }

  public boolean loadFromConfiguration(final ODocument config) {
    acquireExclusiveLock();
    try {
      configuration.set(config);
      clustersToIndex.clear();

      IndexMetadata indexMetadata = loadMetadata(configuration.get());
      indexDefinition = indexMetadata.getIndexDefinition();
      clustersToIndex.addAll(indexMetadata.getClustersToIndex());
      algorithm = indexMetadata.getAlgorithm();
      valueContainerAlgorithm = indexMetadata.getValueContainerAlgorithm();

      try {
        indexId = storage.loadIndexEngine(name);

        if (indexId == -1) {
          indexId = storage.loadExternalIndexEngine(name, algorithm, indexDefinition, determineValueSerializer(), isAutomatic(),
              isDurableInNonTxMode(), version, getEngineProperties());
        }

        if (indexId == -1)
          return false;

        onIndexEngineChange(indexId);

      } catch (Exception e) {
        OLogManager.instance().error(this, "Error during load of index '%s'", e, name != null ? name : "null");

        if (isAutomatic()) {
          // AUTOMATIC REBUILD IT
          OLogManager.instance().warn(this, "Cannot load index '%s' rebuilt it from scratch", getName());
          try {
            rebuild();
          } catch (Throwable t) {
            OLogManager.instance().error(this,
                "Cannot rebuild index '%s'  BECAUSE '" + t + "'. The index will be removed in configuration", e, getName());
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

  protected Map<String, String> getEngineProperties() {
    return Collections.emptyMap();
  }

  @Override
  public IndexMetadata loadMetadata(final ODocument config) {
    return loadMetadataInternal(config, type, algorithm, valueContainerAlgorithm);
  }

  public boolean contains(Object key) {
    checkForRebuild();

    key = getCollatingValue(key);

    final ODatabase database = getDatabase();
    final boolean txIsActive = database.getTransaction().isActive();

    if (!txIsActive)
      keyLockManager.acquireSharedLock(key);
    try {

      acquireSharedLock();
      try {
        assert indexId >= 0;
        return storage.indexContainsKey(indexId, key);
      } finally {
        releaseSharedLock();
      }
    } finally {
      if (!txIsActive)
        keyLockManager.releaseSharedLock(key);
    }
  }

  public long rebuild() {
    return rebuild(new OIndexRebuildOutputListener(this));
  }

  @Override
  public void setRebuildingFlag() {
    rebuilding = true;
  }

  @Override
  public void close() {
  }

  @Override
  public Object getFirstKey() {
    acquireSharedLock();
    try {
      return storage.getIndexFirstKey(indexId);
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public Object getLastKey() {
    acquireSharedLock();
    try {
      return storage.getIndexLastKey(indexId);
    } finally {
      releaseSharedLock();
    }
  }

  /**
   * Populates the index with all the existent records. Uses the massive insert intent to speed up and keep the consumed memory low.
   */
  public long rebuild(final OProgressListener iProgressListener) {
    long documentIndexed = 0;

    final boolean intentInstalled = getDatabase().declareIntent(new OIntentMassiveInsert());

    acquireExclusiveLock();
    try {
      rebuildThread = Thread.currentThread();
      rebuilding = true;

      try {
        if (indexId >= 0)
          storage.deleteIndexEngine(indexId);
      } catch (Exception e) {
        OLogManager.instance().error(this, "Error during index '%s' delete", name);
      }

      removeValuesContainer();

      indexId = storage.addIndexEngine(name, algorithm, indexDefinition, determineValueSerializer(), isAutomatic(),
          isDurableInNonTxMode(), version, getEngineProperties());

      onIndexEngineChange(indexId);

      long documentNum = 0;
      long documentTotal = 0;

      for (final String cluster : clustersToIndex)
        documentTotal += getDatabase().countClusterElements(cluster);

      if (iProgressListener != null)
        iProgressListener.onBegin(this, documentTotal, true);

      // INDEX ALL CLUSTERS
      for (final String clusterName : clustersToIndex) {
        final long[] metrics = indexCluster(clusterName, iProgressListener, documentNum, documentIndexed, documentTotal);
        documentNum = metrics[0];
        documentIndexed = metrics[1];
      }

      if (iProgressListener != null)
        iProgressListener.onCompletition(this, true);

    } catch (final Exception e) {
      if (iProgressListener != null)
        iProgressListener.onCompletition(this, false);

      try {
        if (indexId >= 0)
          storage.clearIndex(indexId);
      } catch (Exception e2) {
        OLogManager.instance().error(this, "Error during index rebuild", e2);
        // IGNORE EXCEPTION: IF THE REBUILD WAS LAUNCHED IN CASE OF RID INVALID CLEAR ALWAYS GOES IN ERROR
      }

      throw OException.wrapException(new OIndexException("Error on rebuilding the index for clusters: " + clustersToIndex), e);

    } finally {
      rebuilding = false;
      rebuildThread = null;

      if (intentInstalled)
        getDatabase().declareIntent(null);

      releaseExclusiveLock();
    }

    return documentIndexed;
  }

  public boolean remove(Object key, final OIdentifiable value) {
    return remove(key);
  }

  public boolean remove(Object key) {
    checkForRebuild();

    key = getCollatingValue(key);

    final ODatabase database = getDatabase();
    final boolean txIsActive = database.getTransaction().isActive();

    if (!txIsActive)
      keyLockManager.acquireExclusiveLock(key);
    try {
      acquireSharedLock();
      try {
        return storage.removeKeyFromIndex(indexId, key);
      } finally {
        releaseSharedLock();
      }
    } finally {
      if (!txIsActive)
        keyLockManager.releaseExclusiveLock(key);
    }
  }

  @Override
  public void lockKeysForUpdateNoTx(Object... key) {
    final ODatabase database = getDatabase();
    final boolean txIsActive = database.getTransaction().isActive();

    if (!txIsActive)
      keyLockManager.acquireExclusiveLocksInBatch(key);
  }

  @Override
  public void lockKeysForUpdateNoTx(Collection<Object> keys) {
    final ODatabase database = getDatabase();
    final boolean txIsActive = database.getTransaction().isActive();

    if (!txIsActive)
      keyLockManager.acquireExclusiveLocksInBatch(keys);
  }

  @Override
  public void releaseKeysForUpdateNoTx(Object... key) {
    if (key == null)
      return;

    final ODatabase database = getDatabase();
    final boolean txIsActive = database.getTransaction().isActive();

    if (!txIsActive) {
      for (Object k : key) {
        keyLockManager.releaseExclusiveLock(k);
      }
    }
  }

  @Override
  public void releaseKeysForUpdateNoTx(Collection<Object> keys) {
    if (keys == null)
      return;

    final ODatabase database = getDatabase();
    final boolean txIsActive = database.getTransaction().isActive();

    if (!txIsActive) {
      for (Object k : keys) {
        keyLockManager.releaseExclusiveLock(k);
      }
    }
  }

  public OIndex<T> clear() {
    checkForRebuild();

    final ODatabase database = getDatabase();
    final boolean txIsActive = database.getTransaction().isActive();

    if (!txIsActive)
      keyLockManager.lockAllExclusive();

    try {
      acquireSharedLock();
      try {
        storage.clearIndex(indexId);
        return this;
      } finally {
        releaseSharedLock();
      }
    } finally {
      if (!txIsActive)
        keyLockManager.unlockAllExclusive();
    }
  }

  public OIndexInternal<T> delete() {
    acquireExclusiveLock();

    try {
      storage.deleteIndexEngine(indexId);

      // REMOVE THE INDEX ALSO FROM CLASS MAP
      if (getDatabase().getMetadata() != null)
        getDatabase().getMetadata().getIndexManager().removeClassPropertyIndex(this);

      removeValuesContainer();

      return this;

    } finally {
      releaseExclusiveLock();
    }
  }

  public String getName() {
    acquireSharedLock();
    try {
      return name;
    } finally {
      releaseSharedLock();
    }
  }

  public String getType() {
    acquireSharedLock();
    try {
      return type;
    } finally {
      releaseSharedLock();
    }
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

  public OIndexInternal<T> getInternal() {
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

  public OIndexAbstract<T> addCluster(final String clusterName) {
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

  public OIndexAbstract<T> removeCluster(String iClusterName) {
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

  public ODocument checkEntry(final OIdentifiable iRecord, final Object iKey) {
    return null;
  }

  public final ODocument updateConfiguration() {
    ODocument oldConfig;
    ODocument newConfig;
    do {
      oldConfig = configuration.get();
      newConfig = new ODocument().setTrackingChanges(false);
      if (oldConfig.hasOwners()) {
        ODocumentInternal.addOwner(newConfig, oldConfig.getOwner());
      } else {
        ORecordInternal.getDirtyManager(oldConfig).removeNew(oldConfig);
      }

      oldConfig.copyTo(newConfig);

      newConfig.setInternalStatus(ORecordElement.STATUS.UNMARSHALLING);

      try {
        doConfigurationUpdate(newConfig);
      } finally {
        newConfig.setInternalStatus(ORecordElement.STATUS.LOADED);
      }

    } while (!configuration.compareAndSet(oldConfig, newConfig));

    return newConfig;
  }

  protected void doConfigurationUpdate(ODocument newConfig) {
    newConfig.field(OIndexInternal.CONFIG_TYPE, type);
    newConfig.field(OIndexInternal.CONFIG_NAME, name);
    newConfig.field(OIndexInternal.INDEX_VERSION, version);

    if (indexDefinition != null) {

      final ODocument indexDefDocument = indexDefinition.toStream();
      if (!indexDefDocument.hasOwners())
        ODocumentInternal.addOwner(indexDefDocument, newConfig);

      newConfig.field(OIndexInternal.INDEX_DEFINITION, indexDefDocument, OType.EMBEDDED);
      newConfig.field(OIndexInternal.INDEX_DEFINITION_CLASS, indexDefinition.getClass().getName());
    } else {
      newConfig.removeField(OIndexInternal.INDEX_DEFINITION);
      newConfig.removeField(OIndexInternal.INDEX_DEFINITION_CLASS);
    }

    newConfig.field(CONFIG_CLUSTERS, clustersToIndex, OType.EMBEDDEDSET);
    newConfig.field(ALGORITHM, algorithm);
    newConfig.field(VALUE_CONTAINER_ALGORITHM, valueContainerAlgorithm);
  }

  @SuppressWarnings("unchecked")
  public void addTxOperation(final ODocument operationDocument) {
    checkForRebuild();

    if (operationDocument == null)
      return;

    acquireSharedLock();
    try {
      final IndexTxSnapshot indexTxSnapshot = txSnapshot.get();

      final Boolean clearAll = operationDocument.field("clear");
      if (clearAll != null && clearAll)
        clearSnapshot(indexTxSnapshot);

      final Collection<ODocument> entries = operationDocument.field("entries");
      final Map<Object, Object> snapshot = indexTxSnapshot.indexSnapshot;
      for (final ODocument entry : entries)
        applyIndexTxEntry(snapshot, entry);

      final ODocument nullIndexEntry = operationDocument.field("nullEntries");
      applyIndexTxEntry(snapshot, nullIndexEntry);
    } finally {
      releaseSharedLock();
    }
  }

  public void commit() {
    acquireSharedLock();
    try {
      final IndexTxSnapshot indexTxSnapshot = txSnapshot.get();
      if (indexTxSnapshot.clear)
        clear();

      commitSnapshot(indexTxSnapshot.indexSnapshot);
    } finally {
      releaseSharedLock();
    }
  }

  public void preCommit() {
    txSnapshot.set(new IndexTxSnapshot());
  }

  public void postCommit() {
    txSnapshot.set(new IndexTxSnapshot());
  }

  public ODocument getConfiguration() {
    acquireSharedLock();
    try {
      return configuration.get();
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public ODocument getMetadata() {
    // return getConfiguration().field("metadata", OType.EMBEDDED);
    return metadata;
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
      if (indexDefinition == null)
        return null;

      return indexDefinition.getTypes();
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public OIndexKeyCursor keyCursor() {
    checkForRebuild();

    acquireSharedLock();
    try {
      return storage.getIndexKeyCursor(indexId);
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
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;

      final OIndexAbstract<?> that = (OIndexAbstract<?>) o;

      if (!name.equals(that.name))
        return false;

      return true;
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

  public String getDatabaseName() {
    return databaseName;
  }

  public boolean isRebuiding() {
    return rebuilding;
  }

  protected abstract OBinarySerializer determineValueSerializer();

  protected void populateIndex(ODocument doc, Object fieldValue) {
    if (fieldValue instanceof Collection) {
      for (final Object fieldValueItem : (Collection<?>) fieldValue) {
        put(fieldValueItem, doc);
      }
    } else
      put(fieldValue, doc);
  }

  public Object getCollatingValue(final Object key) {
    if (key != null && getDefinition() != null)
      return getDefinition().getCollate().transform(key);
    return key;
  }

  protected void commitSnapshot(Map<Object, Object> snapshot) {
    // do nothing by default
    // storage will delay real operations till the end of tx
  }

  protected void putInSnapshot(Object key, OIdentifiable value, Map<Object, Object> snapshot) {
    // storage will delay real operations till the end of tx
    checkForKeyType(key);
    put(key, value);
  }

  protected void removeFromSnapshot(Object key, OIdentifiable value, Map<Object, Object> snapshot) {
    // storage will delay real operations till the end of tx
    remove(key, value);
  }

  protected void removeFromSnapshot(Object key, Map<Object, Object> snapshot) {
    // storage will delay real operations till the end of tx
    remove(key);
  }

  protected void clearSnapshot(IndexTxSnapshot indexTxSnapshot) {
    // storage will delay real operations till the end of tx
    clear();
  }

  @Override
  public int compareTo(OIndex<T> index) {
    acquireSharedLock();
    try {
      final String name = index.getName();
      return this.name.compareTo(name);
    } finally {
      releaseSharedLock();
    }
  }

  protected void checkForKeyType(final Object iKey) {
    if (indexDefinition == null) {
      // RECOGNIZE THE KEY TYPE AT RUN-TIME

      final OType type = OType.getTypeByClass(iKey.getClass());
      if (type == null)
        return;

      indexDefinition = new OSimpleKeyIndexDefinition(version, type);

      updateConfiguration();
    }
  }

  protected ODatabaseDocumentInternal getDatabase() {
    return ODatabaseRecordThreadLocal.INSTANCE.get();
  }

  protected void checkForRebuild() {
    if (rebuilding && !Thread.currentThread().equals(rebuildThread)) {
      throw new OIndexException("Index " + name + " is rebuilding now and cannot be used");
    }
  }

  protected long[] indexCluster(final String clusterName, final OProgressListener iProgressListener, long documentNum,
      long documentIndexed, long documentTotal) {
    try {
      for (final ORecord record : getDatabase().browseCluster(clusterName)) {
        if (Thread.interrupted())
          throw new OCommandExecutionException("The index rebuild has been interrupted");

        if (record instanceof ODocument) {
          final ODocument doc = (ODocument) record;

          if (indexDefinition == null)
            throw new OConfigurationException("Index '" + name + "' cannot be rebuilt because has no a valid definition ("
                + indexDefinition + ")");

          final Object fieldValue = indexDefinition.getDocumentValueToIndex(doc);

          if (fieldValue != null || !indexDefinition.isNullValuesIgnored()) {
            try {
              populateIndex(doc, fieldValue);
            } catch (OIndexException e) {
              OLogManager.instance().error(
                  this,
                  "Exception during index rebuild. Exception was caused by following key/ value pair - key %s, value %s."
                      + " Rebuild will continue from this point", e, fieldValue, doc.getIdentity());
            }

            ++documentIndexed;
          }
        }
        documentNum++;

        if (iProgressListener != null)
          iProgressListener.onProgress(this, documentNum, (float) (documentNum * 100.0 / documentTotal));
      }
    } catch (NoSuchElementException e) {
      // END OF CLUSTER REACHED, IGNORE IT
    }

    return new long[] { documentNum, documentIndexed };
  }

  protected void releaseExclusiveLock() {
    rwLock.releaseWriteLock();
  }

  protected void acquireExclusiveLock() {
    rwLock.acquireWriteLock();
  }

  protected void releaseSharedLock() {
    rwLock.releaseReadLock();
  }

  protected void acquireSharedLock() {
    rwLock.acquireReadLock();
  }

  private void removeValuesContainer() {
    if (valueContainerAlgorithm.equals(ODefaultIndexFactory.SBTREEBONSAI_VALUE_CONTAINER)) {

      final OAtomicOperation atomicOperation = storage.getAtomicOperationsManager().getCurrentOperation();

      final OReadCache readCache = storage.getReadCache();
      final OWriteCache writeCache = storage.getWriteCache();

      if (atomicOperation == null) {
        try {
          final String fileName = getName() + OIndexRIDContainer.INDEX_FILE_EXTENSION;
          if (writeCache.exists(fileName)) {
            final long fileId = readCache.openFile(fileName, writeCache);
            readCache.deleteFile(fileId, writeCache);
          }
        } catch (IOException e) {
          OLogManager.instance().error(this, "Cannot delete file for value containers", e);
        }
      } else {
        try {
          final String fileName = getName() + OIndexRIDContainer.INDEX_FILE_EXTENSION;
          if (atomicOperation.isFileExists(fileName)) {
            final long fileId = atomicOperation.openFile(fileName);
            atomicOperation.deleteFile(fileId);
          }
        } catch (IOException e) {
          OLogManager.instance().error(this, "Cannot delete file for value containers", e);
        }
      }

    }
  }

  private void applyIndexTxEntry(Map<Object, Object> snapshot, ODocument entry) {
    final Object key;
    if (entry.field("k") != null) {
      Object serKey = entry.field("k");
      try {
        ODocument keyContainer = null;
        // Check for PROTOCOL_VERSION_24 that remove CSV serialization.
        if (serKey instanceof String) {
          final String serializedKey = OStringSerializerHelper.decode((String) serKey);
          keyContainer = new ODocument();
          keyContainer.setLazyLoad(false);
          keyContainer.setTrackingChanges(false);

          ORecordSerializerSchemaAware2CSV.INSTANCE.fromString(serializedKey, keyContainer, null);
        } else if (serKey instanceof ODocument) {
          keyContainer = (ODocument) serKey;
        }

        if (keyContainer == null)
          throw new OTransactionException("Key was not provided during key-value pair insertion");

        final Object storedKey = keyContainer.field("key");
        if (storedKey instanceof List)
          key = new OCompositeKey((List<? extends Comparable<?>>) storedKey);
        else if (Boolean.TRUE.equals(keyContainer.field("binary"))) {
          key = OStreamSerializerAnyStreamable.INSTANCE.fromStream((byte[]) storedKey);
        } else
          key = storedKey;
      } catch (IOException ioe) {
        throw OException.wrapException(new OTransactionException("Error during index changes deserialization. "), ioe);
      }
    } else
      key = null;

    final List<ODocument> operations = entry.field("ops");
    if (operations != null) {
      for (final ODocument op : operations) {
        op.setLazyLoad(false);
        final int operation = (Integer) op.rawField("o");
        final OIdentifiable value = op.field("v");

        if (operation == OPERATION.PUT.ordinal())
          putInSnapshot(key, value, snapshot);
        else if (operation == OPERATION.REMOVE.ordinal()) {
          if (value == null)
            removeFromSnapshot(key, snapshot);
          else {
            removeFromSnapshot(key, value, snapshot);
          }
        }
      }
    }
  }

  protected void onIndexEngineChange(int indexId) {

  }

  protected static final class IndexTxSnapshot {
    public Map<Object, Object> indexSnapshot = new HashMap<Object, Object>();
    public boolean             clear         = false;
  }

  private static class IndexTxSnapshotThreadLocal extends ThreadLocal<IndexTxSnapshot> {
    @Override
    protected IndexTxSnapshot initialValue() {
      return new IndexTxSnapshot();
    }
  }
}
