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

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.orientechnologies.common.concur.lock.OModificationLock;
import com.orientechnologies.common.concur.lock.ONewLockManager;
import com.orientechnologies.common.concur.resource.OSharedResourceAdaptiveExternal;
import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.annotation.ODocumentInstance;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OIndexRIDContainer;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.exception.OTransactionException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.hashindex.local.cache.ODiskCache;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerSchemaAware2CSV;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializer;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerAnyStreamable;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges.OPERATION;

/**
 * Handles indexing when records change.
 * 
 * @author Luca Garulli
 * 
 */
public abstract class OIndexAbstract<T> implements OIndexInternal<T> {
  protected static final String              CONFIG_MAP_RID   = "mapRid";
  protected static final String              CONFIG_CLUSTERS  = "clusters";
  protected final OModificationLock          modificationLock = new OModificationLock();
  protected final OIndexEngine<T>            indexEngine;
  private final String                       databaseName;
  protected String                           type;
  protected String                           valueContainerAlgorithm;
  protected final ONewLockManager<Object>    keyLockManager   = new ONewLockManager<Object>();
  @ODocumentInstance
  protected ODocument                        configuration;
  protected ODocument                        metadata;
  private String                             name;
  private String                             algorithm;
  private Set<String>                        clustersToIndex  = new HashSet<String>();

  private volatile OIndexDefinition          indexDefinition;
  private volatile boolean                   rebuilding       = false;

  private Thread                             rebuildThread    = null;

  private final ThreadLocal<IndexTxSnapshot> txSnapshot       = new ThreadLocal<IndexTxSnapshot>() {
                                                                @Override
                                                                protected IndexTxSnapshot initialValue() {
                                                                  return new IndexTxSnapshot();
                                                                }
                                                              };
  private final ReadWriteLock                rwLock           = new ReentrantReadWriteLock();

  protected static final class RemovedValue {
    public static final RemovedValue INSTANCE = new RemovedValue();
  }

  protected static final class IndexTxSnapshot {
    public Map<Object, Object> indexSnapshot = new HashMap<Object, Object>();
    public boolean             clear         = false;
  }

  public OIndexAbstract(final String type, String algorithm, final OIndexEngine<T> indexEngine, String valueContainerAlgorithm,
      ODocument metadata) {
    acquireExclusiveLock();
    try {
      databaseName = ODatabaseRecordThreadLocal.INSTANCE.get().getName();
      this.type = type;
      this.indexEngine = indexEngine;
      this.algorithm = algorithm;
      this.metadata = metadata;
      this.valueContainerAlgorithm = valueContainerAlgorithm;

      indexEngine.init();
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
        throw new OIndexException("Error during deserialization of index definition", e);
      } catch (final NoSuchMethodException e) {
        throw new OIndexException("Error during deserialization of index definition", e);
      } catch (final InvocationTargetException e) {
        throw new OIndexException("Error during deserialization of index definition", e);
      } catch (final InstantiationException e) {
        throw new OIndexException("Error during deserialization of index definition", e);
      } catch (final IllegalAccessException e) {
        throw new OIndexException("Error during deserialization of index definition", e);
      }
    } else {
      // @COMPATIBILITY 1.0rc6 new index model was implemented
      final Boolean isAutomatic = config.field(OIndexInternal.CONFIG_AUTOMATIC);
      if (Boolean.TRUE.equals(isAutomatic)) {
        final int pos = indexName.lastIndexOf('.');
        if (pos < 0) {
            throw new OIndexException("Can not convert from old index model to new one. "
                    + "Invalid index name. Dot (.) separator should be present.");
        }
        final String className = indexName.substring(0, pos);
        final String propertyName = indexName.substring(pos + 1);

        final String keyTypeStr = config.field(OIndexInternal.CONFIG_KEYTYPE);
        if (keyTypeStr == null) {
            throw new OIndexException("Can not convert from old index model to new one. " + "Index key type is absent.");
        }
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

    final Set<String> clusters = new HashSet<String>((Collection<String>) config.field(CONFIG_CLUSTERS, OType.EMBEDDEDSET));

    return new IndexMetadata(indexName, loadedIndexDefinition, clusters, type, algorithm, valueContainerAlgorithm);
  }

  public void flush() {
    acquireSharedLock();
    try {
      indexEngine.flush();
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public boolean hasRangeQuerySupport() {
    acquireSharedLock();
    try {
      return indexEngine.hasRangeQuerySupport();
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
   */
  public OIndexInternal<?> create(final String name, final OIndexDefinition indexDefinition, final String clusterIndexName,
      final Set<String> clustersToIndex, boolean rebuild, final OProgressListener progressListener,
      final OStreamSerializer valueSerializer) {
    acquireExclusiveLock();
    try {
      this.name = name;
      configuration = new ODocument();

      this.indexDefinition = indexDefinition;

      if (clustersToIndex != null) {
          this.clustersToIndex = new HashSet<String>(clustersToIndex);
      } else {
          this.clustersToIndex = new HashSet<String>();
      }

      markStorageDirty();
      // do not remove this, it is needed to remove index garbage if such one exists
      try {
        indexEngine.deleteWithoutLoad(name);
        removeValuesContainer();
      } catch (Exception e) {
        OLogManager.instance().error(this, "Error during deletion of index %s .", name);
      }

      indexEngine.create(this.name, indexDefinition, clusterIndexName, valueSerializer, isAutomatic());

      if (rebuild) {
          rebuild(progressListener);
      }

      updateConfiguration();
    } catch (Exception e) {
      OLogManager.instance().error(this, "Exception during index %s creation.", e, name);

      try {
        indexEngine.delete();
      } catch (Exception ex) {
        OLogManager.instance().error(this, "Exception during index %s deletion.", ex, name);
      }

      if (e instanceof OIndexException) {
          throw (OIndexException) e;
      }

      throw new OIndexException("Cannot create the index '" + name + "'", e);

    } finally {
      releaseExclusiveLock();
    }

    return this;
  }

  public boolean loadFromConfiguration(final ODocument config) {
    acquireExclusiveLock();
    try {
      configuration = config;
      clustersToIndex.clear();

      IndexMetadata indexMetadata = loadMetadata(configuration);
      name = indexMetadata.getName();
      indexDefinition = indexMetadata.getIndexDefinition();
      clustersToIndex.addAll(indexMetadata.getClustersToIndex());
      algorithm = indexMetadata.getAlgorithm();
      valueContainerAlgorithm = indexMetadata.getValueContainerAlgorithm();

      final ORID rid = config.field(CONFIG_MAP_RID, ORID.class);

      try {
        indexEngine.load(rid, name, indexDefinition, determineValueSerializer(), isAutomatic());
      } catch (Exception e) {
        OLogManager.instance().error(this, "Error during load of index %s .", e, name != null ? name : "null");

        if (isAutomatic() && getStorage() instanceof OAbstractPaginatedStorage) {
          // AUTOMATIC REBUILD IT
          OLogManager.instance()
              .warn(this, "Cannot load index '%s' from storage (rid=%s): rebuilt it from scratch", getName(), rid);
          try {
            rebuild();
          } catch (Throwable t) {
            OLogManager.instance().error(this,
                "Cannot rebuild index '%s' from storage (rid=%s) because '" + t + "'. The index will be removed in configuration",
                e, getName(), rid);
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
  public IndexMetadata loadMetadata(final ODocument config) {
    return loadMetadataInternal(config, type, algorithm, valueContainerAlgorithm);
  }

  public boolean contains(Object key) {
    checkForRebuild();

    key = getCollatingValue(key);

    acquireSharedLock();
    try {
      return indexEngine.contains(key);
    } finally {
      releaseSharedLock();
    }
  }

  public ORID getIdentity() {
    acquireSharedLock();
    try {
      return indexEngine.getIdentity();
    } finally {
      releaseSharedLock();
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
    acquireSharedLock();
    try {
      indexEngine.close();
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public Object getFirstKey() {
    acquireSharedLock();
    try {
      return indexEngine.getFirstKey();
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public Object getLastKey() {
    acquireSharedLock();
    try {
      return indexEngine.getLastKey();
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

    modificationLock.requestModificationLock();
    try {
      acquireExclusiveLock();
      try {
        markStorageDirty();

        rebuildThread = Thread.currentThread();
        rebuilding = true;

        try {
          indexEngine.deleteWithoutLoad(name);
        } catch (Exception e) {
          OLogManager.instance().error(this, "Error during index %s delete .", name);
        }

        removeValuesContainer();

        indexEngine.create(name, indexDefinition, getDatabase().getMetadata().getIndexManager().getDefaultClusterName(),
            determineValueSerializer(), isAutomatic());

        long documentNum = 0;
        long documentTotal = 0;

        for (final String cluster : clustersToIndex) {
            documentTotal += getDatabase().countClusterElements(cluster);
        }

        if (iProgressListener != null) {
            iProgressListener.onBegin(this, documentTotal, true);
        }

        // INDEX ALL CLUSTERS
        for (final String clusterName : clustersToIndex) {
          final long[] metrics = indexCluster(clusterName, iProgressListener, documentNum, documentIndexed, documentTotal);
          documentNum += metrics[0];
          documentIndexed += metrics[1];
        }

        if (iProgressListener != null) {
            iProgressListener.onCompletition(this, true);
        }

      } catch (final Exception e) {
        if (iProgressListener != null) {
            iProgressListener.onCompletition(this, false);
        }

        try {
          indexEngine.clear();
        } catch (Exception e2) {
          // IGNORE EXCEPTION: IF THE REBUILD WAS LAUNCHED IN CASE OF RID INVALID CLEAR ALWAYS GOES IN ERROR
        }

        throw new OIndexException("Error on rebuilding the index for clusters: " + clustersToIndex, e);

      } finally {
        rebuilding = false;
        rebuildThread = null;

        if (intentInstalled) {
            getDatabase().declareIntent(null);
        }

        releaseExclusiveLock();
      }
    } finally {
      modificationLock.releaseModificationLock();
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

    if (txIsActive) {
        keyLockManager.acquireSharedLock(key);
    }
    try {
      modificationLock.requestModificationLock();
      try {
        acquireSharedLock();
        try {
          markStorageDirty();
          return indexEngine.remove(key);
        } finally {
          releaseSharedLock();
        }
      } finally {
        modificationLock.releaseModificationLock();
      }
    } finally {
      if (txIsActive) {
          keyLockManager.releaseSharedLock(key);
      }
    }
  }

  @Override
  public void lockKeysForUpdate(Object... key) {
    keyLockManager.acquireExclusiveLocksInBatch(key);
  }

  @Override
  public void lockKeysForUpdate(Collection<Object> keys) {
    keyLockManager.acquireExclusiveLocksInBatch(keys);
  }

  @Override
  public void releaseKeysForUpdate(Object... key) {
    if (key == null) {
        return;
    }

    for (Object k : key) {
      keyLockManager.releaseExclusiveLock(k);
    }
  }

  @Override
  public void releaseKeysForUpdate(Collection<Object> keys) {
    if (keys == null) {
        return;
    }

    for (Object k : keys) {
      keyLockManager.releaseExclusiveLock(k);
    }
  }

  public OIndex<T> clear() {
    checkForRebuild();

    modificationLock.requestModificationLock();

    try {
      acquireSharedLock();
      try {
        markStorageDirty();
        indexEngine.clear();
        return this;
      } finally {
        releaseSharedLock();
      }
    } finally {
      modificationLock.releaseModificationLock();
    }
  }

  public OIndexInternal<T> delete() {
    modificationLock.requestModificationLock();

    try {
      acquireExclusiveLock();

      try {
        markStorageDirty();
        indexEngine.delete();

        // REMOVE THE INDEX ALSO FROM CLASS MAP
        if (getDatabase().getMetadata() != null) {
            getDatabase().getMetadata().getIndexManager().removeClassPropertyIndex(this);
        }

        removeValuesContainer();

        return this;

      } finally {
        releaseExclusiveLock();
      }
    } finally {
      modificationLock.releaseModificationLock();
    }
  }

  @Override
  public void deleteWithoutIndexLoad(String indexName) {
    modificationLock.requestModificationLock();
    try {
      acquireExclusiveLock();
      try {
        markStorageDirty();
        indexEngine.deleteWithoutLoad(indexName);
      } finally {
        releaseExclusiveLock();
      }
    } finally {
      modificationLock.releaseModificationLock();
    }
  }

  public String getName() {
    return name;
  }

  public String getType() {
    return type;
  }

  @Override
  public String getAlgorithm() {
    return algorithm;
  }

  @Override
  public String toString() {
    return name;
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

  public ODocument updateConfiguration() {
    acquireExclusiveLock();
    try {

      configuration.setInternalStatus(ORecordElement.STATUS.UNMARSHALLING);

      try {
        configuration.field(OIndexInternal.CONFIG_TYPE, type);
        configuration.field(OIndexInternal.CONFIG_NAME, name);

        if (indexDefinition != null) {
          final ODocument indexDefDocument = indexDefinition.toStream();
          if (!indexDefDocument.hasOwners()) {
              ODocumentInternal.addOwner(indexDefDocument, configuration);
          }

          configuration.field(OIndexInternal.INDEX_DEFINITION, indexDefDocument, OType.EMBEDDED);
          configuration.field(OIndexInternal.INDEX_DEFINITION_CLASS, indexDefinition.getClass().getName());
        } else {
          configuration.removeField(OIndexInternal.INDEX_DEFINITION);
          configuration.removeField(OIndexInternal.INDEX_DEFINITION_CLASS);
        }

        configuration.field(CONFIG_CLUSTERS, clustersToIndex, OType.EMBEDDEDSET);
        configuration.field(CONFIG_MAP_RID, indexEngine.getIdentity());
        configuration.field(ALGORITHM, algorithm);
        configuration.field(VALUE_CONTAINER_ALGORITHM, valueContainerAlgorithm);

      } finally {
        configuration.setInternalStatus(ORecordElement.STATUS.LOADED);
      }

    } finally {
      releaseExclusiveLock();
    }
    return configuration;
  }

  @SuppressWarnings("unchecked")
  public void addTxOperation(final ODocument operationDocument) {
    checkForRebuild();

    if (operationDocument == null) {
        return;
    }

    acquireExclusiveLock();
    try {
      final IndexTxSnapshot indexTxSnapshot = txSnapshot.get();

      final Boolean clearAll = operationDocument.field("clear");
      if (clearAll != null && clearAll) {
        indexTxSnapshot.clear = true;
        indexTxSnapshot.indexSnapshot.clear();
      }

      final Collection<ODocument> entries = operationDocument.field("entries");
      final Map<Object, Object> snapshot = indexTxSnapshot.indexSnapshot;
      for (final ODocument entry : entries) {
          applyIndexTxEntry(snapshot, entry);
      }

      final ODocument nullIndexEntry = operationDocument.field("nullEntries");
      applyIndexTxEntry(snapshot, nullIndexEntry);
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public void commit() {
    acquireExclusiveLock();
    try {
      final IndexTxSnapshot indexTxSnapshot = txSnapshot.get();
      if (indexTxSnapshot.clear) {
          clear();
      }

      commitSnapshot(indexTxSnapshot.indexSnapshot);
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public void preCommit() {
    txSnapshot.set(new IndexTxSnapshot());
  }

  @Override
  public void postCommit() {
    txSnapshot.set(new IndexTxSnapshot());
  }

  public ODocument getConfiguration() {
    acquireSharedLock();
    try {
      return configuration;
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public ODocument getMetadata() {
    return getConfiguration().field("metadata", OType.EMBEDDED);
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
      if (indexDefinition == null) {
          return null;
      }

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
      return indexEngine.keyCursor();
    } finally {
      releaseSharedLock();
    }
  }

  public OIndexDefinition getDefinition() {
    return indexDefinition;
  }

  public void freeze(boolean throwException) {
    modificationLock.prohibitModifications(throwException);
  }

  public void release() {
    modificationLock.allowModifications();
  }

  public void acquireModificationLock() {
    modificationLock.requestModificationLock();
  }

  public void releaseModificationLock() {
    try {
      modificationLock.releaseModificationLock();
    } catch (IllegalMonitorStateException e) {
      OLogManager.instance().error(this, "Error on releasing index lock against %s", e, getName());
      throw e;
    }
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
        return true;
    }
    if (o == null || getClass() != o.getClass()) {
        return false;
    }

    final OIndexAbstract<?> that = (OIndexAbstract<?>) o;

    if (!name.equals(that.name)) {
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return name.hashCode();
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public boolean isRebuiding() {
    return rebuilding;
  }

  protected void startStorageAtomicOperation() {
    try {
      getStorage().startAtomicOperation();
    } catch (IOException e) {
      throw new OIndexException("Error during start of atomic operation", e);
    }
  }

  protected void commitStorageAtomicOperation() {
    try {
      getStorage().commitAtomicOperation();
    } catch (IOException e) {
      throw new OIndexException("Error during commit of atomic operation", e);
    }
  }

  protected void rollbackStorageAtomicOperation() {
    try {
      getStorage().rollbackAtomicOperation();
    } catch (IOException e) {
      throw new OIndexException("Error during rollback of atomic operation", e);
    }
  }

  protected void markStorageDirty() {
    try {
      getStorage().markDirty();
    } catch (IOException e) {
      throw new OIndexException("Can not mark storage as dirty", e);
    }
  }

  protected abstract OStreamSerializer determineValueSerializer();

  protected void populateIndex(ODocument doc, Object fieldValue) {
    if (fieldValue instanceof Collection) {
      for (final Object fieldValueItem : (Collection<?>) fieldValue) {
        put(fieldValueItem, doc);
      }
    } else {
        put(fieldValue, doc);
    }
  }

  protected Object getCollatingValue(final Object key) {
    if (key != null && getDefinition() != null) {
        return getDefinition().getCollate().transform(key);
    }
    return key;
  }

  protected abstract void commitSnapshot(Map<Object, Object> snapshot);

  protected abstract void putInSnapshot(Object key, OIdentifiable value, Map<Object, Object> snapshot);

  protected abstract void removeFromSnapshot(Object key, OIdentifiable value, Map<Object, Object> snapshot);

  @Override
  public int compareTo(OIndex<T> index) {
    final String name = index.getName();
    return this.name.compareTo(name);
  }

  protected void removeFromSnapshot(Object key, Map<Object, Object> snapshot) {
    key = getCollatingValue(key);
    snapshot.put(key, RemovedValue.INSTANCE);
  }

  protected void checkForKeyType(final Object iKey) {
    if (indexDefinition == null) {
      // RECOGNIZE THE KEY TYPE AT RUN-TIME

      final OType type = OType.getTypeByClass(iKey.getClass());
      if (type == null) {
          return;
      }

      indexDefinition = new OSimpleKeyIndexDefinition(type);
      updateConfiguration();
    }
  }

  protected ODatabaseDocumentInternal getDatabase() {
    return ODatabaseRecordThreadLocal.INSTANCE.get();
  }

  protected void checkForRebuild() {
    if (rebuilding && !Thread.currentThread().equals(rebuildThread)) {
      throw new OIndexException("Index " + name + " is rebuilding now and can not be used.");
    }
  }

  protected long[] indexCluster(final String clusterName, final OProgressListener iProgressListener, long documentNum,
      long documentIndexed, long documentTotal) {
    try {
      for (final ORecord record : getDatabase().browseCluster(clusterName)) {
        if (Thread.interrupted()) {
            throw new OCommandExecutionException("The index rebuild has been interrupted");
        }

        if (record instanceof ODocument) {
          final ODocument doc = (ODocument) record;

          if (indexDefinition == null) {
              throw new OConfigurationException("Index '" + name + "' cannot be rebuilt because has no a valid definition ("
                      + indexDefinition + ")");
          }

          final Object fieldValue = indexDefinition.getDocumentValueToIndex(doc);

          if (fieldValue != null) {
            try {
              populateIndex(doc, fieldValue);
            } catch (OIndexException e) {
              OLogManager.instance().error(
                  this,
                  "Exception during index rebuild. Exception was caused by following key/ value pair - key %s, value %s."
                      + " Rebuild will continue from this point.", e, fieldValue, doc.getIdentity());
            }

            ++documentIndexed;
          }
        }
        documentNum++;

        if (iProgressListener != null) {
            iProgressListener.onProgress(this, documentNum, (float) (documentNum * 100.0 / documentTotal));
        }
      }
    } catch (NoSuchElementException e) {
      // END OF CLUSTER REACHED, IGNORE IT
    }

    return new long[] { documentNum, documentIndexed };
  }

  private OAbstractPaginatedStorage getStorage() {
    return ((OAbstractPaginatedStorage) getDatabase().getStorage().getUnderlying());
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
    if (valueContainerAlgorithm.equals(ODefaultIndexFactory.SBTREEBONSAI_VALUE_CONTAINER)) {
      final OStorage storage = getStorage();
      if (storage instanceof OAbstractPaginatedStorage) {
        final ODiskCache diskCache = ((OAbstractPaginatedStorage) storage).getDiskCache();
        try {
          final String fileName = getName() + OIndexRIDContainer.INDEX_FILE_EXTENSION;
          if (diskCache.exists(fileName)) {
            final long fileId = diskCache.openFile(fileName);
            diskCache.deleteFile(fileId);
          }
        } catch (IOException e) {
          OLogManager.instance().error(this, "Can't delete file for value containers", e);
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

          ORecordSerializerSchemaAware2CSV.INSTANCE.fromString(serializedKey, keyContainer, null);
        } else if (serKey instanceof ODocument) {
          keyContainer = (ODocument) serKey;
        }
        final Object storedKey = keyContainer.field("key");
        if (storedKey instanceof List) {
            key = new OCompositeKey((List<? extends Comparable<?>>) storedKey);
        } else if (Boolean.TRUE.equals(keyContainer.field("binary"))) {
          key = OStreamSerializerAnyStreamable.INSTANCE.fromStream((byte[]) storedKey);
        } else {
            key = storedKey;
        }
      } catch (IOException ioe) {
        throw new OTransactionException("Error during index changes deserialization. ", ioe);
      }
    } else {
        key = null;
    }

    final List<ODocument> operations = entry.field("ops");
    if (operations != null) {
      for (final ODocument op : operations) {
        op.setLazyLoad(false);
        final int operation = (Integer) op.rawField("o");
        final OIdentifiable value = op.field("v");

        if (operation == OPERATION.PUT.ordinal()) {
            putInSnapshot(key, value, snapshot);
        } else if (operation == OPERATION.REMOVE.ordinal()) {
          if (value == null) {
              removeFromSnapshot(key, snapshot);
            } else {
            removeFromSnapshot(key, value, snapshot);
          }

        }
      }
    }
  }
}
