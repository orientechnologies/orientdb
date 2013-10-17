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
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;

import com.orientechnologies.common.collection.OCompositeKey;
import com.orientechnologies.common.concur.lock.OModificationLock;
import com.orientechnologies.common.concur.resource.OSharedResourceAdaptiveExternal;
import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.annotation.ODocumentInstance;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.db.record.ridset.sbtree.OSBTreeIndexRIDContainer;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.exception.OTransactionException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.hashindex.local.cache.ODiskCache;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializer;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerAnyStreamable;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.OStorageEmbedded;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocal;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges.OPERATION;

/**
 * Handles indexing when records change.
 * 
 * @author Luca Garulli
 * 
 */
public abstract class OIndexAbstract<T> extends OSharedResourceAdaptiveExternal implements OIndexInternal<T> {
  protected final OModificationLock modificationLock = new OModificationLock();

  protected static final String     CONFIG_MAP_RID   = "mapRid";
  protected static final String     CONFIG_CLUSTERS  = "clusters";

  private String                    name;
  protected String                  type;
  private String                    algorithm;
  protected String                  valueContainerAlgorithm;

  protected final OIndexEngine<T>   indexEngine;
  private Set<String>               clustersToIndex  = new HashSet<String>();
  private OIndexDefinition          indexDefinition;
  private final String              databaseName;

  @ODocumentInstance
  protected ODocument               configuration;

  private volatile boolean          rebuilding       = false;

  private Thread                    rebuildThread    = null;

  public OIndexAbstract(final String type, String algorithm, final OIndexEngine<T> indexEngine, String valueContainerAlgorithm) {
    super(OGlobalConfiguration.ENVIRONMENT_CONCURRENT.getValueAsBoolean(), OGlobalConfiguration.MVRBTREE_TIMEOUT
        .getValueAsInteger(), true);
    acquireExclusiveLock();
    try {
      databaseName = ODatabaseRecordThreadLocal.INSTANCE.get().getName();
      this.type = type;
      this.indexEngine = indexEngine;
      this.algorithm = algorithm;
      this.valueContainerAlgorithm = valueContainerAlgorithm;

      indexEngine.init();
    } finally {
      releaseExclusiveLock();
    }
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

      if (clustersToIndex != null)
        this.clustersToIndex = new HashSet<String>(clustersToIndex);
      else
        this.clustersToIndex = new HashSet<String>(clustersToIndex);

      indexEngine.create(this.name, indexDefinition, clusterIndexName, valueSerializer, isAutomatic());

      if (rebuild)
        rebuild(progressListener);

      updateConfiguration();
    } catch (Exception e) {
      indexEngine.delete();

      if (e instanceof OIndexException)
        throw (OIndexException) e;

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
        indexEngine.load(rid, name, indexDefinition, isAutomatic());
      } catch (Exception e) {
        if (onCorruptionRepairDatabase(null, "load", "Index will be rebuilt")) {
          if (isAutomatic() && getDatabase().getStorage() instanceof OStorageEmbedded)
            // AUTOMATIC REBUILD IT
            OLogManager.instance().warn(this, "Cannot load index '%s' from storage (rid=%s): rebuilt it from scratch", getName(),
                rid);
          try {
            rebuild();
          } catch (Throwable t) {
            OLogManager.instance().error(this,
                "Cannot rebuild index '%s' from storage (rid=%s) because '" + t + "'. The index will be removed in configuration",
                getName(), rid);
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
  public IndexMetadata loadMetadata(ODocument config) {
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
        if (pos < 0)
          throw new OIndexException("Can not convert from old index model to new one. "
              + "Invalid index name. Dot (.) separator should be present.");
        final String className = indexName.substring(0, pos);
        final String propertyName = indexName.substring(pos + 1);

        final String keyTypeStr = config.field(OIndexInternal.CONFIG_KEYTYPE);
        if (keyTypeStr == null)
          throw new OIndexException("Can not convert from old index model to new one. " + "Index key type is absent.");
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

    final Set<String> clusters = new HashSet<String>((Collection<String>) config.field(CONFIG_CLUSTERS));

    return new IndexMetadata(indexName, loadedIndexDefinition, clusters, type, algorithm, valueContainerAlgorithm);
  }

  public boolean contains(final Object key) {
    checkForRebuild();

    acquireSharedLock();
    try {
      return indexEngine.contains(key);
    } finally {
      releaseSharedLock();
    }
  }

  /**
   * Returns a set of records with key between the range passed as parameter. Range bounds are included.
   * <p/>
   * In case of {@link com.orientechnologies.common.collection.OCompositeKey}s partial keys can be used as values boundaries.
   * 
   * @param iRangeFrom
   *          Starting range
   * @param iRangeTo
   *          Ending range
   * @return a set of records with key between the range passed as parameter. Range bounds are included.
   * @see com.orientechnologies.common.collection.OCompositeKey#compareTo(com.orientechnologies.common.collection.OCompositeKey)
   * @see #getValuesBetween(Object, boolean, Object, boolean)
   */
  public Collection<OIdentifiable> getValuesBetween(final Object iRangeFrom, final Object iRangeTo) {
    checkForRebuild();

    return getValuesBetween(iRangeFrom, true, iRangeTo, true);
  }

  /**
   * Returns a set of documents with key between the range passed as parameter. Range bounds are included.
   * 
   * @param iRangeFrom
   *          Starting range
   * @param iRangeTo
   *          Ending range
   * @see #getEntriesBetween(Object, Object, boolean)
   * @return
   */
  public Collection<ODocument> getEntriesBetween(final Object iRangeFrom, final Object iRangeTo) {
    checkForRebuild();

    return getEntriesBetween(iRangeFrom, iRangeTo, true);
  }

  public Collection<OIdentifiable> getValuesMajor(final Object fromKey, final boolean isInclusive) {
    checkForRebuild();

    return getValuesMajor(fromKey, isInclusive, -1);
  }

  public Collection<OIdentifiable> getValuesMinor(final Object toKey, final boolean isInclusive) {
    checkForRebuild();

    return getValuesMinor(toKey, isInclusive, -1);
  }

  public Collection<ODocument> getEntriesMajor(final Object fromKey, final boolean isInclusive) {
    checkForRebuild();

    return getEntriesMajor(fromKey, isInclusive, -1);
  }

  public Collection<ODocument> getEntriesMinor(final Object toKey, final boolean isInclusive) {
    checkForRebuild();

    return getEntriesMinor(toKey, isInclusive, -1);
  }

  /**
   * Returns a set of records with key between the range passed as parameter.
   * <p/>
   * In case of {@link com.orientechnologies.common.collection.OCompositeKey}s partial keys can be used as values boundaries.
   * 
   * @param iRangeFrom
   *          Starting range
   * @param iFromInclusive
   *          Indicates whether start range boundary is included in result.
   * @param iRangeTo
   *          Ending range
   * @param iToInclusive
   *          Indicates whether end range boundary is included in result.
   * @return Returns a set of records with key between the range passed as parameter.
   * @see com.orientechnologies.common.collection.OCompositeKey#compareTo(com.orientechnologies.common.collection.OCompositeKey)
   */
  public Collection<OIdentifiable> getValuesBetween(final Object iRangeFrom, final boolean iFromInclusive, final Object iRangeTo,
      final boolean iToInclusive) {
    checkForRebuild();

    return getValuesBetween(iRangeFrom, iFromInclusive, iRangeTo, iToInclusive, -1);
  }

  public Collection<ODocument> getEntriesBetween(final Object iRangeFrom, final Object iRangeTo, final boolean iInclusive) {
    checkForRebuild();

    return getEntriesBetween(iRangeFrom, iRangeTo, iInclusive, -1);
  }

  public Collection<OIdentifiable> getValues(final Collection<?> iKeys) {
    checkForRebuild();

    return getValues(iKeys, -1);
  }

  public Collection<ODocument> getEntries(final Collection<?> iKeys) {
    checkForRebuild();

    return getEntries(iKeys, -1);
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
        rebuildThread = Thread.currentThread();
        rebuilding = true;

        try {
          indexEngine.clear();
        } catch (Exception e) {
          // IGNORE EXCEPTION: IF THE REBUILD WAS LAUNCHED IN CASE OF RID INVALID CLEAR ALWAYS GOES IN ERROR
        }

        int documentNum = 0;
        long documentTotal = 0;

        for (final String cluster : clustersToIndex)
          documentTotal += getDatabase().countClusterElements(cluster);

        if (iProgressListener != null)
          iProgressListener.onBegin(this, documentTotal);

        for (final String clusterName : clustersToIndex)
          try {
            for (final ORecord<?> record : getDatabase().browseCluster(clusterName)) {
              if (Thread.interrupted())
                throw new OCommandExecutionException("The index rebuild has been interrupted");

              if (record instanceof ODocument) {
                final ODocument doc = (ODocument) record;

                if (indexDefinition == null)
                  throw new OConfigurationException("Index '" + name + "' cannot be rebuilt because has no a valid definition ("
                      + indexDefinition + ")");

                final Object fieldValue = indexDefinition.getDocumentValueToIndex(doc);

                if (fieldValue != null) {
                  try {
                    if (fieldValue instanceof Collection) {
                      for (final Object fieldValueItem : (Collection<?>) fieldValue) {
                        put(fieldValueItem, doc);
                      }
                    } else
                      put(fieldValue, doc);
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

              if (iProgressListener != null)
                iProgressListener.onProgress(this, documentNum, documentNum * 100f / documentTotal);
            }
          } catch (NoSuchElementException e) {
            // END OF CLUSTER REACHED, IGNORE IT
          }

        flush();
        unload();

        if (iProgressListener != null)
          iProgressListener.onCompletition(this, true);

      } catch (final Exception e) {
        if (iProgressListener != null)
          iProgressListener.onCompletition(this, false);

        try {
          indexEngine.clear();
        } catch (Exception e2) {
          // IGNORE EXCEPTION: IF THE REBUILD WAS LAUNCHED IN CASE OF RID INVALID CLEAR ALWAYS GOES IN ERROR
        }

        throw new OIndexException("Error on rebuilding the index for clusters: " + clustersToIndex, e);

      } finally {
        rebuilding = false;
        rebuildThread = null;

        if (intentInstalled)
          getDatabase().declareIntent(null);

        releaseExclusiveLock();
      }
    } finally {
      modificationLock.releaseModificationLock();
    }

    return documentIndexed;
  }

  public boolean remove(final Object key, final OIdentifiable value) {
    checkForRebuild();

    modificationLock.requestModificationLock();
    try {
      return remove(key);
    } finally {
      modificationLock.releaseModificationLock();
    }

  }

  @Override
  public int remove(OIdentifiable iRID) {
    checkForRebuild();

    modificationLock.requestModificationLock();

    try {
      acquireExclusiveLock();
      try {
        return indexEngine.removeValue(iRID, null);
      } finally {
        releaseExclusiveLock();
      }
    } finally {
      modificationLock.releaseModificationLock();
    }
  }

  public boolean remove(final Object key) {
    checkForRebuild();

    modificationLock.requestModificationLock();

    try {
      acquireSharedLock();
      try {
        return indexEngine.remove(key);
      } finally {
        releaseSharedLock();
      }
    } finally {
      modificationLock.releaseModificationLock();
    }
  }

  public OIndex<T> clear() {
    checkForRebuild();

    modificationLock.requestModificationLock();

    try {
      acquireSharedLock();
      try {
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
        indexEngine.delete();

        if (valueContainerAlgorithm.equals(ODefaultIndexFactory.SBTREEBONSAI_VALUE_CONTAINER)) {
          final OStorage storage = getDatabase().getStorage();
          if (storage instanceof OStorageLocal) {
            final ODiskCache diskCache = ((OStorageLocal) storage).getDiskCache();
            try {
              final String fileName = getName() + OSBTreeIndexRIDContainer.INDEX_FILE_EXTENSION;
              if (diskCache.exists(fileName)) {
                final long fileId = diskCache.openFile(fileName);
                diskCache.deleteFile(fileId);
              }
            } catch (IOException e) {
              OLogManager.instance().error(this, "Can't delete file for value containers", e);
            }
          }
        }

        return this;

      } finally {
        releaseExclusiveLock();
      }
    } finally {
      modificationLock.releaseModificationLock();
    }
  }

  public Iterator<Entry<Object, T>> iterator() {
    checkForRebuild();

    acquireSharedLock();
    try {
      return indexEngine.iterator();
    } finally {
      releaseSharedLock();
    }
  }

  public Iterator<Entry<Object, T>> inverseIterator() {
    checkForRebuild();

    acquireSharedLock();
    try {
      return indexEngine.inverseIterator();
    } finally {
      releaseSharedLock();
    }
  }

  public Iterable<Object> keys() {
    checkForRebuild();

    acquireSharedLock();
    try {
      return indexEngine.keys();
    } finally {
      releaseSharedLock();
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

  public OIndexAbstract<T> addCluster(final String iClusterName) {
    acquireExclusiveLock();
    try {
      if (clustersToIndex.add(iClusterName))
        updateConfiguration();
      return this;
    } finally {
      releaseExclusiveLock();
    }
  }

  public OIndexAbstract<T> removeCluster(String iClusterName) {
    acquireExclusiveLock();
    try {
      if (clustersToIndex.remove(iClusterName))
        updateConfiguration();
      return this;
    } finally {
      releaseExclusiveLock();
    }
  }

  public void checkEntry(final OIdentifiable iRecord, final Object iKey) {
  }

  public void unload() {
    acquireSharedLock();
    try {
      indexEngine.unload();
    } finally {
      releaseSharedLock();
    }
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
          if (!indexDefDocument.hasOwners())
            indexDefDocument.addOwner(configuration);

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
  public void commit(final ODocument document) {
    checkForRebuild();

    if (document == null)
      return;

    acquireExclusiveLock();
    try {
      indexEngine.startTransaction();

      final Boolean clearAll = document.field("clear");
      if (clearAll != null && clearAll)
        clear();

      final Collection<ODocument> entries = document.field("entries");

      for (final ODocument entry : entries) {
        final String serializedKey = OStringSerializerHelper.decode((String) entry.field("k"));
        final Object key;
        try {
          if (serializedKey.equals("*"))
            key = "*";
          else {
            final ODocument keyContainer = new ODocument();
            keyContainer.setLazyLoad(false);

            keyContainer.fromString(serializedKey);

            final Object storedKey = keyContainer.field("key");
            if (storedKey instanceof List)
              key = new OCompositeKey((List<? extends Comparable<?>>) storedKey);
            else if (Boolean.TRUE.equals(keyContainer.field("binary"))) {
              key = OStreamSerializerAnyStreamable.INSTANCE.fromStream((byte[]) storedKey);
            } else
              key = storedKey;
          }
        } catch (IOException ioe) {
          throw new OTransactionException("Error during index changes deserialization. ", ioe);
        }

        final List<ODocument> operations = (List<ODocument>) entry.field("ops");
        if (operations != null) {
          for (final ODocument op : operations) {
            final int operation = (Integer) op.rawField("o");
            final OIdentifiable value = op.field("v", OType.LINK);

            if (operation == OPERATION.PUT.ordinal())
              put(key, value);
            else if (operation == OPERATION.REMOVE.ordinal()) {
              if (key.equals("*"))
                remove(value);
              else if (value == null)
                remove(key);
              else {
                remove(key, value);
              }

            }
          }
        }
      }

    } finally {
      indexEngine.stopTransaction();
      releaseExclusiveLock();
    }
  }

  public ODocument getConfiguration() {
    acquireSharedLock();
    try {
      return configuration;
    } finally {
      releaseSharedLock();
    }
  }

  public boolean isAutomatic() {
    acquireSharedLock();
    try {
      return indexDefinition != null && indexDefinition.getClassName() != null;
    } finally {
      releaseSharedLock();
    }
  }

  public void onCreate(final ODatabase iDatabase) {
  }

  public void onDelete(final ODatabase iDatabase) {
  }

  public void onOpen(final ODatabase iDatabase) {
  }

  public void onBeforeTxBegin(final ODatabase iDatabase) {
    acquireSharedLock();
    try {
      indexEngine.beforeTxBegin();
    } finally {
      releaseSharedLock();
    }
  }

  public void onBeforeTxRollback(final ODatabase iDatabase) {
  }

  public boolean onCorruptionRepairDatabase(final ODatabase database, final String reason, String whatWillbeFixed) {
    if (reason.equals("load"))
      return true;
    return false;
  }

  public void onAfterTxRollback(final ODatabase database) {
    acquireSharedLock();
    try {
      indexEngine.afterTxRollback();
    } finally {
      releaseSharedLock();
    }
  }

  public void onBeforeTxCommit(final ODatabase database) {
  }

  public void onAfterTxCommit(final ODatabase iDatabase) {
    acquireSharedLock();
    try {
      indexEngine.afterTxCommit();
    } finally {
      releaseSharedLock();
    }
  }

  public void onClose(final ODatabase iDatabase) {
    if (isRebuiding())
      return;

    acquireSharedLock();
    try {
      indexEngine.closeDb();
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

      indexDefinition = new OSimpleKeyIndexDefinition(type);
      updateConfiguration();
    }
  }

  protected ODatabaseRecord getDatabase() {
    return ODatabaseRecordThreadLocal.INSTANCE.get();
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

  public OIndexDefinition getDefinition() {
    acquireSharedLock();
    try {
      return indexDefinition;
    } finally {
      releaseSharedLock();
    }
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
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    final OIndexAbstract<?> that = (OIndexAbstract<?>) o;

    if (!name.equals(that.name))
      return false;

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

  protected void checkForRebuild() {
    if (rebuilding && !Thread.currentThread().equals(rebuildThread)) {
      throw new OIndexException("Index " + name + " is rebuilding now and can not be used.");
    }
  }
}
