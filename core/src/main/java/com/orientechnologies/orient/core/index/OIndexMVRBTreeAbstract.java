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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;

import com.orientechnologies.common.collection.OCompositeKey;
import com.orientechnologies.common.collection.OMVRBTree;
import com.orientechnologies.common.concur.lock.OModificationLock;
import com.orientechnologies.common.concur.resource.OSharedResourceAdaptiveExternal;
import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.profiler.OProfiler.METRIC_TYPE;
import com.orientechnologies.common.profiler.OProfiler.OProfilerHookValue;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.annotation.ODocumentInstance;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.exception.OTransactionException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.memory.OMemoryWatchDog.Listener;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.profiler.OJVMProfiler;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializerFactory;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.index.OCompositeKeySerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.index.OSimpleKeySerializer;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializer;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerAnyStreamable;
import com.orientechnologies.orient.core.storage.OStorageEmbedded;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges.OPERATION;
import com.orientechnologies.orient.core.type.tree.OMVRBTreeDatabaseLazySave;
import com.orientechnologies.orient.core.type.tree.provider.OMVRBTreeProviderAbstract;

/**
 * Handles indexing when records change.
 * 
 * @author Luca Garulli
 * 
 */
public abstract class OIndexMVRBTreeAbstract<T> extends OSharedResourceAdaptiveExternal implements OIndexInternal<T> {
  protected final OModificationLock              modificationLock = new OModificationLock();

  protected static final String                  CONFIG_MAP_RID   = "mapRid";
  protected static final String                  CONFIG_CLUSTERS  = "clusters";
  protected String                               name;
  protected String                               type;
  protected OMVRBTreeDatabaseLazySave<Object, T> map;
  protected Set<String>                          clustersToIndex  = new LinkedHashSet<String>();
  protected OIndexDefinition                     indexDefinition;
  protected final String                         databaseName;
  protected int                                  maxUpdatesBeforeSave;

  @ODocumentInstance
  protected ODocument                            configuration;
  private final Listener                         watchDog;
  private volatile boolean                       rebuilding       = false;

  private Thread                                 rebuildThread    = null;

  public OIndexMVRBTreeAbstract(final String type) {
    super(OGlobalConfiguration.ENVIRONMENT_CONCURRENT.getValueAsBoolean(), OGlobalConfiguration.MVRBTREE_TIMEOUT
        .getValueAsInteger(), true);

    databaseName = ODatabaseRecordThreadLocal.INSTANCE.get().getName();

    this.type = type;
    watchDog = new Listener() {
      public void memoryUsageLow(final long iFreeMemory, final long iFreeMemoryPercentage) {
        map.setOptimization(iFreeMemoryPercentage < 10 ? 2 : 1);
      }
    };
  }

  public void flush() {
    lazySave();
  }

  /**
   * Creates the index.
   * 
   * @param iDatabase
   *          Current Database instance
   * @param iClusterIndexName
   *          Cluster name where to place the TreeMap
   * @param rebuild
   * @param iProgressListener
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public OIndexInternal<?> create(final String iName, final OIndexDefinition iIndexDefinition, final ODatabaseRecord iDatabase,
      final String iClusterIndexName, final int[] iClusterIdsToIndex, boolean rebuild, final OProgressListener iProgressListener,
      final OStreamSerializer iValueSerializer) {
    acquireExclusiveLock();
    try {

      name = iName;
      configuration = new ODocument();

      indexDefinition = iIndexDefinition;
      maxUpdatesBeforeSave = lazyUpdates();

      if (iClusterIdsToIndex != null)
        for (final int id : iClusterIdsToIndex)
          clustersToIndex.add(iDatabase.getClusterNameById(id));

      if (indexDefinition != null) {
        if (indexDefinition instanceof ORuntimeKeyIndexDefinition) {
          map = new OMVRBTreeDatabaseLazySave<Object, T>(iClusterIndexName,
              ((ORuntimeKeyIndexDefinition) indexDefinition).getSerializer(), iValueSerializer, 1, maxUpdatesBeforeSave);
        } else {
          final OBinarySerializer<?> keySerializer;
          if (indexDefinition.getTypes().length > 1) {
            keySerializer = OCompositeKeySerializer.INSTANCE;
          } else {
            keySerializer = OBinarySerializerFactory.INSTANCE.getObjectSerializer(indexDefinition.getTypes()[0]);
          }
          map = new OMVRBTreeDatabaseLazySave<Object, T>(iClusterIndexName, (OBinarySerializer<Object>) keySerializer,
              iValueSerializer, indexDefinition.getTypes().length, maxUpdatesBeforeSave);
        }
      } else
        map = new OMVRBTreeDatabaseLazySave<Object, T>(iClusterIndexName, new OSimpleKeySerializer(), iValueSerializer, 1,
            maxUpdatesBeforeSave);

      installHooks(iDatabase);

      if (rebuild)
        rebuild(iProgressListener);

      updateConfiguration();
    } catch (Exception e) {
      if (map != null)
        map.delete();
      if (e instanceof OIndexException)
        throw (OIndexException) e;

      throw new OIndexException("Cannot create the index '" + iName + "'", e);

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
      maxUpdatesBeforeSave = lazyUpdates();

      IndexMetadata indexMetadata = loadMetadata(configuration);
      name = indexMetadata.getName();
      indexDefinition = indexMetadata.getIndexDefinition();
      clustersToIndex.addAll(indexMetadata.getClustersToIndex());

      final ORID rid = config.field(CONFIG_MAP_RID, ORID.class);
      map = new OMVRBTreeDatabaseLazySave<Object, T>(getDatabase(), rid, maxUpdatesBeforeSave);
      try {
        map.load();
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

      installHooks(config.getDatabase());

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

    return new IndexMetadata(indexName, loadedIndexDefinition, clusters, type);
  }

  public boolean contains(final Object iKey) {
    checkForRebuild();

    acquireExclusiveLock();
    try {
      return map.containsKey(iKey);

    } finally {
      releaseExclusiveLock();
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
    return ((OMVRBTreeProviderAbstract<Object, ?>) map.getProvider()).getRecord().getIdentity();
  }

  public long rebuild() {
    return rebuild(new OIndexRebuildOutputListener(this));
  }

  @Override
  public void setRebuildingFlag() {
    rebuilding = true;
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
        map.clear();
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

      lazySave();
      unload();

      if (iProgressListener != null)
        iProgressListener.onCompletition(this, true);

    } catch (final Exception e) {
      if (iProgressListener != null)
        iProgressListener.onCompletition(this, false);

      try {
        map.clear();
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

    return documentIndexed;
  }

  public boolean remove(final Object iKey, final OIdentifiable iValue) {
    checkForRebuild();

    modificationLock.requestModificationLock();
    try {
      return remove(iKey);
    } finally {
      modificationLock.releaseModificationLock();
    }

  }

  public boolean remove(final Object key) {
    checkForRebuild();

    modificationLock.requestModificationLock();

    try {
      acquireExclusiveLock();
      try {

        return map.remove(key) != null;

      } finally {
        releaseExclusiveLock();
      }
    } finally {
      modificationLock.releaseModificationLock();
    }
  }

  public OIndex<T> clear() {
    checkForRebuild();

    modificationLock.requestModificationLock();

    try {
      acquireExclusiveLock();
      try {

        map.clear();
        return this;

      } finally {
        releaseExclusiveLock();
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
        map.delete();
        return this;

      } finally {
        releaseExclusiveLock();
      }
    } finally {
      modificationLock.releaseModificationLock();
    }
  }

  public OIndexInternal<T> lazySave() {

    acquireExclusiveLock();
    try {

      map.lazySave();
      return this;

    } finally {
      releaseExclusiveLock();
    }
  }

  public ORecord<?> getRecord() {
    return ((OMVRBTreeProviderAbstract<Object, ?>) map.getProvider()).getRecord();
  }

  public Iterator<Entry<Object, T>> iterator() {
    checkForRebuild();

    acquireExclusiveLock();
    try {

      return map.entrySet().iterator();

    } finally {
      releaseExclusiveLock();
    }
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  public Iterator<Entry<Object, T>> inverseIterator() {
    checkForRebuild();

    acquireExclusiveLock();
    try {

      return ((OMVRBTree.EntrySet) map.entrySet()).inverseIterator();

    } finally {
      releaseExclusiveLock();
    }
  }

  public Iterable<Object> keys() {
    checkForRebuild();

    acquireExclusiveLock();
    try {

      return map.keySet();

    } finally {
      releaseExclusiveLock();
    }
  }

  public String getName() {
    return name;
  }

  public String getType() {
    return type;
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

  public OIndexMVRBTreeAbstract<T> addCluster(final String iClusterName) {
    acquireExclusiveLock();
    try {
      if (clustersToIndex.add(iClusterName))
        updateConfiguration();
      return this;
    } finally {
      releaseExclusiveLock();
    }
  }

  public OIndexMVRBTreeAbstract<T> removeCluster(String iClusterName) {
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

    acquireExclusiveLock();
    try {

      map.unload();

    } finally {
      releaseExclusiveLock();
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
        configuration.field(CONFIG_MAP_RID, ((OMVRBTreeProviderAbstract<Object, ?>) map.getProvider()).getRecord().getIdentity());

      } finally {
        configuration.setInternalStatus(ORecordElement.STATUS.LOADED);
      }

    } finally {
      releaseExclusiveLock();
    }
    return configuration;
  }

  @SuppressWarnings("unchecked")
  public void commit(final ODocument iDocument) {
    checkForRebuild();

    if (iDocument == null)
      return;

    acquireExclusiveLock();
    try {
      map.setRunningTransaction(true);

      final Boolean clearAll = (Boolean) iDocument.field("clear");
      if (clearAll != null && clearAll)
        clear();

      final Collection<ODocument> entries = iDocument.field("entries");

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
              else
                remove(key, value);
            }
          }
        }
      }

    } finally {
      releaseExclusiveLock();
      map.setRunningTransaction(false);
    }
  }

  public ODocument getConfiguration() {
    return configuration;
  }

  public boolean isAutomatic() {
    return indexDefinition != null && indexDefinition.getClassName() != null;
  }

  protected void installHooks(final ODatabaseRecord iDatabase) {
    final OJVMProfiler profiler = Orient.instance().getProfiler();
    final String profilerPrefix = profiler.getDatabaseMetric(iDatabase.getName(), "index." + name + '.');
    final String profilerMetadataPrefix = "db.*.index.*.";

    profiler.registerHookValue(profilerPrefix + "items", "Index size", METRIC_TYPE.SIZE, new OProfilerHookValue() {
      public Object getValue() {
        acquireSharedLock();
        try {
          return map != null ? map.size() : "-";
        } finally {
          releaseSharedLock();
        }
      }
    }, profilerMetadataPrefix + "items");

    profiler.registerHookValue(profilerPrefix + "entryPointSize", "Number of entrypoints in an index", METRIC_TYPE.SIZE,
        new OProfilerHookValue() {
          public Object getValue() {
            return map != null ? map.getEntryPointSize() : "-";
          }
        }, profilerMetadataPrefix + "items");

    profiler.registerHookValue(profilerPrefix + "maxUpdateBeforeSave", "Maximum number of updates in a index before force saving",
        METRIC_TYPE.SIZE, new OProfilerHookValue() {
          public Object getValue() {
            return map != null ? map.getMaxUpdatesBeforeSave() : "-";
          }
        }, profilerMetadataPrefix + "maxUpdateBeforeSave");

    Orient.instance().getMemoryWatchDog().addListener(watchDog);
    iDatabase.registerListener(this);
  }

  protected void uninstallHooks(final ODatabaseRecord iDatabase) {
    final OJVMProfiler profiler = Orient.instance().getProfiler();
    final String profilerPrefix = profiler.getDatabaseMetric(iDatabase.getName(), "index." + name + '.');

    profiler.unregisterHookValue(profilerPrefix + ".items");
    profiler.unregisterHookValue(profilerPrefix + ".entryPointSize");
    profiler.unregisterHookValue(profilerPrefix + ".maxUpdateBeforeSave");
    profiler.unregisterHookValue(profilerPrefix + ".optimizationThreshold");

    Orient.instance().getMemoryWatchDog().removeListener(watchDog);
    iDatabase.unregisterListener(this);
  }

  public void onCreate(final ODatabase iDatabase) {
  }

  public void onDelete(final ODatabase iDatabase) {
  }

  public void onOpen(final ODatabase iDatabase) {
  }

  public void onBeforeTxBegin(final ODatabase iDatabase) {
    acquireExclusiveLock();
    try {

      map.commitChanges(true);

    } finally {
      releaseExclusiveLock();
    }
  }

  public void onBeforeTxRollback(final ODatabase iDatabase) {
  }

  public boolean onCorruptionRepairDatabase(final ODatabase iDatabase, final String iReason, String iWhatWillbeFixed) {
    if (iReason.equals("load"))
      return true;
    return false;
  }

  public void onAfterTxRollback(final ODatabase iDatabase) {

    acquireExclusiveLock();
    try {

      map.unload();

    } finally {
      releaseExclusiveLock();
    }
  }

  public void onBeforeTxCommit(final ODatabase iDatabase) {
  }

  public void onAfterTxCommit(final ODatabase iDatabase) {
    acquireExclusiveLock();
    try {

      map.onAfterTxCommit();

    } finally {
      releaseExclusiveLock();
    }
  }

  public void onClose(final ODatabase iDatabase) {
    if (isRebuiding())
      return;

    acquireExclusiveLock();
    try {

      map.commitChanges(true);
      map.unload();
      Orient.instance().getMemoryWatchDog().removeListener(watchDog);

    } finally {
      releaseExclusiveLock();
    }
  }

  protected void optimize(final boolean iHardMode) {
    if (map == null)
      return;

    acquireExclusiveLock();
    try {

      OLogManager.instance().debug(this,
          "Forcing " + (iHardMode ? "hard" : "soft") + " optimization of Index %s (%d items). Found %d entries in memory...", name,
          map.size(), map.getNumberOfNodesInCache());

      map.setOptimization(iHardMode ? 2 : 1);
      final int freed = map.optimize(iHardMode);

      OLogManager.instance().debug(this, "Completed! Freed %d entries and now %d entries reside in memory", freed,
          map.getNumberOfNodesInCache());

    } finally {
      releaseExclusiveLock();
    }
  }

  protected void checkForKeyType(final Object iKey) {
    if (indexDefinition == null) {
      // RECOGNIZE THE KEY TYPE AT RUN-TIME

      final OType type = OType.getTypeByClass(iKey.getClass());
      if (type == null)
        return;

      indexDefinition = new OSimpleKeyIndexDefinition(type);
      maxUpdatesBeforeSave = lazyUpdates();
      updateConfiguration();
    }
  }

  protected ODatabaseRecord getDatabase() {
    return ODatabaseRecordThreadLocal.INSTANCE.get();
  }

  public OType[] getKeyTypes() {
    if (indexDefinition == null)
      return null;

    return indexDefinition.getTypes();
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
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    final OIndexMVRBTreeAbstract<?> that = (OIndexMVRBTreeAbstract<?>) o;

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

  private int lazyUpdates() {
    return isAutomatic() ? OGlobalConfiguration.INDEX_AUTO_LAZY_UPDATES.getValueAsInteger()
        : OGlobalConfiguration.INDEX_MANUAL_LAZY_UPDATES.getValueAsInteger();
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
