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
package com.orientechnologies.orient.core.index.hashindex.local;

import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import com.orientechnologies.common.concur.resource.OCloseable;
import com.orientechnologies.common.concur.resource.OSharedResourceAdaptive;
import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexException;
import com.orientechnologies.orient.core.index.OIndexInternal;
import com.orientechnologies.orient.core.index.OIndexRebuildOutputListener;
import com.orientechnologies.orient.core.index.ORuntimeKeyIndexDefinition;
import com.orientechnologies.orient.core.index.OSimpleKeyIndexDefinition;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;
import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializerFactory;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.index.OCompositeKeySerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.index.OSimpleKeySerializer;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocalAbstract;

/**
 * @author Andrey Lomakin
 * @since 2/17/13
 */
public abstract class OAbstractLocalHashIndex<T> extends OSharedResourceAdaptive implements OIndexInternal<T>, OCloseable {
  private static final String              CONFIG_CLUSTERS                       = "clusters";
  private static final String              CONFIG_MAP_RID                        = "mapRid";
  public static final String               BUCKET_FILE_EXTENSION                 = ".obf";
  public static final String               METADATA_CONFIGURATION_FILE_EXTENSION = ".imc";
  public static final String               TREE_STATE_FILE_EXTENSION             = ".tsc";

  private final OLocalHashTable<Object, T> localHashTable;
  private OStorageLocalAbstract            storage;

  private String                           name;
  private String                           type;

  private OIndexDefinition                 indexDefinition;
  private Set<String>                      clustersToIndex                       = new LinkedHashSet<String>();

  private ODocument                        configuration;
  private ORID                             identity;
  private OMurmurHash3HashFunction<Object> keyHashFunction;
  private boolean                          rebuiding                             = false;

  public OAbstractLocalHashIndex(String type) {
    super(OGlobalConfiguration.ENVIRONMENT_CONCURRENT.getValueAsBoolean());

    this.type = type;
    this.keyHashFunction = new OMurmurHash3HashFunction<Object>();
    this.localHashTable = new OLocalHashTable<Object, T>(METADATA_CONFIGURATION_FILE_EXTENSION, TREE_STATE_FILE_EXTENSION,
        BUCKET_FILE_EXTENSION, keyHashFunction);
  }

  @Override
  public void setRebuildingFlag() {
    rebuiding = true;
  }

  public OIndex<T> create(String name, OIndexDefinition indexDefinition, ODatabaseRecord database, String clusterIndexName,
      int[] clusterIdsToIndex, boolean rebuild, OProgressListener progressListener, OBinarySerializer<T> valueSerializer) {
    acquireExclusiveLock();
    try {
      configuration = new ODocument();
      this.indexDefinition = indexDefinition;
      this.name = name;
      storage = (OStorageLocalAbstract) database.getStorage();

      final ORecord<?> emptyRecord = new ORecordBytes(new byte[] {});
      emptyRecord.save(clusterIndexName);
      identity = emptyRecord.getIdentity();

      OBinarySerializer<Object> keySerializer = (OBinarySerializer<Object>) detectKeySerializer(indexDefinition);

      if (clusterIdsToIndex != null)
        for (final int id : clusterIdsToIndex)
          clustersToIndex.add(database.getClusterNameById(id));

      keyHashFunction.setValueSerializer(keySerializer);
      localHashTable.create(name, keySerializer, valueSerializer, storage);

      updateConfiguration();
      if (rebuild)
        rebuild(progressListener);

      return this;
    } finally {
      releaseExclusiveLock();
    }
  }

  private OBinarySerializer<?> detectKeySerializer(OIndexDefinition indexDefinition) {
    if (indexDefinition != null) {
      if (indexDefinition instanceof ORuntimeKeyIndexDefinition)
        return ((ORuntimeKeyIndexDefinition) indexDefinition).getSerializer();
      else {
        if (indexDefinition.getTypes().length > 1)
          return OCompositeKeySerializer.INSTANCE;
        else
          return OBinarySerializerFactory.INSTANCE.getObjectSerializer(indexDefinition.getTypes()[0]);
      }
    } else
      return new OSimpleKeySerializer();
  }

  @Override
  public void unload() {
  }

  @Override
  public String getDatabaseName() {
    return storage.getName();
  }

  @Override
  public OType[] getKeyTypes() {
    if (indexDefinition == null)
      return null;

    return indexDefinition.getTypes();
  }

  @Override
  public Iterator<Map.Entry<Object, T>> iterator() {
    return null;
  }

  @Override
  public Iterator<Map.Entry<Object, T>> inverseIterator() {
    return null;
  }

  @Override
  public Iterator<OIdentifiable> valuesIterator() {
    return null;
  }

  @Override
  public Iterator<OIdentifiable> valuesInverseIterator() {
    return null;
  }

  @Override
  public T get(Object key) {
    acquireSharedLock();
    try {
      return localHashTable.get(key);
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public OIndex<T> put(Object key, OIdentifiable value) {
    acquireExclusiveLock();
    try {
      localHashTable.put(key, (T) value);
      return this;
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public boolean remove(Object key) {
    acquireExclusiveLock();
    try {
      return localHashTable.remove(key) != null;
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public boolean remove(Object iKey, OIdentifiable iRID) {
    return remove(iKey);
  }

  @Override
  public int remove(OIdentifiable iRID) {
    throw new UnsupportedOperationException("remove(rid)");
  }

  @Override
  public OIndex<T> clear() {
    acquireExclusiveLock();
    try {
      localHashTable.clear();

      return this;
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public Iterable<Object> keys() {
    throw new UnsupportedOperationException("keys");
  }

  @Override
  public Collection<OIdentifiable> getValuesBetween(Object iRangeFrom, Object iRangeTo) {
    throw new UnsupportedOperationException("getValuesBetween");
  }

  @Override
  public Collection<OIdentifiable> getValuesBetween(Object iRangeFrom, boolean iFromInclusive, Object iRangeTo, boolean iToInclusive) {
    throw new UnsupportedOperationException("getValuesBetween");
  }

  @Override
  public Collection<OIdentifiable> getValuesBetween(Object iRangeFrom, boolean iFromInclusive, Object iRangeTo,
      boolean iToInclusive, int maxValuesToFetch) {
    throw new UnsupportedOperationException("getValuesBetween");
  }

  @Override
  public long count(Object iRangeFrom, boolean iFromInclusive, Object iRangeTo, boolean iToInclusive, int maxValuesToFetch) {
    throw new UnsupportedOperationException("count");
  }

  @Override
  public Collection<OIdentifiable> getValuesMajor(Object fromKey, boolean isInclusive) {
    throw new UnsupportedOperationException("getValuesMajor");
  }

  @Override
  public Collection<OIdentifiable> getValuesMajor(Object fromKey, boolean isInclusive, int maxValuesToFetch) {
    throw new UnsupportedOperationException("getValuesMajor");
  }

  @Override
  public Collection<OIdentifiable> getValuesMinor(Object toKey, boolean isInclusive) {
    throw new UnsupportedOperationException("getValuesMinor");
  }

  @Override
  public Collection<OIdentifiable> getValuesMinor(Object toKey, boolean isInclusive, int maxValuesToFetch) {
    throw new UnsupportedOperationException("getValuesMinor");
  }

  @Override
  public Collection<ODocument> getEntriesMajor(Object fromKey, boolean isInclusive) {
    throw new UnsupportedOperationException("getEntriesMajor");
  }

  @Override
  public Collection<ODocument> getEntriesMajor(Object fromKey, boolean isInclusive, int maxEntriesToFetch) {
    throw new UnsupportedOperationException("getEntriesMajor");
  }

  @Override
  public Collection<ODocument> getEntriesMinor(Object toKey, boolean isInclusive) {
    throw new UnsupportedOperationException("getEntriesMinor");
  }

  @Override
  public Collection<ODocument> getEntriesMinor(Object toKey, boolean isInclusive, int maxEntriesToFetch) {
    throw new UnsupportedOperationException("getEntriesMinor");
  }

  @Override
  public Collection<ODocument> getEntriesBetween(Object iRangeFrom, Object iRangeTo, boolean iInclusive) {
    throw new UnsupportedOperationException("getEntriesBetween");
  }

  @Override
  public Collection<ODocument> getEntriesBetween(Object iRangeFrom, Object iRangeTo, boolean iInclusive, int maxEntriesToFetch) {
    throw new UnsupportedOperationException("getEntriesBetween");
  }

  @Override
  public Collection<ODocument> getEntriesBetween(Object iRangeFrom, Object iRangeTo) {
    throw new UnsupportedOperationException("getEntriesBetween");
  }

  @Override
  public long getSize() {
    return localHashTable.size();
  }

  @Override
  public long getKeySize() {
    return localHashTable.size();
  }

  @Override
  public OIndex<T> lazySave() {
    flush();
    return this;
  }

  @Override
  public OIndex<T> delete() {
    acquireExclusiveLock();
    try {
      localHashTable.delete();
      return this;
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getType() {
    return type;
  }

  @Override
  public boolean isAutomatic() {
    return indexDefinition != null && indexDefinition.getClassName() != null;
  }

  @Override
  public long rebuild() {
    return rebuild(new OIndexRebuildOutputListener(this));
  }

  @Override
  public long rebuild(OProgressListener iProgressListener) {
    long documentIndexed = 0;

    final boolean intentInstalled = getDatabase().declareIntent(new OIntentMassiveInsert());

    acquireExclusiveLock();
    try {
      rebuiding = true;

      try {
        clear();
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
            if (record instanceof ODocument) {
              final ODocument doc = (ODocument) record;

              if (indexDefinition == null)
                throw new OConfigurationException("Index '" + name + "' cannot be rebuilt because has no a valid definition ("
                    + indexDefinition + ")");

              final Object fieldValue = indexDefinition.getDocumentValueToIndex(doc);

              if (fieldValue != null) {
                if (fieldValue instanceof Collection) {
                  for (final Object fieldValueItem : (Collection<?>) fieldValue) {
                    put(fieldValueItem, doc);
                  }
                } else
                  put(fieldValue, doc);

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

      if (iProgressListener != null)
        iProgressListener.onCompletition(this, true);

    } catch (final Exception e) {
      if (iProgressListener != null)
        iProgressListener.onCompletition(this, false);

      try {
        clear();
      } catch (Exception e2) {
        // IGNORE EXCEPTION: IF THE REBUILD WAS LAUNCHED IN CASE OF RID INVALID CLEAR ALWAYS GOES IN ERROR
      }

      throw new OIndexException("Error on rebuilding the index for clusters: " + clustersToIndex, e);

    } finally {
      rebuiding = false;

      if (intentInstalled)
        getDatabase().declareIntent(null);

      releaseExclusiveLock();
    }

    return documentIndexed;
  }

  protected ODatabaseRecord getDatabase() {
    return ODatabaseRecordThreadLocal.INSTANCE.get();
  }

  @Override
  public ODocument getConfiguration() {
    return configuration;
  }

  @Override
  public ORID getIdentity() {
    return identity;
  }

  @Override
  public void commit(ODocument iDocument) {
    throw new UnsupportedOperationException("commit");
  }

  @Override
  public OIndexInternal<T> getInternal() {
    return this;
  }

  protected void checkForKeyType(final Object iKey) {
    if (indexDefinition == null) {
      // RECOGNIZE THE KEY TYPE AT RUN-TIME

      final OType type = OType.getTypeByClass(iKey.getClass());
      if (type == null)
        return;

      indexDefinition = new OSimpleKeyIndexDefinition(type);

      final OBinarySerializer<Object> keySerializer = (OBinarySerializer<Object>) detectKeySerializer(indexDefinition);
      keyHashFunction.setValueSerializer(keySerializer);

      localHashTable.setKeySerializer(keySerializer);
      updateConfiguration();
    }
  }

  @Override
  public Collection<OIdentifiable> getValues(Collection<?> iKeys) {
    throw new UnsupportedOperationException("getValues()");
  }

  @Override
  public Collection<OIdentifiable> getValues(Collection<?> iKeys, int maxValuesToFetch) {
    throw new UnsupportedOperationException("getValues()");
  }

  @Override
  public Collection<ODocument> getEntries(Collection<?> iKeys) {
    throw new UnsupportedOperationException("getEntries()");
  }

  @Override
  public Collection<ODocument> getEntries(Collection<?> iKeys, int maxEntriesToFetch) {
    throw new UnsupportedOperationException("getEntries()");
  }

  @Override
  public OIndexDefinition getDefinition() {
    return indexDefinition;
  }

  @Override
  public Set<String> getClusters() {
    return Collections.unmodifiableSet(clustersToIndex);
  }

  @Override
  public boolean supportsOrderedIterations() {
    return false; // To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public IndexMetadata loadMetadata(ODocument config) {
    final String indexName = config.field(OIndexInternal.CONFIG_NAME);
    final String indexType = config.field(OIndexInternal.CONFIG_TYPE);

    OIndexDefinition loadedIndexDefinition = null;

    final ODocument indexDefinitionDoc = config.field(OIndexInternal.INDEX_DEFINITION);
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

    }

    final Set<String> clusters = new HashSet<String>((Collection<String>) config.field(CONFIG_CLUSTERS));
    return new IndexMetadata(indexName, loadedIndexDefinition, clusters, indexType);
  }

  @Override
  public void flush() {
    acquireExclusiveLock();
    try {
      localHashTable.flush();
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public boolean loadFromConfiguration(ODocument configuration) {
    acquireExclusiveLock();
    try {
      final ORID rid = configuration.field(CONFIG_MAP_RID, ORID.class);
      if (rid == null)
        throw new OIndexException("Error during deserialization of index definition: '" + CONFIG_MAP_RID + "' attribute is null");
      identity = rid;

      this.configuration = configuration;
      storage = (OStorageLocalAbstract) getDatabase().getStorage();
      clustersToIndex.clear();

      IndexMetadata indexMetadata = loadMetadata(configuration);

      name = indexMetadata.getName();
      type = indexMetadata.getType();
      indexDefinition = indexMetadata.getIndexDefinition();
      clustersToIndex.addAll(indexMetadata.getClustersToIndex());

      keyHashFunction.setValueSerializer((OBinarySerializer<Object>) detectKeySerializer(indexDefinition));
      localHashTable.load(name, storage);

      return true;
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    final OAbstractLocalHashIndex<?> that = (OAbstractLocalHashIndex<?>) o;

    if (!name.equals(that.name))
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    return name.hashCode();
  }

  @Override
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
        configuration.field(CONFIG_MAP_RID, identity);
      } finally {
        configuration.setInternalStatus(ORecordElement.STATUS.LOADED);
      }

      return configuration;
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public OIndex<T> addCluster(String iClusterName) {
    acquireExclusiveLock();
    try {
      if (clustersToIndex.add(iClusterName))
        updateConfiguration();
      return this;
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public OIndex<T> removeCluster(String iClusterName) {
    acquireExclusiveLock();
    try {
      if (clustersToIndex.remove(iClusterName))
        updateConfiguration();
      return this;
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public boolean canBeUsedInEqualityOperators() {
    return true;
  }

  @Override
  public void freeze(boolean throwException) {
    throw new UnsupportedOperationException("freeze");
  }

  @Override
  public void release() {
    throw new UnsupportedOperationException("release");
  }

  @Override
  public void acquireModificationLock() {
    throw new UnsupportedOperationException("acquireModificationLock");
  }

  @Override
  public void releaseModificationLock() {
    throw new UnsupportedOperationException("releaseModificationLock");
  }

  @Override
  public void onCreate(ODatabase iDatabase) {
  }

  @Override
  public void onDelete(ODatabase iDatabase) {
  }

  @Override
  public void onOpen(ODatabase iDatabase) {
  }

  @Override
  public void onBeforeTxBegin(ODatabase iDatabase) {
    throw new UnsupportedOperationException("onBeforeTxBegin");
  }

  @Override
  public void onBeforeTxRollback(ODatabase iDatabase) {
    throw new UnsupportedOperationException("onBeforeTxRollback");
  }

  @Override
  public void onAfterTxRollback(ODatabase iDatabase) {
    throw new UnsupportedOperationException("onAfterTxRollback");
  }

  @Override
  public void onBeforeTxCommit(ODatabase iDatabase) {
    throw new UnsupportedOperationException("onBeforeTxCommit");
  }

  @Override
  public void onAfterTxCommit(ODatabase iDatabase) {
    throw new UnsupportedOperationException("onAfterTxCommit");
  }

  @Override
  public void onClose(ODatabase iDatabase) {
  }

  @Override
  public boolean onCorruptionRepairDatabase(ODatabase iDatabase, String iReason, String iWhatWillbeFixed) {
    return true;
  }

  @Override
  public void close() {
    acquireExclusiveLock();
    try {
      localHashTable.close();
    } finally {
      releaseExclusiveLock();
    }
  }

  public boolean isRebuiding() {
    return rebuiding;
  }
}
