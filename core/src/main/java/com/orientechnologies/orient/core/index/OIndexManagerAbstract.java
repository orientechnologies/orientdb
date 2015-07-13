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

import com.orientechnologies.common.concur.resource.OCloseable;
import com.orientechnologies.common.util.OMultiKey;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.OScenarioThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.dictionary.ODictionary;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.OMetadata;
import com.orientechnologies.orient.core.metadata.OMetadataDefault;
import com.orientechnologies.orient.core.metadata.OMetadataInternal;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OStorageProxy;
import com.orientechnologies.orient.core.type.ODocumentWrapper;
import com.orientechnologies.orient.core.type.ODocumentWrapperNoClass;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Abstract class to manage indexes.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
@SuppressWarnings({ "unchecked", "serial" })
public abstract class OIndexManagerAbstract extends ODocumentWrapperNoClass implements OIndexManager, OCloseable {
  public static final String                                  CONFIG_INDEXES         = "indexes";
  public static final String                                  DICTIONARY_NAME        = "dictionary";
  protected final Map<String, Map<OMultiKey, Set<OIndex<?>>>> classPropertyIndex     = new HashMap<String, Map<OMultiKey, Set<OIndex<?>>>>();
  protected Map<String, OIndex<?>>                            indexes                = new ConcurrentHashMap<String, OIndex<?>>();
  protected String                                            defaultClusterName     = OMetadataDefault.CLUSTER_INDEX_NAME;
  protected String                                            manualClusterName      = OMetadataDefault.CLUSTER_MANUAL_INDEX_NAME;

  protected ReadWriteLock                                     lock                   = new ReentrantReadWriteLock();

  private volatile boolean                                    fullCheckpointOnChange = false;

  public OIndexManagerAbstract(final ODatabaseDocument iDatabase) {
    super(new ODocument().setTrackingChanges(false));
  }

  public boolean isFullCheckpointOnChange() {
    return fullCheckpointOnChange;
  }

  public void setFullCheckpointOnChange(boolean fullCheckpointOnChange) {
    this.fullCheckpointOnChange = fullCheckpointOnChange;
  }

  @Override
  public OIndexManagerAbstract load() {
    if (!autoRecreateIndexesAfterCrash()) {
      acquireExclusiveLock();
      try {
        if (getDatabase().getStorage().getConfiguration().indexMgrRecordId == null)
          // @COMPATIBILITY: CREATE THE INDEX MGR
          create();

        // RELOAD IT
        ((ORecordId) document.getIdentity()).fromString(getDatabase().getStorage().getConfiguration().indexMgrRecordId);
        super.reload("*:-1 index:0");
      } finally {
        releaseExclusiveLock();
      }
    }
    return this;
  }

  @Override
  public <RET extends ODocumentWrapper> RET reload() {
    acquireExclusiveLock();
    try {
      return (RET) super.reload();
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public <RET extends ODocumentWrapper> RET save() {

    OScenarioThreadLocal.executeAsDistributed(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        acquireExclusiveLock();

        try {
          for (int retry = 0; retry < 10; retry++)
            try {

              toStream();
              document.save();

              getDatabase().getStorage().synch();
              break;

            } catch (OConcurrentModificationException e) {
              reload(null, true);
            }

          document.save();
          getDatabase().getStorage().synch();

          return null;

        } finally {
          releaseExclusiveLock();
        }
      }
    });

    return (RET) this;
  }

  public void create() {
    acquireExclusiveLock();
    try {
      try {
        save(OMetadataDefault.CLUSTER_INTERNAL_NAME);
      } catch (Exception e) {
        // RESET RID TO ALLOCATE A NEW ONE
        if (ORecordId.isPersistent(document.getIdentity().getClusterPosition())) {
          document.getIdentity().reset();
          save(OMetadataDefault.CLUSTER_INTERNAL_NAME);
        }
      }
      getDatabase().getStorage().getConfiguration().indexMgrRecordId = document.getIdentity().toString();
      getDatabase().getStorage().getConfiguration().update();

      OIndexFactory factory = OIndexes.getFactory(OClass.INDEX_TYPE.DICTIONARY.toString(), null);
      createIndex(DICTIONARY_NAME, OClass.INDEX_TYPE.DICTIONARY.toString(), new OSimpleKeyIndexDefinition(factory.getLastVersion(),
          OType.STRING), null, null, null);
    } finally {
      releaseExclusiveLock();
    }
  }

  public void flush() {
    for (final OIndex<?> idx : indexes.values()) {
      OIndexInternal<?> indexInternal = idx.getInternal();
      if (indexInternal != null)
        indexInternal.flush();
    }
  }

  public Collection<? extends OIndex<?>> getIndexes() {
    final Collection<OIndex<?>> rawResult = indexes.values();
    final List<OIndex<?>> result = new ArrayList<OIndex<?>>(rawResult.size());
    for (final OIndex<?> index : rawResult)
      result.add(preProcessBeforeReturn(index));
    return result;
  }

  public OIndex<?> getIndex(final String iName) {
    final OIndex<?> index = indexes.get(iName.toLowerCase());
    if (index == null)
      return null;
    return preProcessBeforeReturn(index);
  }

  @Override
  public void addClusterToIndex(final String clusterName, final String indexName) {
    final OIndex<?> index = indexes.get(indexName.toLowerCase());
    if (index == null)
      throw new OIndexException("Index with name " + indexName + " does not exist.");

    if (index.getInternal() == null)
      throw new OIndexException("Index with name " + indexName + " has no internal presentation.");

    index.getInternal().addCluster(clusterName);
    save();
  }

  @Override
  public void removeClusterFromIndex(final String clusterName, final String indexName) {
    final OIndex<?> index = indexes.get(indexName.toLowerCase());
    if (index == null)
      throw new OIndexException("Index with name " + indexName + " does not exist.");
    index.getInternal().removeCluster(clusterName);
    save();
  }

  public boolean existsIndex(final String iName) {
    return indexes.containsKey(iName.toLowerCase());
  }

  public String getDefaultClusterName() {
    acquireSharedLock();
    try {
      return defaultClusterName;
    } finally {
      releaseSharedLock();
    }
  }

  public void setDefaultClusterName(final String defaultClusterName) {
    acquireExclusiveLock();
    try {
      this.defaultClusterName = defaultClusterName;
    } finally {
      releaseExclusiveLock();
    }
  }

  public ODictionary<ORecord> getDictionary() {
    OIndex<?> idx;
    acquireSharedLock();
    try {
      idx = getIndex(DICTIONARY_NAME);
    } finally {
      releaseSharedLock();
    }
    // we lock exclusively only when ODictionary not found
    if (idx == null) {
      idx = createDictionaryIfNeeded();
    }
    return new ODictionary<ORecord>((OIndex<OIdentifiable>) idx);
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

  public OIndexManager setDirty() {
    acquireExclusiveLock();
    try {
      document.setDirty();
      return this;
    } finally {
      releaseExclusiveLock();
    }
  }

  public OIndex<?> getIndex(final ORID iRID) {
    for (final OIndex<?> idx : indexes.values()) {
      if (idx.getIdentity().equals(iRID)) {
        return idx;
      }
    }
    return null;
  }

  public Set<OIndex<?>> getClassInvolvedIndexes(final String className, Collection<String> fields) {
    acquireSharedLock();
    try {
      fields = normalizeFieldNames(fields);

      final OMultiKey multiKey = new OMultiKey(fields);

      final Map<OMultiKey, Set<OIndex<?>>> propertyIndex = classPropertyIndex.get(className.toLowerCase());

      if (propertyIndex == null || !propertyIndex.containsKey(multiKey))
        return Collections.emptySet();

      final Set<OIndex<?>> rawResult = propertyIndex.get(multiKey);
      final Set<OIndex<?>> transactionalResult = new HashSet<OIndex<?>>(rawResult.size());
      for (final OIndex<?> index : rawResult)
        transactionalResult.add(preProcessBeforeReturn(index));

      return transactionalResult;
    } finally {
      releaseSharedLock();
    }
  }

  public Set<OIndex<?>> getClassInvolvedIndexes(final String className, final String... fields) {
    return getClassInvolvedIndexes(className, Arrays.asList(fields));
  }

  public boolean areIndexed(final String className, Collection<String> fields) {
    acquireSharedLock();
    try {
      fields = normalizeFieldNames(fields);

      final OMultiKey multiKey = new OMultiKey(fields);

      final Map<OMultiKey, Set<OIndex<?>>> propertyIndex = classPropertyIndex.get(className.toLowerCase());

      if (propertyIndex == null)
        return false;

      return propertyIndex.containsKey(multiKey) && !propertyIndex.get(multiKey).isEmpty();
    } finally {
      releaseSharedLock();
    }
  }

  public boolean areIndexed(final String className, final String... fields) {
    return areIndexed(className, Arrays.asList(fields));
  }

  public Set<OIndex<?>> getClassIndexes(final String className) {
    final HashSet<OIndex<?>> coll = new HashSet<OIndex<?>>(4);
    getClassIndexes(className, coll);
    return coll;
  }

  @Override
  public void getClassIndexes(final String className, final Collection<OIndex<?>> indexes) {
    acquireSharedLock();
    try {
      final Map<OMultiKey, Set<OIndex<?>>> propertyIndex = classPropertyIndex.get(className.toLowerCase());

      if (propertyIndex == null)
        return;

      for (final Set<OIndex<?>> propertyIndexes : propertyIndex.values())
        for (final OIndex<?> index : propertyIndexes)
          indexes.add(preProcessBeforeReturn(index));
    } finally {
      releaseSharedLock();
    }
  }

  public OIndex<?> getClassIndex(String className, String indexName) {
    className = className.toLowerCase();
    indexName = indexName.toLowerCase();
    final OIndex<?> index = indexes.get(indexName);
    if (index != null && index.getDefinition() != null && index.getDefinition().getClassName() != null
        && className.equals(index.getDefinition().getClassName().toLowerCase()))
      return preProcessBeforeReturn(index);
    return null;
  }

  protected void acquireSharedLock() {
    lock.readLock().lock();
  }

  protected void releaseSharedLock() {
    lock.readLock().unlock();

  }

  protected void acquireExclusiveLock() {
    final ODatabaseDocument databaseRecord = getDatabaseIfDefined();
    if (databaseRecord != null && !databaseRecord.isClosed()) {
      final OMetadataInternal metadata = (OMetadataInternal) databaseRecord.getMetadata();
      if (metadata != null)
        metadata.makeThreadLocalSchemaSnapshot();
    }

    lock.writeLock().lock();
  }

  protected void releaseExclusiveLock() {
    lock.writeLock().unlock();

    final ODatabaseDocument databaseRecord = getDatabaseIfDefined();
    if (databaseRecord != null && !databaseRecord.isClosed()) {
      final OMetadata metadata = databaseRecord.getMetadata();
      if (metadata != null)
        ((OMetadataInternal) metadata).clearThreadLocalSchemaSnapshot();
    }
  }

  protected void clearMetadata() {
    acquireExclusiveLock();
    try {
      indexes.clear();
      classPropertyIndex.clear();
    } finally {
      releaseExclusiveLock();
    }
  }

  protected ODatabaseDocumentInternal getDatabase() {
    return ODatabaseRecordThreadLocal.INSTANCE.get();
  }

  protected ODatabaseDocumentInternal getDatabaseIfDefined() {
    return ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();
  }

  protected void addIndexInternal(final OIndex<?> index) {
    acquireExclusiveLock();
    try {
      indexes.put(index.getName().toLowerCase(), index);

      final OIndexDefinition indexDefinition = index.getDefinition();
      if (indexDefinition == null || indexDefinition.getClassName() == null)
        return;

      Map<OMultiKey, Set<OIndex<?>>> propertyIndex = classPropertyIndex.get(indexDefinition.getClassName().toLowerCase());

      if (propertyIndex == null) {
        propertyIndex = new HashMap<OMultiKey, Set<OIndex<?>>>();
        classPropertyIndex.put(indexDefinition.getClassName().toLowerCase(), propertyIndex);
      }

      final int paramCount = indexDefinition.getParamCount();

      for (int i = 1; i <= paramCount; i++) {
        final List<String> fields = indexDefinition.getFields().subList(0, i);
        final OMultiKey multiKey = new OMultiKey(normalizeFieldNames(fields));
        Set<OIndex<?>> indexSet = propertyIndex.get(multiKey);
        if (indexSet == null)
          indexSet = new HashSet<OIndex<?>>();
        indexSet.add(index);
        propertyIndex.put(multiKey, indexSet);
      }
    } finally {
      releaseExclusiveLock();
    }
  }

  protected List<String> normalizeFieldNames(final Collection<String> fieldNames) {
    final ArrayList<String> result = new ArrayList<String>(fieldNames.size());
    for (final String fieldName : fieldNames)
      result.add(fieldName.toLowerCase());
    return result;
  }

  protected OIndex<?> preProcessBeforeReturn(final OIndex<?> index) {
    if (!(getDatabase().getStorage() instanceof OStorageProxy)) {
      if (index instanceof OIndexMultiValues)
        return new OIndexTxAwareMultiValue(getDatabase(), (OIndex<Set<OIdentifiable>>) index);
      else if (index instanceof OIndexDictionary)
        return new OIndexTxAwareDictionary(getDatabase(), (OIndex<OIdentifiable>) index);
      else if (index instanceof OIndexOneValue)
        return new OIndexTxAwareOneValue(getDatabase(), (OIndex<OIdentifiable>) index);
    }
    return index;
  }

  private OIndex<?> createDictionaryIfNeeded() {
    acquireExclusiveLock();
    try {
      OIndex<?> idx = getIndex(DICTIONARY_NAME);
      return idx != null ? idx : createDictionary();
    } finally {
      releaseExclusiveLock();
    }
  }

  private OIndex<?> createDictionary() {
    final OIndexFactory factory = OIndexes.getFactory(OClass.INDEX_TYPE.DICTIONARY.toString(), null);
    return createIndex(DICTIONARY_NAME, OClass.INDEX_TYPE.DICTIONARY.toString(),
        new OSimpleKeyIndexDefinition(factory.getLastVersion(), OType.STRING), null, null, null);
  }
}
