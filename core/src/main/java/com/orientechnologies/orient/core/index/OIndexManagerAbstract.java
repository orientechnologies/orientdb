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
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OMultiKey;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.OScenarioThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.dictionary.ODictionary;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.OMetadata;
import com.orientechnologies.orient.core.metadata.OMetadataDefault;
import com.orientechnologies.orient.core.metadata.OMetadataInternal;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sharding.auto.OAutoShardingIndexFactory;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.type.ODocumentWrapper;
import com.orientechnologies.orient.core.type.ODocumentWrapperNoClass;

import java.util.*;
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
  public static final String CONFIG_INDEXES  = "indexes";
  public static final String DICTIONARY_NAME = "dictionary";

  // values of this Map should be IMMUTABLE !! for thread safety reasons.
  protected final Map<String, Map<OMultiKey, Set<OIndex<?>>>> classPropertyIndex = new ConcurrentHashMap<String, Map<OMultiKey, Set<OIndex<?>>>>();
  protected       Map<String, OIndex<?>>                      indexes            = new ConcurrentHashMap<String, OIndex<?>>();
  protected       String                                      defaultClusterName = OMetadataDefault.CLUSTER_INDEX_NAME;
  protected       String                                      manualClusterName  = OMetadataDefault.CLUSTER_MANUAL_INDEX_NAME;

  protected ReadWriteLock lock = new ReentrantReadWriteLock();

  public OIndexManagerAbstract(final ODatabaseDocument iDatabase) {
    super(new ODocument().setTrackingChanges(false));
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
          boolean saved = false;
          for (int retry = 0; retry < 10; retry++)
            try {

              toStream();
              document.save();
              saved = true;
              break;

            } catch (OConcurrentModificationException e) {
              OLogManager.instance().debug(this, "concurrent modification while saving index manager configuration", e);
              reload(null, true);
            }

          if (!saved)
            OLogManager.instance().error(this, "failed to save the index manager configuration after 10 retries");

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
      createIndex(DICTIONARY_NAME, OClass.INDEX_TYPE.DICTIONARY.toString(),
          new OSimpleKeyIndexDefinition(factory.getLastVersion(), OType.STRING), null, null, null);
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
    final Locale locale = getServerLocale();

    final OIndex<?> index = indexes.get(iName.toLowerCase(locale));
    if (index == null)
      return null;
    return preProcessBeforeReturn(index);
  }

  @Override
  public void addClusterToIndex(final String clusterName, final String indexName) {
    final Locale locale = getServerLocale();

    final OIndex<?> index = indexes.get(indexName.toLowerCase(locale));
    if (index == null)
      throw new OIndexException("Index with name " + indexName + " does not exist.");

    if (index.getInternal() == null)
      throw new OIndexException("Index with name " + indexName + " has no internal presentation.");
    if (!index.getInternal().getClusters().contains(clusterName)) {
      index.getInternal().addCluster(clusterName);
      save();
    }
  }

  @Override
  public void removeClusterFromIndex(final String clusterName, final String indexName) {
    final Locale locale = getServerLocale();

    final OIndex<?> index = indexes.get(indexName.toLowerCase(locale));
    if (index == null)
      throw new OIndexException("Index with name " + indexName + " does not exist.");
    index.getInternal().removeCluster(clusterName);
    save();
  }

  public boolean existsIndex(final String iName) {
    final Locale locale = getServerLocale();
    return indexes.containsKey(iName.toLowerCase(locale));
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

  public Set<OIndex<?>> getClassInvolvedIndexes(final String className, Collection<String> fields) {
    fields = normalizeFieldNames(fields);

    final OMultiKey multiKey = new OMultiKey(fields);

    final Map<OMultiKey, Set<OIndex<?>>> propertyIndex = getIndexOnProperty(className);

    if (propertyIndex == null || !propertyIndex.containsKey(multiKey))
      return Collections.emptySet();

    final Set<OIndex<?>> rawResult = propertyIndex.get(multiKey);
    final Set<OIndex<?>> transactionalResult = new HashSet<OIndex<?>>(rawResult.size());
    for (final OIndex<?> index : rawResult) {
      //ignore indexes that ignore null values on partial match
      if (fields.size() == index.getDefinition().getFields().size() || !index.getDefinition().isNullValuesIgnored()) {
        transactionalResult.add(preProcessBeforeReturn(index));
      }
    }

    return transactionalResult;
  }

  public Set<OIndex<?>> getClassInvolvedIndexes(final String className, final String... fields) {
    return getClassInvolvedIndexes(className, Arrays.asList(fields));
  }

  public boolean areIndexed(final String className, Collection<String> fields) {
    fields = normalizeFieldNames(fields);

    final OMultiKey multiKey = new OMultiKey(fields);

    final Map<OMultiKey, Set<OIndex<?>>> propertyIndex = getIndexOnProperty(className);

    if (propertyIndex == null)
      return false;

    return propertyIndex.containsKey(multiKey) && !propertyIndex.get(multiKey).isEmpty();
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
    final Map<OMultiKey, Set<OIndex<?>>> propertyIndex = getIndexOnProperty(className);

    if (propertyIndex == null)
      return;

    for (final Set<OIndex<?>> propertyIndexes : propertyIndex.values())
      for (final OIndex<?> index : propertyIndexes)
        indexes.add(preProcessBeforeReturn(index));
  }

  @Override
  public OIndexUnique getClassUniqueIndex(final String className) {
    final Map<OMultiKey, Set<OIndex<?>>> propertyIndex = getIndexOnProperty(className);

    if (propertyIndex != null)
      for (final Set<OIndex<?>> propertyIndexes : propertyIndex.values())
        for (final OIndex<?> index : propertyIndexes)
          if (index instanceof OIndexUnique)
            return (OIndexUnique) index;

    return null;
  }

  public OIndex<?> getClassIndex(String className, String indexName) {
    final Locale locale = getServerLocale();
    className = className.toLowerCase(locale);
    indexName = indexName.toLowerCase(locale);

    final OIndex<?> index = indexes.get(indexName);
    if (index != null && index.getDefinition() != null && index.getDefinition().getClassName() != null && className
        .equals(index.getDefinition().getClassName().toLowerCase(locale)))
      return preProcessBeforeReturn(index);
    return null;
  }

  @Override
  public OIndex<?> getClassAutoShardingIndex(String className) {
    final Locale locale = getServerLocale();
    className = className.toLowerCase(locale);

    // LOOK FOR INDEX
    for (OIndex<?> index : indexes.values()) {
      if (index != null && OAutoShardingIndexFactory.AUTOSHARDING_ALGORITHM.equals(index.getAlgorithm())
          && index.getDefinition() != null && index.getDefinition().getClassName() != null && className
          .equals(index.getDefinition().getClassName().toLowerCase(locale)))
        return preProcessBeforeReturn(index);
    }
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

  protected static ODatabaseDocumentInternal getDatabase() {
    return ODatabaseRecordThreadLocal.INSTANCE.get();
  }

  protected ODatabaseDocumentInternal getDatabaseIfDefined() {
    return ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();
  }

  protected void addIndexInternal(final OIndex<?> index) {
    acquireExclusiveLock();
    try {
      final Locale locale = getServerLocale();
      indexes.put(index.getName().toLowerCase(locale), index);

      final OIndexDefinition indexDefinition = index.getDefinition();
      if (indexDefinition == null || indexDefinition.getClassName() == null)
        return;

      Map<OMultiKey, Set<OIndex<?>>> propertyIndex = getIndexOnProperty(indexDefinition.getClassName());

      if (propertyIndex == null) {
        propertyIndex = new HashMap<OMultiKey, Set<OIndex<?>>>();
      } else {
        propertyIndex = new HashMap<OMultiKey, Set<OIndex<?>>>(propertyIndex);
      }

      final int paramCount = indexDefinition.getParamCount();

      for (int i = 1; i <= paramCount; i++) {
        final List<String> fields = indexDefinition.getFields().subList(0, i);
        final OMultiKey multiKey = new OMultiKey(normalizeFieldNames(fields));
        Set<OIndex<?>> indexSet = propertyIndex.get(multiKey);

        if (indexSet == null)
          indexSet = new HashSet<OIndex<?>>();
        else
          indexSet = new HashSet<OIndex<?>>(indexSet);

        indexSet.add(index);
        propertyIndex.put(multiKey, indexSet);
      }

      classPropertyIndex.put(indexDefinition.getClassName().toLowerCase(locale), copyPropertyMap(propertyIndex));
    } finally {
      releaseExclusiveLock();
    }
  }

  protected static Map<OMultiKey, Set<OIndex<?>>> copyPropertyMap(Map<OMultiKey, Set<OIndex<?>>> original) {
    final Map<OMultiKey, Set<OIndex<?>>> result = new HashMap<OMultiKey, Set<OIndex<?>>>();

    for (Map.Entry<OMultiKey, Set<OIndex<?>>> entry : original.entrySet()) {
      Set<OIndex<?>> indexes = new HashSet<OIndex<?>>(entry.getValue());
      assert indexes.equals(entry.getValue());

      result.put(entry.getKey(), Collections.unmodifiableSet(indexes));
    }

    assert result.equals(original);

    return Collections.unmodifiableMap(result);
  }

  protected List<String> normalizeFieldNames(final Collection<String> fieldNames) {
    final Locale locale = getServerLocale();
    final ArrayList<String> result = new ArrayList<String>(fieldNames.size());
    for (final String fieldName : fieldNames)
      result.add(fieldName.toLowerCase(locale));
    return result;
  }

  protected abstract OIndex<?> preProcessBeforeReturn(final OIndex<?> index);

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

  protected Locale getServerLocale() {
    ODatabaseDocumentInternal db = getDatabase();
    OStorage storage = db.getStorage();
    OStorageConfiguration configuration = storage.getConfiguration();
    return configuration.getLocaleInstance();
  }


  private Map<OMultiKey, Set<OIndex<?>>> getIndexOnProperty(final String className) {
    final Locale locale = getServerLocale();

    acquireSharedLock();
    try {

      return classPropertyIndex.get(className.toLowerCase(locale));

    } finally {
      releaseSharedLock();
    }
  }
}
