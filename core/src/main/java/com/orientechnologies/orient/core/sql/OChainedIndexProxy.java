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
package com.orientechnologies.orient.core.sql;

import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.common.profiler.OProfilerStub;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexAbstract;
import com.orientechnologies.orient.core.index.OIndexCursor;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexInternal;
import com.orientechnologies.orient.core.index.OIndexKeyCursor;
import com.orientechnologies.orient.core.index.OIndexMetadata;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItemField;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges;
import com.orientechnologies.orient.core.tx.OTransactionIndexChangesPerKey;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * There are some cases when we need to create index for some class by traversed property.
 * Unfortunately, such functionality is not supported yet. But we can do that by creating index for
 * each element of {@link OSQLFilterItemField.FieldChain} (which define "way" to our property), and
 * then process operations consequently using previously created indexes.
 *
 * <p>This class provides possibility to find optimal chain of indexes and then use it just like it
 * was index for traversed property.
 *
 * <p>IMPORTANT: this class is only for internal usage!
 *
 * @author Artem Orobets
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class OChainedIndexProxy<T> implements OIndexInternal {
  private final OIndex firstIndex;

  private final List<OIndex> indexChain;
  private final OIndex lastIndex;

  private OChainedIndexProxy(List<OIndex> indexChain) {
    this.firstIndex = indexChain.get(0);
    this.indexChain = Collections.unmodifiableList(indexChain);
    lastIndex = indexChain.get(indexChain.size() - 1);
  }

  /**
   * Create proxies that support maximum number of different operations. In case when several
   * different indexes which support different operations (e.g. indexes of {@code UNIQUE} and {@code
   * FULLTEXT} types) are possible, the creates the only one index of each type.
   *
   * @param longChain - property chain from the query, which should be evaluated
   * @return proxies needed to process query.
   */
  public static <T> Collection<OChainedIndexProxy<T>> createProxies(
      OClass iSchemaClass, OSQLFilterItemField.FieldChain longChain) {
    List<OChainedIndexProxy<T>> proxies = new ArrayList<>();

    for (List<OIndex> indexChain : getIndexesForChain(iSchemaClass, longChain)) {
      //noinspection ObjectAllocationInLoop
      proxies.add(new OChainedIndexProxy<>(indexChain));
    }

    return proxies;
  }

  private static boolean isComposite(OIndex currentIndex) {
    return currentIndex.getDefinition().getParamCount() > 1;
  }

  private static Iterable<List<OIndex>> getIndexesForChain(
      OClass iSchemaClass, OSQLFilterItemField.FieldChain fieldChain) {
    List<OIndex> baseIndexes = prepareBaseIndexes(iSchemaClass, fieldChain);

    if (baseIndexes == null) return Collections.emptyList();

    Collection<OIndex> lastIndexes = prepareLastIndexVariants(iSchemaClass, fieldChain);

    Collection<List<OIndex>> result = new ArrayList<>();
    for (OIndex lastIndex : lastIndexes) {
      @SuppressWarnings("ObjectAllocationInLoop")
      final List<OIndex> indexes = new ArrayList<>(fieldChain.getItemCount());
      indexes.addAll(baseIndexes);
      indexes.add(lastIndex);

      result.add(indexes);
    }

    return result;
  }

  private static Collection<OIndex> prepareLastIndexVariants(
      OClass iSchemaClass, OSQLFilterItemField.FieldChain fieldChain) {
    OClass oClass = iSchemaClass;
    final Collection<OIndex> result = new ArrayList<>();

    for (int i = 0; i < fieldChain.getItemCount() - 1; i++) {
      oClass = oClass.getProperty(fieldChain.getItemName(i)).getLinkedClass();
      if (oClass == null) {
        return result;
      }
    }

    final Set<OIndex> involvedIndexes =
        new TreeSet<>(Comparator.comparingInt(o -> o.getDefinition().getParamCount()));

    involvedIndexes.addAll(
        oClass.getInvolvedIndexes(fieldChain.getItemName(fieldChain.getItemCount() - 1)));
    final Collection<Class<? extends OIndex>> indexTypes = new HashSet<>(3);

    for (OIndex involvedIndex : involvedIndexes) {
      if (!indexTypes.contains(involvedIndex.getInternal().getClass())) {
        result.add(involvedIndex);
        indexTypes.add(involvedIndex.getInternal().getClass());
      }
    }

    return result;
  }

  private static List<OIndex> prepareBaseIndexes(
      OClass iSchemaClass, OSQLFilterItemField.FieldChain fieldChain) {
    List<OIndex> result = new ArrayList<>(fieldChain.getItemCount() - 1);

    OClass oClass = iSchemaClass;
    for (int i = 0; i < fieldChain.getItemCount() - 1; i++) {
      final Set<OIndex> involvedIndexes = oClass.getInvolvedIndexes(fieldChain.getItemName(i));
      final OIndex bestIndex = findBestIndex(involvedIndexes);

      if (bestIndex == null) return null;

      result.add(bestIndex);
      oClass = oClass.getProperty(fieldChain.getItemName(i)).getLinkedClass();
    }
    return result;
  }

  /**
   * Finds the index that fits better as a base index in chain. Requirements to the base index:
   *
   * <ul>
   *   <li>Should be unique or not unique. Other types cannot be used to get all documents with
   *       required links.
   *   <li>Should not be composite hash index. As soon as hash index does not support partial match
   *       search.
   *   <li>Composite index that ignores null values should not be used.
   *   <li>Hash index is better than tree based indexes.
   *   <li>Non composite indexes is better that composite.
   * </ul>
   *
   * @param indexes where search
   * @return the index that fits better as a base index in chain
   */
  protected static OIndex findBestIndex(Iterable<OIndex> indexes) {
    OIndex bestIndex = null;
    for (OIndex index : indexes) {
      if (priorityOfUsage(index) > priorityOfUsage(bestIndex)) bestIndex = index;
    }
    return bestIndex;
  }

  private static int priorityOfUsage(OIndex index) {
    if (index == null) return -1;

    final OClass.INDEX_TYPE indexType = OClass.INDEX_TYPE.valueOf(index.getType());
    final boolean isComposite = isComposite(index);
    final boolean supportNullValues = supportNullValues(index);

    int priority = 1;

    if (isComposite) {
      if (!supportNullValues) return -1;
    } else {
      priority += 10;
    }

    switch (indexType) {
      case UNIQUE_HASH_INDEX:
      case NOTUNIQUE_HASH_INDEX:
        if (isComposite) return -1;
        else priority += 10;
        break;
      case UNIQUE:
      case NOTUNIQUE:
        priority += 5;
        break;
      case PROXY:
      case FULLTEXT:
        //noinspection deprecation
      case DICTIONARY:
      case DICTIONARY_HASH_INDEX:
      case SPATIAL:
        return -1;
    }

    return priority;
  }

  /**
   * Checks if index can be used as base index. Requirements to the base index:
   *
   * <ul>
   *   <li>Should be unique or not unique. Other types cannot be used to get all documents with
   *       required links.
   *   <li>Should not be composite hash index. As soon as hash index does not support partial match
   *       search.
   *   <li>Composite index that ignores null values should not be used.
   * </ul>
   *
   * @param index to check
   * @return true if index usage is allowed as base index.
   */
  public static boolean isAppropriateAsBase(OIndex index) {
    return priorityOfUsage(index) > 0;
  }

  private static boolean supportNullValues(OIndex index) {
    final ODocument metadata = index.getMetadata();
    if (metadata == null) return false;

    final Boolean ignoreNullValues = metadata.field("ignoreNullValues");
    return Boolean.FALSE.equals(ignoreNullValues);
  }

  public String getDatabaseName() {
    return firstIndex.getDatabaseName();
  }

  public List<String> getIndexNames() {
    final ArrayList<String> names = new ArrayList<>(indexChain.size());
    for (OIndex oIndex : indexChain) {
      names.add(oIndex.getName());
    }

    return names;
  }

  @Override
  public String getName() {
    final StringBuilder res = new StringBuilder("IndexChain{");
    final List<String> indexNames = getIndexNames();

    for (int i = 0; i < indexNames.size(); i++) {
      String indexName = indexNames.get(i);
      if (i > 0) res.append(", ");
      res.append(indexName);
    }

    res.append("}");

    return res.toString();
  }

  /** {@inheritDoc} */
  @Override
  @Deprecated
  public T get(Object key) {
    final List<ORID> lastIndexResult;
    try (Stream<ORID> stream = lastIndex.getInternal().getRids(key)) {
      lastIndexResult = stream.collect(Collectors.toList());
    }

    final Set<OIdentifiable> result = new HashSet<>(applyTailIndexes(lastIndexResult));
    return (T) result;
  }

  @Override
  public Stream<ORID> getRids(Object key) {
    final List<ORID> lastIndexResult;
    try (Stream<ORID> stream = lastIndex.getInternal().getRids(key)) {
      lastIndexResult = stream.collect(Collectors.toList());
    }

    final Set<OIdentifiable> result = new HashSet<>(applyTailIndexes(lastIndexResult));

    //noinspection resource
    return result.stream().map(OIdentifiable::getIdentity);
  }

  /**
   * Returns internal index of last chain index, because proxy applicable to all operations that
   * last index applicable.
   */
  public OIndexInternal getInternal() {
    return this;
  }

  /** {@inheritDoc} */
  public OIndexDefinition getDefinition() {
    return lastIndex.getDefinition();
  }

  private List<ORID> applyTailIndexes(final Object lastIndexResult) {
    final OIndex beforeTheLastIndex = indexChain.get(indexChain.size() - 2);
    Set<Comparable> currentKeys = prepareKeys(beforeTheLastIndex, lastIndexResult);

    for (int j = indexChain.size() - 2; j > 0; j--) {
      final OIndex currentIndex = indexChain.get(j);
      final OIndex nextIndex = indexChain.get(j - 1);

      final Set<Comparable> newKeys;
      if (isComposite(currentIndex)) {
        //noinspection ObjectAllocationInLoop
        newKeys = new TreeSet<>();
        for (Comparable currentKey : currentKeys) {
          final List<ORID> currentResult = getFromCompositeIndex(currentKey, currentIndex);
          newKeys.addAll(prepareKeys(nextIndex, currentResult));
        }
      } else {
        final List<OIdentifiable> keys;
        try (Stream<ORawPair<Object, ORID>> stream =
            currentIndex.getInternal().streamEntries(currentKeys, true)) {
          keys = stream.map((pair) -> pair.second).collect(Collectors.toList());
        }
        newKeys = prepareKeys(nextIndex, keys);
      }

      updateStatistic(currentIndex);

      currentKeys = newKeys;
    }

    return applyFirstIndex(currentKeys);
  }

  private List<ORID> applyFirstIndex(Collection<Comparable> currentKeys) {
    final List<ORID> result;
    if (isComposite(firstIndex)) {
      result = new ArrayList<>();
      for (Comparable key : currentKeys) {
        result.addAll(getFromCompositeIndex(key, firstIndex));
      }
    } else {
      try (Stream<ORawPair<Object, ORID>> stream =
          firstIndex.getInternal().streamEntries(currentKeys, true)) {
        result = stream.map((pair) -> pair.second).collect(Collectors.toList());
      }
    }

    updateStatistic(firstIndex);

    return result;
  }

  private static List<ORID> getFromCompositeIndex(Comparable currentKey, OIndex currentIndex) {
    try (Stream<ORawPair<Object, ORID>> stream =
        currentIndex.getInternal().streamEntriesBetween(currentKey, true, currentKey, true, true)) {
      return stream.map((pair) -> pair.second).collect(Collectors.toList());
    }
  }

  /**
   * Make type conversion of keys for specific index.
   *
   * @param index - index for which keys prepared for.
   * @param keys - which should be prepared.
   * @return keys converted to necessary type.
   */
  private static Set<Comparable> prepareKeys(OIndex index, Object keys) {
    final OIndexDefinition indexDefinition = index.getDefinition();
    if (keys instanceof Collection) {
      final Set<Comparable> newKeys = new TreeSet<>();
      for (Object o : ((Collection) keys)) {
        newKeys.add((Comparable) indexDefinition.createValue(o));
      }
      return newKeys;
    } else {
      return Collections.singleton((Comparable) indexDefinition.createValue(keys));
    }
  }

  /**
   * Register statistic information about usage of index in {@link OProfilerStub}.
   *
   * @param index which usage is registering.
   */
  private static void updateStatistic(OIndex index) {

    final OProfiler profiler = Orient.instance().getProfiler();
    if (profiler.isRecording()) {
      Orient.instance()
          .getProfiler()
          .updateCounter(
              profiler.getDatabaseMetric(index.getDatabaseName(), "query.indexUsed"),
              "Used index in query",
              +1);

      final int paramCount = index.getDefinition().getParamCount();
      if (paramCount > 1) {
        final String profiler_prefix =
            profiler.getDatabaseMetric(index.getDatabaseName(), "query.compositeIndexUsed");
        profiler.updateCounter(profiler_prefix, "Used composite index in query", +1);
        profiler.updateCounter(
            profiler_prefix + "." + paramCount,
            "Used composite index in query with " + paramCount + " params",
            +1);
      }
    }
  }

  //
  // Following methods are not allowed for proxy.
  //

  @Override
  public OIndex create(
      OIndexMetadata indexMetadat, boolean rebuild, OProgressListener progressListener) {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  public OType[] getKeyTypes() {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  public Iterator<Map.Entry<Object, T>> iterator() {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  public OIndex put(Object key, OIdentifiable value) {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  public boolean remove(Object key) {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  public boolean remove(Object key, OIdentifiable rid) {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  /**
   * {@inheritDoc}
   *
   * @deprecated Manual indexes are deprecated and will be removed
   */
  @Deprecated
  @Override
  public OIndex clear() {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  @Override
  public long getSize() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long count(Object iKey) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getKeySize() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void flush() {}

  @Override
  public long getRebuildVersion() {
    return 0;
  }

  @Override
  public boolean isRebuilding() {
    return false;
  }

  @Override
  public Object getFirstKey() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object getLastKey() {
    throw new UnsupportedOperationException();
  }

  @Override
  public OIndexCursor cursor() {
    throw new UnsupportedOperationException();
  }

  @Override
  public OIndexCursor descCursor() {
    throw new UnsupportedOperationException();
  }

  @Override
  public OIndexKeyCursor keyCursor() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object getCollatingValue(Object key) {
    return this.lastIndex.getInternal().getCollatingValue(key);
  }

  @Override
  public boolean loadFromConfiguration(ODocument iConfig) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ODocument updateConfiguration() {
    throw new UnsupportedOperationException();
  }

  @Override
  public OIndex addCluster(String iClusterName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OIndex removeCluster(String iClusterName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean canBeUsedInEqualityOperators() {
    return this.lastIndex.getInternal().canBeUsedInEqualityOperators();
  }

  @Override
  public boolean hasRangeQuerySupport() {
    return this.lastIndex.getInternal().hasRangeQuerySupport();
  }

  @Override
  public OIndexMetadata loadMetadata(ODocument iConfig) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void preCommit(OIndexAbstract.IndexTxSnapshot snapshots) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addTxOperation(
      OIndexAbstract.IndexTxSnapshot snapshots, OTransactionIndexChanges changes) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void commit(OIndexAbstract.IndexTxSnapshot snapshots) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void postCommit(OIndexAbstract.IndexTxSnapshot snapshots) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setType(OType type) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getIndexNameByKey(Object key) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean acquireAtomicExclusiveLock(Object key) {
    throw new UnsupportedOperationException();
  }

  public long size() {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  public OIndex delete() {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  public String getType() {
    return lastIndex.getType();
  }

  @Override
  public String getAlgorithm() {
    return lastIndex.getAlgorithm();
  }

  public boolean isAutomatic() {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  public long rebuild() {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  public long rebuild(OProgressListener iProgressListener) {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  public ODocument getConfiguration() {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  @Override
  public ODocument getMetadata() {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  public Set<String> getClusters() {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  @Override
  public OIndexCursor iterateEntries(Collection<?> keys, boolean ascSortOrder) {
    return null;
  }

  @Override
  public OIndexCursor iterateEntriesBetween(
      Object fromKey, boolean fromInclusive, Object toKey, boolean toInclusive, boolean ascOrder) {
    return null;
  }

  @Override
  public OIndexCursor iterateEntriesMajor(Object fromKey, boolean fromInclusive, boolean ascOrder) {
    return null;
  }

  @Override
  public OIndexCursor iterateEntriesMinor(Object toKey, boolean toInclusive, boolean ascOrder) {
    return null;
  }

  @Override
  public int getIndexId() {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  @Override
  public boolean isUnique() {
    return firstIndex.isUnique();
  }

  @Override
  public Stream<ORawPair<Object, ORID>> stream() {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  @Override
  public Stream<ORawPair<Object, ORID>> descStream() {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  @Override
  public Stream<Object> keyStream() {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  @Override
  public int getVersion() {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  public boolean supportsOrderedIterations() {
    return false;
  }

  @Override
  public Stream<ORawPair<Object, ORID>> streamEntries(Collection<?> keys, boolean ascSortOrder) {
    return applyTailIndexes(lastIndex.getInternal().streamEntries(keys, ascSortOrder));
  }

  @Override
  public Stream<ORawPair<Object, ORID>> streamEntriesBetween(
      Object fromKey, boolean fromInclusive, Object toKey, boolean toInclusive, boolean ascOrder) {
    return applyTailIndexes(
        lastIndex
            .getInternal()
            .streamEntriesBetween(fromKey, fromInclusive, toKey, toInclusive, ascOrder));
  }

  @Override
  public Stream<ORawPair<Object, ORID>> streamEntriesMajor(
      Object fromKey, boolean fromInclusive, boolean ascOrder) {
    return applyTailIndexes(
        lastIndex.getInternal().streamEntriesMajor(fromKey, fromInclusive, ascOrder));
  }

  @Override
  public Stream<ORawPair<Object, ORID>> streamEntriesMinor(
      Object toKey, boolean toInclusive, boolean ascOrder) {
    return applyTailIndexes(
        lastIndex.getInternal().streamEntriesMinor(toKey, toInclusive, ascOrder));
  }

  @Override
  public boolean isNativeTxSupported() {
    return false;
  }

  @Override
  public Iterable<OTransactionIndexChangesPerKey.OTransactionIndexEntry> interpretTxKeyChanges(
      OTransactionIndexChangesPerKey changes) {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  @Override
  public void doPut(OAbstractPaginatedStorage storage, Object key, ORID rid) {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  @Override
  public boolean doRemove(OAbstractPaginatedStorage storage, Object key, ORID rid) {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  @Override
  public boolean doRemove(OAbstractPaginatedStorage storage, Object key) {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  private Stream<ORawPair<Object, ORID>> applyTailIndexes(
      Stream<ORawPair<Object, ORID>> indexStream) {
    //noinspection resource
    return indexStream.flatMap(
        (entry) -> applyTailIndexes(entry.second).stream().map((rid) -> new ORawPair<>(null, rid)));
  }

  @Override
  public int compareTo(OIndex o) {
    throw new UnsupportedOperationException();
  }
}
