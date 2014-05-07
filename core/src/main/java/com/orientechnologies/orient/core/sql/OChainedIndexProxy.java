/*
 * Copyright 2010-2013 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.orient.core.sql;

import static com.orientechnologies.orient.core.metadata.schema.OClass.INDEX_TYPE.NOTUNIQUE_HASH_INDEX;
import static com.orientechnologies.orient.core.metadata.schema.OClass.INDEX_TYPE.UNIQUE_HASH_INDEX;

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

import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.common.profiler.OProfilerMBean;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabaseComplex;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexAbstractCursor;
import com.orientechnologies.orient.core.index.OIndexCursor;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexInternal;
import com.orientechnologies.orient.core.index.OIndexKeyCursor;
import com.orientechnologies.orient.core.index.OIndexNotUnique;
import com.orientechnologies.orient.core.index.OIndexUnique;
import com.orientechnologies.orient.core.iterator.OEmptyIterator;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItemField;

/**
 * <p>
 * There are some cases when we need to create index for some class by traversed property. Unfortunately, such functionality is not
 * supported yet. But we can do that by creating index for each element of {@link OSQLFilterItemField.FieldChain} (which define
 * "way" to our property), and then process operations consequently using previously created indexes.
 * </p>
 * <p>
 * This class provides possibility to find optimal chain of indexes and then use it just like it was index for traversed property.
 * </p>
 * <p>
 * IMPORTANT: this class is only for internal usage!
 * </p>
 * 
 * @author Artem Orobets
 */

@SuppressWarnings({ "unchecked", "rawtypes" })
public class OChainedIndexProxy<T> implements OIndex<T> {
  private final OIndex<T>       firstIndex;

  private final List<OIndex<?>> indexChain;
  private final OIndex<?>       lastIndex;

  /**
   * Create proxies that support maximum number of different operations. In case when several different indexes which support
   * different operations (e.g. indexes of {@code UNIQUE} and {@code FULLTEXT} types) are possible, the creates the only one index
   * of each type.
   * 
   * @param index
   *          - the index which proxies created for
   * @param longChain
   *          - property chain from the query, which should be evaluated
   * @param database
   *          - current database instance
   * @return proxies needed to process query.
   */
  public static <T> Collection<OChainedIndexProxy<T>> createdProxy(OIndex<T> index, OSQLFilterItemField.FieldChain longChain,
      ODatabaseComplex<?> database) {
    if (!isAppropriateAsBase(index))
      return Collections.emptyList();

    Collection<OChainedIndexProxy<T>> proxies = new ArrayList<OChainedIndexProxy<T>>();

    for (List<OIndex<?>> indexChain : getIndexesForChain(index, longChain, database)) {
      proxies.add(new OChainedIndexProxy<T>(index, indexChain));
    }

    return proxies;
  }

  private OChainedIndexProxy(OIndex<T> firstIndex, List<OIndex<?>> indexChain) {
    this.firstIndex = firstIndex;
    this.indexChain = Collections.unmodifiableList(indexChain);
    lastIndex = indexChain.get(indexChain.size() - 1);
  }

  public String getDatabaseName() {
    return firstIndex.getDatabaseName();
  }

  public List<String> getIndexNames() {
    final ArrayList<String> names = new ArrayList<String>(indexChain.size());
    for (OIndex<?> oIndex : indexChain) {
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
      if (i > 0)
        res.append(", ");
      res.append(indexName);
    }

    res.append("}");

    return res.toString();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public T get(Object iKey) {
    final Object lastIndexResult = lastIndex.get(iKey);

    final Set<OIdentifiable> result = new HashSet<OIdentifiable>();

    result.addAll(applyTailIndexes(lastIndexResult));

    return (T) result;
  }

  /**
   * Returns internal index of last chain index, because proxy applicable to all operations that last index applicable.
   */
  public OIndexInternal<T> getInternal() {
    return (OIndexInternal<T>) lastIndex.getInternal();
  }

  /**
   * {@inheritDoc}
   */
  public OIndexDefinition getDefinition() {
    return lastIndex.getDefinition();
  }

  private List<OIdentifiable> applyTailIndexes(final Object lastIndexResult) {
    final OIndex<?> beforeTheLastIndex = indexChain.get(indexChain.size() - 2);
    Set<Comparable> currentKeys = prepareKeys(beforeTheLastIndex, lastIndexResult);

    for (int j = indexChain.size() - 2; j > 0; j--) {
      final OIndex<?> currentIndex = indexChain.get(j);
      final OIndex<?> nextIndex = indexChain.get(j - 1);

      final Set<Comparable> newKeys;
      if (isComposite(currentIndex)) {
        newKeys = new TreeSet<Comparable>();
        for (Comparable currentKey : currentKeys) {
          final List<OIdentifiable> currentResult = getFromCompositeIndex(currentKey, currentIndex);
          newKeys.addAll(prepareKeys(nextIndex, currentResult));
        }
      } else {
        final OIndexCursor cursor = currentIndex.iterateEntries(currentKeys, true);
        final List<OIdentifiable> keys = cursorToList(cursor);
        newKeys = prepareKeys(nextIndex, keys);
      }

      updateStatistic(currentIndex);

      currentKeys = newKeys;
    }

    return applyFirstIndex(currentKeys);
  }

  private List<OIdentifiable> applyFirstIndex(Collection<Comparable> currentKeys) {
    final List<OIdentifiable> result;
    if (isComposite(firstIndex)) {
      result = new ArrayList<OIdentifiable>();
      for (Comparable key : currentKeys) {
        result.addAll(getFromCompositeIndex(key, firstIndex));
      }
    } else {
      final OIndexCursor cursor = firstIndex.iterateEntries(currentKeys, true);

      result = cursorToList(cursor);
    }

    updateStatistic(firstIndex);

    return result;
  }

  private static boolean isComposite(OIndex<?> currentIndex) {
    return currentIndex.getDefinition().getParamCount() > 1;
  }

  private List<OIdentifiable> getFromCompositeIndex(Comparable currentKey, OIndex<?> currentIndex) {
    final OIndexCursor cursor = currentIndex.iterateEntriesBetween(currentKey, true, currentKey, true, true);

    return cursorToList(cursor);
  }

  private List<OIdentifiable> cursorToList(OIndexCursor cursor) {
    final List<OIdentifiable> currentResult = new ArrayList<OIdentifiable>();
    Map.Entry<Object, OIdentifiable> entry = cursor.nextEntry();
    while (entry != null) {
      currentResult.add(entry.getValue());
      entry = cursor.nextEntry();
    }
    return currentResult;
  }

  /**
   * Make type conversion of keys for specific index.
   * 
   * @param index
   *          - index for which keys prepared for.
   * @param keys
   *          - which should be prepared.
   * @return keys converted to necessary type.
   */
  private Set<Comparable> prepareKeys(OIndex<?> index, Object keys) {
    final OIndexDefinition indexDefinition = index.getDefinition();
    if (keys instanceof Collection) {
      final Set<Comparable> newKeys = new TreeSet<Comparable>();
      for (Object o : ((Collection) keys)) {
        newKeys.add((Comparable) indexDefinition.createValue(o));
      }
      return newKeys;
    } else {
      return Collections.singleton((Comparable) indexDefinition.createValue(keys));
    }
  }

  private static Iterable<List<OIndex<?>>> getIndexesForChain(OIndex<?> index, OSQLFilterItemField.FieldChain fieldChain,
      ODatabaseComplex<?> database) {
    List<OIndex<?>> baseIndexes = prepareBaseIndexes(index, fieldChain, database);

    Collection<OIndex<?>> lastIndexes = prepareLastIndexVariants(index, fieldChain, database);

    Collection<List<OIndex<?>>> result = new ArrayList<List<OIndex<?>>>();
    for (OIndex<?> lastIndex : lastIndexes) {
      final List<OIndex<?>> indexes = new ArrayList<OIndex<?>>(fieldChain.getItemCount());
      indexes.addAll(baseIndexes);
      indexes.add(lastIndex);

      result.add(indexes);
    }

    return result;
  }

  private static Collection<OIndex<?>> prepareLastIndexVariants(OIndex<?> index, OSQLFilterItemField.FieldChain fieldChain,
      ODatabaseComplex<?> database) {
    OClass oClass = database.getMetadata().getSchema().getClass(index.getDefinition().getClassName());
    for (int i = 0; i < fieldChain.getItemCount() - 1; i++) {
      oClass = oClass.getProperty(fieldChain.getItemName(i)).getLinkedClass();
    }

    final Set<OIndex<?>> involvedIndexes = new TreeSet<OIndex<?>>(new Comparator<OIndex<?>>() {
      public int compare(OIndex<?> o1, OIndex<?> o2) {
        return o1.getDefinition().getParamCount() - o2.getDefinition().getParamCount();
      }
    });

    involvedIndexes.addAll(oClass.getInvolvedIndexes(fieldChain.getItemName(fieldChain.getItemCount() - 1)));
    final Collection<Class<? extends OIndex>> indexTypes = new HashSet<Class<? extends OIndex>>(3);
    final Collection<OIndex<?>> result = new ArrayList<OIndex<?>>();

    for (OIndex<?> involvedIndex : involvedIndexes) {
      if (!indexTypes.contains(involvedIndex.getInternal().getClass())) {
        result.add(involvedIndex);
        indexTypes.add(involvedIndex.getInternal().getClass());
      }
    }

    return result;
  }

  private static List<OIndex<?>> prepareBaseIndexes(OIndex<?> index, OSQLFilterItemField.FieldChain fieldChain,
      ODatabaseComplex<?> database) {
    List<OIndex<?>> result = new ArrayList<OIndex<?>>(fieldChain.getItemCount() - 1);

    result.add(index);

    OClass oClass = database.getMetadata().getSchema().getClass(index.getDefinition().getClassName());
    oClass = oClass.getProperty(fieldChain.getItemName(0)).getLinkedClass();
    for (int i = 1; i < fieldChain.getItemCount() - 1; i++) {
      final Set<OIndex<?>> involvedIndexes = oClass.getInvolvedIndexes(fieldChain.getItemName(i));
      final OIndex<?> bestIndex = findBestIndex(involvedIndexes);

      result.add(bestIndex);
      oClass = oClass.getProperty(fieldChain.getItemName(i)).getLinkedClass();
    }
    return result;
  }

  private static OIndex<?> findBestIndex(Iterable<OIndex<?>> involvedIndexes) {
    OIndex<?> bestIndex = null;
    for (OIndex<?> index : involvedIndexes) {
      bestIndex = index;
      if (isAppropriateAsBase(index)) {
        return index;
      }
    }
    return bestIndex;
  }

  /**
   * Check if index can be used as base index.
   * 
   * Requirements to the base index:
   * <ul>
   * <li>Should be unique or not unique. Other types can not be used to get all documents with required links.</li>
   * <li>Should not be composite hash index. As soon as hash index does not support partial match search.</li>
   * </ul>
   * 
   * @param index
   *          to check
   * @return true if index usage is allowed as base index.
   */
  private static boolean isAppropriateAsBase(OIndex<?> index) {
    OIndexInternal<?> internalIndex = index.getInternal();

    String type = index.getType();
    return (internalIndex instanceof OIndexUnique || internalIndex instanceof OIndexNotUnique)
        && !(isComposite(index) && (UNIQUE_HASH_INDEX.toString().equals(type) || NOTUNIQUE_HASH_INDEX.toString().equals(type)));
  }

  /**
   * Register statistic information about usage of index in {@link OProfiler}.
   * 
   * @param index
   *          which usage is registering.
   */
  private void updateStatistic(OIndex<?> index) {

    final OProfilerMBean profiler = Orient.instance().getProfiler();
    if (profiler.isRecording()) {
      Orient.instance().getProfiler()
          .updateCounter(profiler.getDatabaseMetric(index.getDatabaseName(), "query.indexUsed"), "Used index in query", +1);

      final int paramCount = index.getDefinition().getParamCount();
      if (paramCount > 1) {
        final String profiler_prefix = profiler.getDatabaseMetric(index.getDatabaseName(), "query.compositeIndexUsed");
        profiler.updateCounter(profiler_prefix, "Used composite index in query", +1);
        profiler.updateCounter(profiler_prefix + "." + paramCount, "Used composite index in query with " + paramCount + " params",
            +1);
      }
    }
  }

  public void checkEntry(final OIdentifiable iRecord, final Object iKey) {
    firstIndex.checkEntry(iRecord, iKey);
  }

  //
  // Following methods are not allowed for proxy.
  //

  @Override
  public OIndex<T> create(String name, OIndexDefinition indexDefinition, String clusterIndexName, Set<String> clustersToIndex,
      boolean rebuild, OProgressListener progressListener) {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  public boolean contains(Object iKey) {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  public void unload() {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  public OType[] getKeyTypes() {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  public Iterator<Map.Entry<Object, T>> iterator() {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  public OIndex<T> put(Object iKey, OIdentifiable iValue) {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  public boolean remove(Object key) {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  public boolean remove(Object iKey, OIdentifiable iRID) {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  public OIndex<T> clear() {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  public Iterable<Object> keys() {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  public long getSize() {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  public long getKeySize() {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  @Override
  public void flush() {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  public OIndex<T> delete() {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  @Override
  public void deleteWithoutIndexLoad(String indexName) {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  public String getType() {
    throw new UnsupportedOperationException("Not allowed operation");
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

  public ORID getIdentity() {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  public Set<String> getClusters() {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  @Override
  public Object getFirstKey() {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  @Override
  public Object getLastKey() {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  @Override
  public OIndexCursor cursor() {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  @Override
  public OIndexKeyCursor keyCursor() {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  public boolean supportsOrderedIterations() {
    return false;
  }

  @Override
  public OIndexCursor iterateEntries(Collection<?> keys, boolean ascSortOrder) {
    final OIndexCursor internalCursor = lastIndex.iterateEntries(keys, ascSortOrder);
    return new ExternalIndexCursor(internalCursor);
  }

  @Override
  public OIndexCursor iterateEntriesBetween(Object fromKey, boolean fromInclusive, Object toKey, boolean toInclusive,
      boolean ascOrder) {
    final OIndexCursor internalCursor = lastIndex.iterateEntriesBetween(fromKey, fromInclusive, toKey, toInclusive, ascOrder);
    return new ExternalIndexCursor(internalCursor);
  }

  @Override
  public OIndexCursor iterateEntriesMajor(Object fromKey, boolean fromInclusive, boolean ascOrder) {
    final OIndexCursor internalCursor = lastIndex.iterateEntriesMajor(fromKey, fromInclusive, ascOrder);
    return new ExternalIndexCursor(internalCursor);
  }

  @Override
  public OIndexCursor iterateEntriesMinor(Object toKey, boolean toInclusive, boolean ascOrder) {
    final OIndexCursor internalCursor = lastIndex.iterateEntriesMinor(toKey, toInclusive, ascOrder);
    return new ExternalIndexCursor(internalCursor);
  }

  @Override
  public boolean isRebuiding() {
    return false;
  }

  private final class ExternalIndexCursor extends OIndexAbstractCursor {
    private final OIndexCursor        internalCursor;

    private final List<OIdentifiable> queryResult     = new ArrayList<OIdentifiable>();
    private Iterator<OIdentifiable>   currentIterator = OEmptyIterator.IDENTIFIABLE_INSTANCE;

    private ExternalIndexCursor(OIndexCursor internalCursor) {
      this.internalCursor = internalCursor;
    }

    @Override
    public Map.Entry<Object, OIdentifiable> nextEntry() {
      if (currentIterator == null)
        return null;

      while (!currentIterator.hasNext()) {
        final Map.Entry<Object, OIdentifiable> entry = internalCursor.nextEntry();

        if (entry == null) {
          currentIterator = null;
          return null;
        }

        queryResult.clear();
        queryResult.addAll(applyTailIndexes(entry.getValue()));

        currentIterator = queryResult.iterator();
      }

      if (!currentIterator.hasNext()) {
        currentIterator = null;
        return null;
      }

      final OIdentifiable result = currentIterator.next();

      return new Map.Entry<Object, OIdentifiable>() {
        @Override
        public Object getKey() {
          throw new UnsupportedOperationException("getKey");
        }

        @Override
        public OIdentifiable getValue() {
          return result;
        }

        @Override
        public OIdentifiable setValue(OIdentifiable value) {
          throw new UnsupportedOperationException("setValue");
        }
      };
    }
  }
}
