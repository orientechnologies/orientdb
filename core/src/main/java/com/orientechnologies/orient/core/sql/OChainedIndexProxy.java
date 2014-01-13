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

import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.common.profiler.OProfilerMBean;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabaseComplex;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexInternal;
import com.orientechnologies.orient.core.index.OIndexNotUnique;
import com.orientechnologies.orient.core.index.OIndexOneValue;
import com.orientechnologies.orient.core.index.OIndexUnique;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItemField;

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
  private final OIndex<T>       index;

  private final List<OIndex<?>> indexChain;
  private final OIndex<?>       lastIndex;
  private final boolean         isOneValue;

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
    Collection<OChainedIndexProxy<T>> proxies = new ArrayList<OChainedIndexProxy<T>>();

    for (List<OIndex<?>> indexChain : getIndexesForChain(index, longChain, database)) {
      proxies.add(new OChainedIndexProxy<T>(index, indexChain));
    }

    return proxies;
  }

  private OChainedIndexProxy(OIndex<T> index, List<OIndex<?>> indexChain) {
    this.index = index;
    this.indexChain = Collections.unmodifiableList(indexChain);
    lastIndex = indexChain.get(indexChain.size() - 1);

    isOneValue = isAllOneValue(indexChain);
  }

  private boolean isAllOneValue(List<OIndex<?>> indexChain) {
    for (OIndex<?> oIndex : indexChain) {
      if (!(oIndex.getInternal() instanceof OIndexOneValue))
        return false;
    }
    return true;
  }

  public String getDatabaseName() {
    return index.getDatabaseName();
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

    applyTailIndexes(lastIndexResult, new IndexValuesResultListener() {
      @Override
      public boolean addResult(OIdentifiable value) {
        result.add(value);
        return true;
      }
    });

    if (isOneValue)
      return (T) (result.isEmpty() ? null : result.iterator().next());

    return (T) result;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Collection<OIdentifiable> getValuesBetween(Object iRangeFrom, Object iRangeTo) {
    final Set<OIdentifiable> result = new HashSet<OIdentifiable>();
    final Object lastIndexValuesBetween = lastIndex.getValuesBetween(iRangeFrom, iRangeTo);

    applyTailIndexes(lastIndexValuesBetween, new IndexValuesResultListener() {
      @Override
      public boolean addResult(OIdentifiable value) {
        result.add(value);
        return true;
      }
    });

    return result;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Collection<OIdentifiable> getValuesBetween(Object iRangeFrom, boolean iFromInclusive, Object iRangeTo, boolean iToInclusive) {
    final Set<OIdentifiable> result = new HashSet<OIdentifiable>();

    final Object lastIndexValuesBetween = lastIndex.getValuesBetween(iRangeFrom, iFromInclusive, iRangeTo, iToInclusive);

    applyTailIndexes(lastIndexValuesBetween, new IndexValuesResultListener() {
      @Override
      public boolean addResult(OIdentifiable value) {
        result.add(value);
        return true;
      }
    });

    return result;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void getValuesBetween(Object iRangeFrom, boolean iFromInclusive, Object iRangeTo, boolean iToInclusive,
      IndexValuesResultListener resultListener) {
    final Object result = lastIndex.getValuesBetween(iRangeFrom, iFromInclusive, iRangeTo, iToInclusive);

    applyTailIndexes(result, resultListener);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Collection<OIdentifiable> getValuesMajor(Object fromKey, boolean isInclusive) {
    final Set<OIdentifiable> result = new HashSet<OIdentifiable>();

    final Object lastIndexValuesMajor = lastIndex.getValuesMajor(fromKey, isInclusive);

    applyTailIndexes(lastIndexValuesMajor, new IndexValuesResultListener() {
      @Override
      public boolean addResult(OIdentifiable value) {
        result.add(value);
        return true;
      }
    });

    return result;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void getValuesMajor(Object fromKey, boolean isInclusive, IndexValuesResultListener resultListener) {
    final Object result = lastIndex.getValuesMajor(fromKey, isInclusive);

    applyTailIndexes(result, resultListener);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Collection<OIdentifiable> getValuesMinor(Object toKey, boolean isInclusive) {
    final Set<OIdentifiable> result = new HashSet<OIdentifiable>();

    final Object lastIndexValuesMinor = lastIndex.getValuesMinor(toKey, isInclusive);

    applyTailIndexes(lastIndexValuesMinor, new IndexValuesResultListener() {
      @Override
      public boolean addResult(OIdentifiable value) {
        result.add(value);
        return true;
      }
    });

    return result;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void getValuesMinor(Object toKey, boolean isInclusive, IndexValuesResultListener resultListener) {
    final Object result = lastIndex.getValuesMinor(toKey, isInclusive);

    applyTailIndexes(result, resultListener);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Collection<OIdentifiable> getValues(Collection<?> iKeys) {
    final Set<OIdentifiable> result = new HashSet<OIdentifiable>();

    final Object lastIndexResult = lastIndex.getValues(iKeys);

    applyTailIndexes(lastIndexResult, new IndexValuesResultListener() {
      @Override
      public boolean addResult(OIdentifiable value) {
        result.add(value);
        return true;
      }
    });

    return result;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void getValues(Collection<?> iKeys, IndexValuesResultListener resultListener) {
    final Object result = lastIndex.getValues(iKeys);

    applyTailIndexes(result, resultListener);
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

  private void applyTailIndexes(final Object result, IndexValuesResultListener resultListener) {
    final OIndex<?> previousIndex = indexChain.get(indexChain.size() - 2);
    Set<Comparable> currentKeys = prepareKeys(previousIndex, result);
    for (int j = indexChain.size() - 2; j > 0; j--) {
      Set<Comparable> newKeys = new TreeSet<Comparable>();

      final OIndex<?> currentIndex = indexChain.get(j);
      for (Comparable currentKey : currentKeys) {
        final Object currentResult = currentIndex.get(currentKey);

        final Set<Comparable> preparedKeys;
        preparedKeys = prepareKeys(indexChain.get(j - 1), currentResult);
        newKeys.addAll(preparedKeys);
      }

      updateStatistic(currentIndex);

      currentKeys = newKeys;
    }

    applyMainIndex(currentKeys, resultListener);
  }

  private Set<Comparable> convertResult(Object result, Class<?> targetType) {
    final Set<Comparable> newKeys;
    if (result instanceof Set) {
      newKeys = new TreeSet<Comparable>();
      for (Object o : ((Set) result)) {
        newKeys.add((Comparable) OType.convert(o, targetType));
      }
      return newKeys;
    } else {
      return Collections.singleton((Comparable) OType.convert(result, targetType));
    }
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
    final Class<?> targetType = index.getKeyTypes()[0].getDefaultJavaType();

    return convertResult(keys, targetType);
  }

  private void applyMainIndex(Iterable<Comparable> currentKeys, IndexValuesResultListener resultListener) {
    keysLoop: for (Comparable key : currentKeys) {
      final T result = index.get(index.getDefinition().createValue(key));
      if (result instanceof Set) {
        for (T o : (Set<T>) result) {
          if (!resultListener.addResult((OIdentifiable) o))
            break keysLoop;
        }
      } else {
        if (!resultListener.addResult((OIdentifiable) result))
          break;
      }
    }

    updateStatistic(index);
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
      OIndexInternal<?> bestInternalIndex = index.getInternal();
      if (bestInternalIndex instanceof OIndexUnique || bestInternalIndex instanceof OIndexNotUnique) {
        return index;
      }
    }
    return bestIndex;
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
    index.checkEntry(iRecord, iKey);
  }

  //
  // Following methods are not allowed for proxy.
  //

  @Override
  public OIndex<T> create(String name, OIndexDefinition indexDefinition, String clusterIndexName, Set<String> clustersToIndex,
      boolean rebuild, OProgressListener progressListener) {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  @Override
  public Collection<ODocument> getEntriesMajor(Object fromKey, boolean isInclusive) {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  @Override
  public void getEntriesMajor(Object fromKey, boolean isInclusive, IndexEntriesResultListener resultListener) {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  @Override
  public Collection<ODocument> getEntriesMinor(Object toKey, boolean isInclusive) {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  @Override
  public void getEntriesMinor(Object toKey, boolean isInclusive, IndexEntriesResultListener resultListener) {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  @Override
  public Collection<ODocument> getEntriesBetween(Object iRangeFrom, Object iRangeTo, boolean iInclusive) {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  @Override
  public void getEntriesBetween(Object iRangeFrom, Object iRangeTo, boolean iInclusive, IndexEntriesResultListener resultListener) {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  @Override
  public Collection<ODocument> getEntriesBetween(Object iRangeFrom, Object iRangeTo) {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  @Override
  public Collection<ODocument> getEntries(Collection<?> iKeys) {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  public void getEntries(Collection<?> iKeys, IndexEntriesResultListener resultListener) {
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

  public Iterator<Map.Entry<Object, T>> inverseIterator() {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  public Iterator<OIdentifiable> valuesIterator() {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  public Iterator<OIdentifiable> valuesInverseIterator() {
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

  public int remove(OIdentifiable iRID) {
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

  public void commit(ODocument iDocument) {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  public Set<String> getClusters() {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  public boolean supportsOrderedIterations() {
    return false;
  }

  @Override
  public boolean isRebuiding() {
    return false;
  }
}
