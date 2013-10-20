package com.orientechnologies.orient.core.sql;

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
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexInternal;
import com.orientechnologies.orient.core.index.OIndexNotUnique;
import com.orientechnologies.orient.core.index.OIndexOneValue;
import com.orientechnologies.orient.core.index.OIndexUnique;
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
public class OIndexProxy<T> implements OIndex<T> {
  private final OIndex<T>       index;

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
  public static <T> Collection<OIndexProxy<T>> createdProxy(OIndex<T> index, OSQLFilterItemField.FieldChain longChain,
      ODatabaseComplex<?> database) {
    Collection<OIndexProxy<T>> proxies = new ArrayList<OIndexProxy<T>>();

    for (List<OIndex<?>> indexChain : getIndexesForChain(index, longChain, database)) {
      proxies.add(new OIndexProxy<T>(index, indexChain));
    }

    return proxies;
  }

  private OIndexProxy(OIndex<T> index, List<OIndex<?>> indexChain) {
    this.index = index;
    this.indexChain = Collections.unmodifiableList(indexChain);
    lastIndex = indexChain.get(indexChain.size() - 1);
  }

  public String getDatabaseName() {
    return index.getDatabaseName();
  }

  /**
   * {@inheritDoc}
   */
  public T get(Object iKey) {
    final Object result = lastIndex.get(iKey);

    final Collection<T> resultSet = applyTailIndexes(result, -1);
    if (getInternal() instanceof OIndexOneValue && resultSet.size() == 1) {
      return resultSet.iterator().next();
    } else {
      return (T) resultSet;
    }
  }

  public long count(Object iKey) {
    return index.count(iKey);
  }

  /**
   * {@inheritDoc}
   */
  public Collection<OIdentifiable> getValuesBetween(Object iRangeFrom, Object iRangeTo) {
    final Object result = lastIndex.getValuesBetween(iRangeFrom, iRangeTo);

    return (Collection<OIdentifiable>) applyTailIndexes(result, -1);
  }

  /**
   * {@inheritDoc}
   */
  public Collection<OIdentifiable> getValuesBetween(Object iRangeFrom, boolean iFromInclusive, Object iRangeTo, boolean iToInclusive) {
    final Object result = lastIndex.getValuesBetween(iRangeFrom, iFromInclusive, iRangeTo, iToInclusive);

    return (Collection<OIdentifiable>) applyTailIndexes(result, -1);
  }

  /**
   * {@inheritDoc}
   */
  public Collection<OIdentifiable> getValuesBetween(Object iRangeFrom, boolean iFromInclusive, Object iRangeTo,
      boolean iToInclusive, int maxValuesToFetch) {
    final Object result = lastIndex.getValuesBetween(iRangeFrom, iFromInclusive, iRangeTo, iToInclusive);

    return (Collection<OIdentifiable>) applyTailIndexes(result, maxValuesToFetch);
  }

  @Override
  public long count(final Object iRangeFrom, final boolean iFromInclusive, final Object iRangeTo, final boolean iToInclusive,
      final int maxValuesToFetch) {
    return lastIndex.count(iRangeFrom, iFromInclusive, iRangeTo, iToInclusive, maxValuesToFetch);
  }

  /**
   * {@inheritDoc}
   */
  public Collection<OIdentifiable> getValuesMajor(Object fromKey, boolean isInclusive) {
    final Object result = lastIndex.getValuesMajor(fromKey, isInclusive);

    return (Collection<OIdentifiable>) applyTailIndexes(result, -1);
  }

  /**
   * {@inheritDoc}
   */
  public Collection<OIdentifiable> getValuesMajor(Object fromKey, boolean isInclusive, int maxValuesToFetch) {
    final Object result = lastIndex.getValuesMajor(fromKey, isInclusive);

    return (Collection<OIdentifiable>) applyTailIndexes(result, maxValuesToFetch);
  }

  /**
   * {@inheritDoc}
   */
  public Collection<OIdentifiable> getValuesMinor(Object toKey, boolean isInclusive) {
    final Object result = lastIndex.getValuesMinor(toKey, isInclusive);

    return (Collection<OIdentifiable>) applyTailIndexes(result, -1);
  }

  /**
   * {@inheritDoc}
   */
  public Collection<OIdentifiable> getValuesMinor(Object toKey, boolean isInclusive, int maxValuesToFetch) {
    final Object result = lastIndex.getValuesMinor(toKey, isInclusive);

    return (Collection<OIdentifiable>) applyTailIndexes(result, maxValuesToFetch);
  }

  /**
   * {@inheritDoc}
   */
  public Collection<OIdentifiable> getValues(Collection<?> iKeys) {
    final Object result = lastIndex.getValues(iKeys);

    return (Collection<OIdentifiable>) applyTailIndexes(result, -1);
  }

  /**
   * {@inheritDoc}
   */
  public Collection<OIdentifiable> getValues(Collection<?> iKeys, int maxValuesToFetch) {
    final Object result = lastIndex.getValues(iKeys);

    return (Collection<OIdentifiable>) applyTailIndexes(result, maxValuesToFetch);
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
    return indexChain.get(indexChain.size() - 1).getDefinition();
  }

  private Collection<T> applyTailIndexes(final Object result, int maxValuesToFetch) {
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

    return applyMainIndex(currentKeys, maxValuesToFetch);
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

  private Collection<T> applyMainIndex(Iterable<Comparable> currentKeys, int maxValuesToFetch) {
    Collection<T> resultSet = new TreeSet<T>();
    for (Comparable key : currentKeys) {
      final T result = index.get(index.getDefinition().createValue(key));
      if (result instanceof Set) {
        for (T o : (Set<T>) result) {
          resultSet.add(o);
          if (maxValuesToFetch != -1 && resultSet.size() >= maxValuesToFetch) {
            break;
          }
        }
      } else {
        resultSet.add(result);
      }
      if (maxValuesToFetch != -1 && resultSet.size() >= maxValuesToFetch) {
        break;
      }
    }

    updateStatistic(index);

    return resultSet;
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

  public OIndex<T> create(String name, OIndexDefinition indexDefinition, String clusterIndexName, Set<String> clustersToIndex,
      boolean rebuild, OProgressListener progressListener) {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  public Collection<ODocument> getEntriesMajor(Object fromKey, boolean isInclusive) {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  public Collection<ODocument> getEntriesMajor(Object fromKey, boolean isInclusive, int maxEntriesToFetch) {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  public Collection<ODocument> getEntriesMinor(Object toKey, boolean isInclusive) {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  public Collection<ODocument> getEntriesMinor(Object toKey, boolean isInclusive, int maxEntriesToFetch) {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  public Collection<ODocument> getEntriesBetween(Object iRangeFrom, Object iRangeTo, boolean iInclusive) {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  public Collection<ODocument> getEntriesBetween(Object iRangeFrom, Object iRangeTo, boolean iInclusive, int maxEntriesToFetch) {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  public Collection<ODocument> getEntriesBetween(Object iRangeFrom, Object iRangeTo) {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  public Collection<ODocument> getEntries(Collection<?> iKeys) {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  public Collection<ODocument> getEntries(Collection<?> iKeys, int maxEntriesToFetch) {
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

  public boolean remove(Object iKey) {
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

  public String getName() {
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

  public void automaticRebuild() {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  public long rebuild(OProgressListener iProgressListener) {
    throw new UnsupportedOperationException("Not allowed operation");
  }

  public ODocument getConfiguration() {
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
