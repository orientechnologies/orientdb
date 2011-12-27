package com.orientechnologies.orient.core.sql;

import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.orient.core.db.ODatabaseComplex;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.*;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItemField;

import java.util.*;

/**
 * @author Artem Orobets
 */
public class OIndexProxy<T> implements OIndex<T> {
    private final OIndex<T> index;

    private final List<OIndex<?>> indexChain;
    private final OIndex<?> lastIndex;

    /**
     * Create proxies that support maximum number of different operations.
     *
     * @param index - the index which proxies created for
     * @param longChain - property chain from the query, which should be evaluated
     * @param database - current database instance
     * @return - proxies needed to process query.
     */
    public static <T> Collection<OIndexProxy<T>> createdProxy(OIndex<T> index, OSQLFilterItemField.FieldChain longChain,
                                                              ODatabaseComplex database) {
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

    /**
     * {@inheritDoc}
     */
    public T get(Object iKey) {
        final Object result = lastIndex.get(iKey);

        return applyTailIndexes(result, -1);
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
    public Collection<OIdentifiable> getValuesBetween(Object iRangeFrom, boolean iFromInclusive, Object iRangeTo, boolean iToInclusive, int maxValuesToFetch) {
        final Object result = lastIndex.getValuesBetween(iRangeFrom, iFromInclusive, iRangeTo, iToInclusive, maxValuesToFetch);

        return (Collection<OIdentifiable>) applyTailIndexes(result, maxValuesToFetch);
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
        final Object result = lastIndex.getValuesMajor(fromKey, isInclusive, maxValuesToFetch);

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
        final Object result = lastIndex.getValuesMinor(toKey, isInclusive, maxValuesToFetch);

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
        final Object result = lastIndex.getValues(iKeys, maxValuesToFetch);

        return (Collection<OIdentifiable>) applyTailIndexes(result, maxValuesToFetch);
    }

    /**
     * Returns internal index of last chain index, because proxy applicable to all
     * operations that last index applicable.
     */
    @SuppressWarnings("unchecked")
    public OIndexInternal<T> getInternal() {
        return (OIndexInternal<T>) lastIndex.getInternal();
    }

    /**
     * {@inheritDoc}
     */
    public OIndexDefinition getDefinition() {
        return indexChain.get(indexChain.size() - 1).getDefinition();
    }

    private T applyTailIndexes(final Object result, int maxValuesToFetch) {
        final Set<Comparable> keys = prepareKeys(indexChain.size() - 1, result, maxValuesToFetch);

        Set<Comparable> currentKeys = keys;
        for (int j = indexChain.size() - 2; j > 0; j--) {
            Set<Comparable> newKeys = new TreeSet<Comparable>();

            for (Comparable currentKey : currentKeys) {
                final OIndex<?> currentIndex = indexChain.get(j);
                final Object currentResult = currentIndex.get(currentKey);

                final Set<Comparable> preparedKeys;
                preparedKeys = prepareKeys(j, currentResult, maxValuesToFetch);
                newKeys.addAll(preparedKeys);

                maxValuesToFetch -= preparedKeys.size();
            }
            currentKeys = newKeys;
        }

        return applyMainIndex(currentKeys, maxValuesToFetch);
    }

    private Set<Comparable> convertResult(Object result, Class<?> targetType, int maxValuesToFetch) {
        final Set<Comparable> newKeys;
        if (result instanceof Set) {
            newKeys = new TreeSet<Comparable>();
            for (Object o : ((Set) result)) {
                newKeys.add((Comparable) OType.convert(o, targetType));
                if (maxValuesToFetch != -1 && newKeys.size() >= maxValuesToFetch) {
                    break;
                }
            }
            return newKeys;
        } else {
            return Collections.singleton((Comparable) OType.convert(result, targetType));
        }
    }

    private Set<Comparable> prepareKeys(int forIndex, Object result, int maxValuesToFetch) {
        final OIndex<?> previousIndex = indexChain.get(forIndex - 1);
        final Class<?> targetType = previousIndex.getKeyTypes()[0].getDefaultJavaType();

        return convertResult(result, targetType, maxValuesToFetch);
    }

    private T applyMainIndex(Collection<Comparable> currentKeys, int maxValuesToFetch) {
        if (currentKeys.size() > 1) {
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
            return (T) resultSet;
        } else {
            final Comparable key = currentKeys.iterator().next();
            return index.get(index.getDefinition().createValue(key));
        }
    }

    private static Iterable<List<OIndex<?>>> getIndexesForChain(OIndex<?> index, OSQLFilterItemField.FieldChain fieldChain, ODatabaseComplex database) {
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

    private static Collection<OIndex<?>> prepareLastIndexVariants(OIndex<?> index, OSQLFilterItemField.FieldChain fieldChain, ODatabaseComplex database) {
        OClass oClass = database.getMetadata().getSchema().getClass(index.getDefinition().getClassName());
        for (int i = 0; i < fieldChain.getItemCount() - 1; i++) {
            oClass = oClass.getProperty(fieldChain.getItemName(i)).getLinkedClass();
        }

        final Set<OIndex<?>> involvedIndexes = oClass.getInvolvedIndexes(fieldChain.getItemName(fieldChain.getItemCount() - 1));
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

    private static List<OIndex<?>> prepareBaseIndexes(OIndex<?> index, OSQLFilterItemField.FieldChain fieldChain, ODatabaseComplex database) {
        List<OIndex<?>> result = new ArrayList<OIndex<?>>(fieldChain.getItemCount() - 2);

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

    //
    // Following methods are not allowed for proxy.
    //

    public OIndex<?> create(String iName, OIndexDefinition iIndexDefinition, ODatabaseRecord iDatabase, String iClusterIndexName, int[] iClusterIdsToIndex, OProgressListener iProgressListener) {
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

    public OIndex<T> lazySave() {
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
}
