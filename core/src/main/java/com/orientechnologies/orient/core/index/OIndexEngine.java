package com.orientechnologies.orient.core.index;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializer;

/**
 * @author Andrey Lomakin
 * @since 6/29/13
 */
public interface OIndexEngine<V> {
  void init();

  void flush();

  void create(String indexName, OIndexDefinition indexDefinition, String clusterIndexName, OStreamSerializer valueSerializer,
      boolean isAutomatic);

  void delete();

  void load(ORID indexRid, String indexName, boolean isAutomatic);

  boolean contains(Object key);

  boolean remove(Object key);

  ORID getIdentity();

  void clear();

  Iterator<Map.Entry<Object, V>> iterator();

  Iterator<Map.Entry<Object, V>> inverseIterator();

  Iterator<V> valuesIterator();

  Iterator<V> inverseValuesIterator();

  Iterable<Object> keys();

  void unload();

  void startTransaction();

  void stopTransaction();

  void afterTxRollback();

  void afterTxCommit();

  void closeDb();

  void close();

  void beforeTxBegin();

  V get(Object key);

  void put(Object key, V value);

  int removeValue(OIdentifiable value, ValuesTransformer<V> transformer);

  Collection<OIdentifiable> getValuesBetween(Object rangeFrom, boolean fromInclusive, Object rangeTo, boolean toInclusive,
      int maxValuesToFetch, ValuesTransformer<V> transformer);

  Collection<OIdentifiable> getValuesMajor(Object fromKey, boolean isInclusive, int maxValuesToFetch,
      ValuesTransformer<V> transformer);

  Collection<OIdentifiable> getValuesMinor(final Object toKey, final boolean isInclusive, final int maxValuesToFetch,
      ValuesTransformer<V> transformer);

  Collection<ODocument> getEntriesMajor(final Object fromKey, final boolean isInclusive, final int maxEntriesToFetch,
      ValuesTransformer<V> transformer);

  Collection<ODocument> getEntriesMinor(Object toKey, boolean isInclusive, int maxEntriesToFetch, ValuesTransformer<V> transformer);

  Collection<ODocument> getEntriesBetween(Object iRangeFrom, Object iRangeTo, boolean iInclusive, int maxEntriesToFetch,
      ValuesTransformer<V> transformer);

  long size(ValuesTransformer<V> transformer);

  long count(Object rangeFrom, final boolean fromInclusive, Object rangeTo, final boolean toInclusive, final int maxValuesToFetch,
      ValuesTransformer<V> transformer);

  boolean hasRangeQuerySupport();

  interface ValuesTransformer<V> {
    Collection<OIdentifiable> transformFromValue(V value);

    V transformToValue(Collection<OIdentifiable> collection);
  }
}
