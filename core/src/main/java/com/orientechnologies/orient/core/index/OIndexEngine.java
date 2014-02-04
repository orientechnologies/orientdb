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

  void deleteWithoutLoad(String indexName);

  void load(ORID indexRid, String indexName, OIndexDefinition indexDefinition, boolean isAutomatic);

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

  void getValuesBetween(Object rangeFrom, boolean fromInclusive, Object rangeTo, boolean toInclusive,
      ValuesTransformer<V> transformer, ValuesResultListener valuesResultListener);

  void getValuesMajor(Object fromKey, boolean isInclusive, ValuesTransformer<V> transformer,
      ValuesResultListener valuesResultListener);

  void getValuesMinor(final Object toKey, final boolean isInclusive, ValuesTransformer<V> transformer,
      ValuesResultListener valuesResultListener);

  void getEntriesMajor(final Object fromKey, final boolean isInclusive, ValuesTransformer<V> transformer,
      EntriesResultListener entriesResultListener);

  void getEntriesMinor(Object toKey, boolean isInclusive, ValuesTransformer<V> transformer,
      EntriesResultListener entriesResultListener);

  void getEntriesBetween(Object iRangeFrom, Object iRangeTo, boolean iInclusive, ValuesTransformer<V> transformer,
      EntriesResultListener entriesResultListener);

  long size(ValuesTransformer<V> transformer);

  boolean hasRangeQuerySupport();

  interface ValuesTransformer<V> {
    Collection<OIdentifiable> transformFromValue(V value);

    V transformToValue(Collection<OIdentifiable> collection);
  }

  interface ValuesResultListener {
    boolean addResult(OIdentifiable identifiable);
  }

  interface EntriesResultListener {
    boolean addResult(ODocument entry);
  }
}
