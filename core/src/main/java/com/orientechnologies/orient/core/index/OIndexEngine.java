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

  void load(ORID indexRid, String indexName, OIndexDefinition indexDefinition, OStreamSerializer valueSerializer,
      boolean isAutomatic);

  boolean contains(Object key);

  boolean remove(Object key);

  ORID getIdentity();

  void clear();

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

  public Object getFirstKey();

  public Object getLastKey();

  OIndexCursor iterateEntriesBetween(Object rangeFrom, boolean fromInclusive, Object rangeTo, boolean toInclusive,
      boolean ascSortOrder, ValuesTransformer<V> transformer);

  OIndexCursor iterateEntriesMajor(Object fromKey, boolean isInclusive, boolean ascSortOrder, ValuesTransformer<V> transformer);

  OIndexCursor iterateEntriesMinor(final Object toKey, final boolean isInclusive, boolean ascSortOrder,
      ValuesTransformer<V> transformer);

  OIndexCursor cursor(ValuesTransformer<V> valuesTransformer);

  OIndexCursor descCursor(ValuesTransformer<V> valuesTransformer);

  OIndexKeyCursor keyCursor();

  long size(ValuesTransformer<V> transformer);

  boolean hasRangeQuerySupport();

  interface ValuesTransformer<V> {
    Collection<OIdentifiable> transformFromValue(V value);
  }
}
