/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.orient.core.index;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Basic interface to handle index.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public interface OIndex<T> {
  /**
   * Creates the index.
   * 
   * 
   * @param name
   * 
   * @param clusterIndexName
   *          Cluster name where to place the TreeMap
   * @param clustersToIndex
   * @param rebuild
   * @param progressListener
   */
  OIndex<T> create(String name, OIndexDefinition indexDefinition, String clusterIndexName, Set<String> clustersToIndex,
      boolean rebuild, OProgressListener progressListener);

  /**
   * Unloads the index freeing the resource in memory.
   */
  void unload();

  String getDatabaseName();

  /**
   * Types of the keys that index can accept, if index contains composite key, list of types of elements from which this index
   * consist will be returned, otherwise single element (key type obviously) will be returned.
   */
  OType[] getKeyTypes();

  /**
   * Gets the set of records associated with the passed key.
   * 
   * @param iKey
   *          The key to search
   * @return The Record set if found, otherwise an empty Set
   */
  T get(Object iKey);

  /**
   * Tells if a key is contained in the index.
   * 
   * @param iKey
   *          The key to search
   * @return True if the key is contained, otherwise false
   */
  boolean contains(Object iKey);

  /**
   * Inserts a new entry in the index. The behaviour depends by the index implementation.
   * 
   * @param iKey
   *          Entry's key
   * @param iValue
   *          Entry's value as OIdentifiable instance
   * @return The index instance itself to allow in chain calls
   */
  OIndex<T> put(Object iKey, OIdentifiable iValue);

  /**
   * Removes an entry by its key.
   * 
   * @param key
   *          The entry's key to remove
   * @return True if the entry has been found and removed, otherwise false
   */
  boolean remove(Object key);

  /**
   * Removes an entry by its key and value.
   * 
   * @param iKey
   *          The entry's key to remove
   * @return True if the entry has been found and removed, otherwise false
   */
  boolean remove(Object iKey, OIdentifiable iRID);

  /**
   * Clears the index removing all the entries in one shot.
   * 
   * @return The index instance itself to allow in chain calls
   */
  OIndex<T> clear();

  /**
   * @return number of entries in the index.
   */
  long getSize();

  /**
   * @return Number of keys in index
   */
  long getKeySize();

  /**
   * For unique indexes it will throw exception if passed in key is contained in index.
   * 
   * @param iRecord
   * @param iKey
   */
  void checkEntry(OIdentifiable iRecord, Object iKey);

  /**
   * Flushes in-memory changes to disk.
   */
  public void flush();

  /**
   * Delete the index.
   * 
   * @return The index instance itself to allow in chain calls
   */
  OIndex<T> delete();

  void deleteWithoutIndexLoad(String indexName);

  /**
   * Returns the index name.
   * 
   * @return The name of the index
   */
  String getName();

  /**
   * Returns the type of the index as string.
   */
  String getType();

  /**
   * Tells if the index is automatic. Automatic means it's maintained automatically by OrientDB. This is the case of indexes created
   * against schema properties. Automatic indexes can always been rebuilt.
   * 
   * @return True if the index is automatic, otherwise false
   */
  boolean isAutomatic();

  /**
   * Rebuilds an automatic index.
   * 
   * @return The number of entries rebuilt
   */
  long rebuild();

  /**
   * Populate the index with all the existent records.
   */
  long rebuild(OProgressListener iProgressListener);

  /**
   * Returns the index configuration.
   * 
   * @return An ODocument object containing all the index properties
   */
  ODocument getConfiguration();

  /**
   * Returns the internal index used.
   * 
   */
  OIndexInternal<T> getInternal();

  /**
   * Returns set of records with keys in specific set
   * 
   * @param iKeys
   *          Set of keys
   * @return
   */
  Collection<OIdentifiable> getValues(Collection<?> iKeys);

  void getValues(Collection<?> iKeys, IndexValuesResultListener resultListener);

  /**
   * Returns a set of documents with keys in specific set
   * 
   * @param iKeys
   *          Set of keys
   * @return
   */
  Collection<ODocument> getEntries(Collection<?> iKeys);

  void getEntries(Collection<?> iKeys, IndexEntriesResultListener resultListener);

  OIndexDefinition getDefinition();

  /**
   * Returns Names of clusters that will be indexed.
   * 
   * @return Names of clusters that will be indexed.
   */
  Set<String> getClusters();

  /**
   * Returns an iterator to walk across all the index items from the first to the latest one.
   * 
   * @return
   */
  public Iterator<Entry<Object, T>> iterator();

  /**
   * Returns an iterator to walk across all the index items from the last to the first one.
   * 
   * @return
   */
  public Iterator<Entry<Object, T>> inverseIterator();

  /**
   * Returns an iterator to walk across all the index values from the first to the latest one.
   * 
   * @return
   */
  public Iterator<OIdentifiable> valuesIterator();

  /**
   * Returns an iterator to walk across all the index values from the last to the first one.
   * 
   * @return
   */
  public Iterator<OIdentifiable> valuesInverseIterator();

  /**
   * Returns an Iterable instance of all the keys contained in the index.
   * 
   * @return A Iterable<Object> that lazy load the entries once fetched
   */
  public Iterable<Object> keys();

  /**
   * Returns a set of records with key between the range passed as parameter. Range bounds are included.
   * 
   * In case of {@link com.orientechnologies.common.collection.OCompositeKey}s partial keys can be used as values boundaries.
   * 
   * @param iRangeFrom
   *          Starting range
   * @param iRangeTo
   *          Ending range
   * 
   * @return a set of records with key between the range passed as parameter. Range bounds are included.
   * @see com.orientechnologies.common.collection.OCompositeKey#compareTo(com.orientechnologies.common.collection.OCompositeKey)
   * @see #getValuesBetween(Object, boolean, Object, boolean)
   */
  public Collection<OIdentifiable> getValuesBetween(Object iRangeFrom, Object iRangeTo);

  /**
   * Returns a set of records with key between the range passed as parameter.
   * 
   * In case of {@link com.orientechnologies.common.collection.OCompositeKey}s partial keys can be used as values boundaries.
   * 
   * @param iRangeFrom
   *          Starting range
   * @param iFromInclusive
   *          Indicates whether start range boundary is included in result.
   * @param iRangeTo
   *          Ending range
   * @param iToInclusive
   *          Indicates whether end range boundary is included in result.
   * 
   * @return Returns a set of records with key between the range passed as parameter.
   * 
   * @see com.orientechnologies.common.collection.OCompositeKey#compareTo(com.orientechnologies.common.collection.OCompositeKey)
   * 
   */
  public Collection<OIdentifiable> getValuesBetween(Object iRangeFrom, boolean iFromInclusive, Object iRangeTo, boolean iToInclusive);

  public void getValuesBetween(Object iRangeFrom, boolean iFromInclusive, Object iRangeTo, boolean iToInclusive,
      IndexValuesResultListener resultListener);

  /**
   * Returns a set of records with keys greater than passed parameter.
   * 
   * @param fromKey
   *          Starting key.
   * @param isInclusive
   *          Indicates whether record with passed key will be included.
   * 
   * @return set of records with keys greater than passed parameter.
   */
  public abstract Collection<OIdentifiable> getValuesMajor(Object fromKey, boolean isInclusive);

  public abstract void getValuesMajor(Object fromKey, boolean isInclusive, IndexValuesResultListener valuesResultListener);

  /**
   * Returns a set of records with keys less than passed parameter.
   * 
   * @param toKey
   *          Ending key.
   * @param isInclusive
   *          Indicates whether record with passed key will be included.
   * 
   * @return set of records with keys less than passed parameter.
   */
  public abstract Collection<OIdentifiable> getValuesMinor(Object toKey, boolean isInclusive);

  public abstract void getValuesMinor(Object toKey, boolean isInclusive, IndexValuesResultListener valuesResultListener);

  /**
   * Returns a set of documents that contains fields ("key", "rid") where "key" - index key, "rid" - record id of records with keys
   * greater than passed parameter.
   * 
   * @param fromKey
   *          Starting key.
   * @param isInclusive
   *          Indicates whether record with passed key will be included.
   * 
   * @return set of records with key greater than passed parameter.
   */
  public abstract Collection<ODocument> getEntriesMajor(Object fromKey, boolean isInclusive);

  public abstract void getEntriesMajor(Object fromKey, boolean isInclusive, IndexEntriesResultListener entriesResultListener);

  /**
   * Returns a set of documents that contains fields ("key", "rid") where "key" - index key, "rid" - record id of records with keys
   * less than passed parameter.
   * 
   * @param toKey
   *          Ending key.
   * @param isInclusive
   *          Indicates whether record with passed key will be included.
   * 
   * @return set of records with key greater than passed parameter.
   */
  public abstract Collection<ODocument> getEntriesMinor(Object toKey, boolean isInclusive);

  public abstract void getEntriesMinor(Object toKey, boolean isInclusive, IndexEntriesResultListener entriesResultListener);

  /**
   * Returns a set of documents with key between the range passed as parameter.
   * 
   * @param iRangeFrom
   *          Starting range
   * @param iRangeTo
   *          Ending range
   * @param iInclusive
   *          Include from/to bounds
   * @see #getEntriesBetween(Object, Object)
   * @return
   */
  public abstract Collection<ODocument> getEntriesBetween(final Object iRangeFrom, final Object iRangeTo, final boolean iInclusive);

  public abstract void getEntriesBetween(final Object iRangeFrom, final Object iRangeTo, final boolean iInclusive,
      IndexEntriesResultListener entriesResultListener);

  public Collection<ODocument> getEntriesBetween(Object iRangeFrom, Object iRangeTo);

  /**
   * Returns the Record Identity of the index if persistent.
   * 
   * @return Valid ORID if it's persistent, otherwise ORID(-1:-1)
   */
  public ORID getIdentity();

  ODocument getMetadata();

  public boolean supportsOrderedIterations();

  public boolean isRebuiding();

  public interface IndexValuesResultListener {
    boolean addResult(OIdentifiable value);
  }

  public interface IndexEntriesResultListener {
    boolean addResult(ODocument entry);
  }
}
