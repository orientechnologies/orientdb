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

import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.common.util.OApi;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.Collection;
import java.util.Set;

/**
 * Basic interface to handle index.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public interface OIndex<T> extends Comparable<OIndex<T>> {
  public static final String MERGE_KEYS = "mergeKeys";

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
  ODocument checkEntry(OIdentifiable iRecord, Object iKey);

  /**
   * Flushes in-memory changes to disk.
   */
  public void flush();

  /**
   * Delete the index.
   * 
   * @return The index instance itself to allow in chain calls
   */
  @OApi(enduser = false)
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
   * Returns the engine of the index as string.
   */
  public String getAlgorithm();

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
   * Returns cursor which presents data associated with passed in keys.
   * 
   * @param keys
   *          Keys data of which should be returned.
   * @param ascSortOrder
   *          Flag which determines whether data iterated by cursor should be in ascending or descending order.
   * @return cursor which presents data associated with passed in keys.
   */
  OIndexCursor iterateEntries(Collection<?> keys, boolean ascSortOrder);

  OIndexDefinition getDefinition();

  /**
   * Returns Names of clusters that will be indexed.
   * 
   * @return Names of clusters that will be indexed.
   */
  Set<String> getClusters();

  /**
   * Returns cursor which presents subset of index data between passed in keys.
   * 
   * @param fromKey
   *          Lower border of index data.
   * @param fromInclusive
   *          Indicates whether lower border should be inclusive or exclusive.
   * @param toKey
   *          Upper border of index data.
   * @param toInclusive
   *          Indicates whether upper border should be inclusive or exclusive.
   * @param ascOrder
   *          Flag which determines whether data iterated by cursor should be in ascending or descending order.
   * @return Cursor which presents subset of index data between passed in keys.
   */
  public OIndexCursor iterateEntriesBetween(Object fromKey, boolean fromInclusive, Object toKey, boolean toInclusive,
      boolean ascOrder);

  /**
   * Returns cursor which presents subset of data which associated with key which is greater than passed in key.
   * 
   * @param fromKey
   *          Lower border of index data.
   * @param fromInclusive
   *          Indicates whether lower border should be inclusive or exclusive.
   * @param ascOrder
   *          Flag which determines whether data iterated by cursor should be in ascending or descending order.
   * @return cursor which presents subset of data which associated with key which is greater than passed in key.
   */
  public OIndexCursor iterateEntriesMajor(Object fromKey, boolean fromInclusive, boolean ascOrder);

  /**
   * Returns cursor which presents subset of data which associated with key which is less than passed in key.
   * 
   * @param toKey
   *          Upper border of index data.
   * @param toInclusive
   *          Indicates Indicates whether upper border should be inclusive or exclusive.
   * @param ascOrder
   *          Flag which determines whether data iterated by cursor should be in ascending or descending order.
   * @return cursor which presents subset of data which associated with key which is less than passed in key.
   */
  public OIndexCursor iterateEntriesMinor(Object toKey, boolean toInclusive, boolean ascOrder);

  /**
   * Returns the Record Identity of the index if persistent.
   * 
   * @return Valid ORID if it's persistent, otherwise ORID(-1:-1)
   */
  public ORID getIdentity();

  public OIndexCursor cursor();

  public OIndexCursor descCursor();

  public OIndexKeyCursor keyCursor();

  ODocument getMetadata();

  public boolean supportsOrderedIterations();

  public boolean isRebuiding();

  public Object getFirstKey();

  public Object getLastKey();
}
