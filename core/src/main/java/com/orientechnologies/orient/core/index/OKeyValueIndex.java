package com.orientechnologies.orient.core.index;

import java.util.Collection;
import java.util.Set;

import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 * @since 2/18/13
 */
public interface OKeyValueIndex<T> {

  /**
   * Creates the index.
   * 
   * 
   * @param iName
   * 
   * @param iDatabase
   *          Current Database instance
   * @param iClusterIndexName
   *          Cluster name where to place the TreeMap
   * @param iClusterIdsToIndex
   * @param rebuild
   * @param iProgressListener
   */
  OIndex<T> create(String iName, OIndexDefinition iIndexDefinition, ODatabaseRecord iDatabase, String iClusterIndexName,
      int[] iClusterIdsToIndex, boolean rebuild, OProgressListener iProgressListener);

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
   * Counts the elements associated with the passed key, if any.
   * 
   * @param iKey
   *          The key to count
   * @return The size of found records, otherwise 0 if the key is not found
   */
  long count(Object iKey);

  public long count(Object iRangeFrom, boolean iFromInclusive, Object iRangeTo, boolean iToInclusive, int maxValuesToFetch);

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
   * @param iKey
   *          The entry's key to remove
   * @return True if the entry has been found and removed, otherwise false
   */
  boolean remove(Object iKey);

  /**
   * Removes an entry by its key and value.
   * 
   * @param iKey
   *          The entry's key to remove
   * @return True if the entry has been found and removed, otherwise false
   */
  boolean remove(Object iKey, OIdentifiable iRID);

  /**
   * Removes a value in all the index entries.
   * 
   * @param iRID
   *          Record id to search
   * @return Times the record was found, 0 if not found at all
   */
  int remove(OIdentifiable iRID);

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
   * Stores all the in-memory changes to disk.
   * 
   * @return The index instance itself to allow in chain calls
   */
  OIndex<T> lazySave();

  /**
   * Delete the index.
   * 
   * @return The index instance itself to allow in chain calls
   */
  OIndex<T> delete();

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

  Collection<OIdentifiable> getValues(Collection<?> iKeys, int maxValuesToFetch);

  /**
   * Returns a set of documents with keys in specific set
   * 
   * @param iKeys
   *          Set of keys
   * @return
   */
  Collection<ODocument> getEntries(Collection<?> iKeys);

  Collection<ODocument> getEntries(Collection<?> iKeys, int maxEntriesToFetch);

  OIndexDefinition getDefinition();

  /**
   * Returns Names of clusters that will be indexed.
   * 
   * @return Names of clusters that will be indexed.
   */
  Set<String> getClusters();

  /**
   * Commits changes as atomic. It's called during the transaction's commit.
   * 
   * @param iDocument
   *          Collection of entries to commit
   */
  void commit(ODocument iDocument);
}
