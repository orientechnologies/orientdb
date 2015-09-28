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

import java.util.Collection;
import java.util.Set;

import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.common.util.OApi;
import com.orientechnologies.orient.core.dictionary.ODictionary;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.type.ODocumentWrapper;

/**
 * Manager of indexes.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public interface OIndexManager {

  /**
   * Load index manager data from database.
   * 
   * IMPORTANT! Only for internal usage.
   * 
   * @return this
   */
  public OIndexManager load();

  /**
   * Creates a document where index manager configuration is saved and creates a "dictionary" index.
   * 
   * IMPORTANT! Only for internal usage.
   */
  public void create();

  /**
   * Drops all indexes and creates them from scratch.
   */
  public void recreateIndexes();

  /**
   * Returns all indexes registered in database.
   * 
   * @return list of registered indexes.
   */
  public Collection<? extends OIndex<?>> getIndexes();

  /**
   * Index by specified name.
   * 
   * @param iName
   *          name of index
   * @return index if one registered in database or null otherwise.
   */
  public OIndex<?> getIndex(final String iName);

  /**
   * Checks if index with specified name exists in database.
   * 
   * @param iName
   *          name of index.
   * @return true if index with specified name exists, false otherwise.
   */
  public boolean existsIndex(final String iName);


  /**
   * Creates a new index with default algorithm.
   * 
   * @param iName
   *          - name of index
   * @param iType
   *          - index type. Specified by plugged index factories.
   * @param indexDefinition
   *          metadata that describes index structure
   * @param clusterIdsToIndex
   *          ids of clusters that index should track for changes.
   * @param progressListener
   *          listener to track task progress.
   * @param metadata
   *          document with additional properties that can be used by index engine.
   * @return a newly created index instance
   */
  public OIndex<?> createIndex(final String iName, final String iType, OIndexDefinition indexDefinition,
      final int[] clusterIdsToIndex, final OProgressListener progressListener, ODocument metadata);

  /**
   * Creates a new index.
   * 
   * May require quite a long time if big amount of data should be indexed.
   * 
   * @param iName
   *          name of index
   * @param iType
   *          index type. Specified by plugged index factories.
   * @param indexDefinition
   *          metadata that describes index structure
   * @param clusterIdsToIndex
   *          ids of clusters that index should track for changes.
   * @param progressListener
   *          listener to track task progress.
   * @param metadata
   *          document with additional properties that can be used by index engine.
   * @param algorithm
   *          tip to an index factory what algorithm to use
   * @return a newly created index instance
   */
  public OIndex<?> createIndex(final String iName, final String iType, OIndexDefinition indexDefinition,
      final int[] clusterIdsToIndex, final OProgressListener progressListener, ODocument metadata, String algorithm);

  /**
   * Drop index with specified name. Do nothing if such index does not exists.
   * 
   * @param iIndexName
   *          the name of index to drop
   * @return this
   */
  @OApi(maturity = OApi.MATURITY.STABLE)
  public OIndexManager dropIndex(final String iIndexName);

  /**
   * IMPORTANT! Only for internal usage.
   * 
   * @return name of default cluster.
   */
  public String getDefaultClusterName();

  /**
   * Sets the new default cluster.
   * 
   * IMPORTANT! Only for internal usage.
   * 
   * @param defaultClusterName
   *          name of new default cluster
   */
  public void setDefaultClusterName(String defaultClusterName);

  /**
   * Return a dictionary index. Could be helpful to store different kinds of configurations.
   * 
   * @return a dictionary
   */
  public ODictionary<ORecord> getDictionary();

  /**
   * Flushes all indexes that is registered in this manager. There might be some changes stored in memory, this method ensures that
   * all this changed are stored to the disk.
   */
  public void flush();

  /**
   * Returns a record where configurations are saved.
   * 
   * IMPORTANT! Only for internal usage.
   * 
   * @return a document that used to store index configurations.
   */
  public ODocument getConfiguration();

  /**
   * Returns list of indexes that contain passed in fields names as their first keys. Order of fields does not matter.
   * <p/>
   * All indexes sorted by their count of parameters in ascending order. If there are indexes for the given set of fields in super
   * class they will be taken into account.
   * 
   * @param className
   *          name of class which is indexed.
   * @param fields
   *          Field names.
   * @return list of indexes that contain passed in fields names as their first keys.
   */
  public Set<OIndex<?>> getClassInvolvedIndexes(String className, Collection<String> fields);

  /**
   * Returns list of indexes that contain passed in fields names as their first keys. Order of fields does not matter.
   * <p/>
   * All indexes sorted by their count of parameters in ascending order. If there are indexes for the given set of fields in super
   * class they will be taken into account.
   * 
   * @param className
   *          name of class which is indexed.
   * @param fields
   *          Field names.
   * @return list of indexes that contain passed in fields names as their first keys.
   */
  public Set<OIndex<?>> getClassInvolvedIndexes(String className, String... fields);

  /**
   * Indicates whether given fields are contained as first key fields in class indexes. Order of fields does not matter. If there
   * are indexes for the given set of fields in super class they will be taken into account.
   * 
   * @param className
   *          name of class which contain {@code fields}.
   * @param fields
   *          Field names.
   * @return <code>true</code> if given fields are contained as first key fields in class indexes.
   */
  public boolean areIndexed(String className, Collection<String> fields);

  /**
   * @param className
   *          name of class which contain {@code fields}.
   * @param fields
   *          Field names.
   * @return <code>true</code> if given fields are contained as first key fields in class indexes.
   * @see #areIndexed(String, java.util.Collection)
   */
  public boolean areIndexed(String className, String... fields);

  /**
   * Gets indexes for a specified class (excluding indexes for sub-classes).
   * 
   * @param className
   *          name of class which is indexed.
   * @return a set of indexes related to specified class
   */
  public Set<OIndex<?>> getClassIndexes(String className);

  /**
   * Gets indexes for a specified class (excluding indexes for sub-classes).
   * 
   * @param className
   *          name of class which is indexed.
   * @param indexes
   *          Collection of indexes where to add all the indexes
   */
  void getClassIndexes(String className, Collection<OIndex<?>> indexes);

  /**
   * Searches for index for a specified class with specified name.
   * 
   * @param className
   *          name of class which is indexed.
   * @param indexName
   *          name of index.
   * @return an index instance or null if such does not exist.
   */
  public OIndex<?> getClassIndex(String className, String indexName);

  /**
   * Blocks current thread till indexes will be restored.
   */
  public void waitTillIndexRestore();

  /**
   * Checks if indexes should be automatically recreated.
   * 
   * IMPORTANT! Only for internal usage.
   * 
   * @return true if crash is happened and database configured to automatically recreate indexes after crash.
   */
  public boolean autoRecreateIndexesAfterCrash();

  /**
   * Adds a cluster to tracked cluster list of specified index.
   * 
   * IMPORTANT! Only for internal usage.
   * 
   * @param clusterName
   *          cluster to add.
   * @param indexName
   *          name of index.
   */
  public void addClusterToIndex(String clusterName, String indexName);

  /**
   * Removes a cluster from tracked cluster list of specified index.
   * 
   * IMPORTANT! Only for internal usage.
   * 
   * @param clusterName
   *          cluster to remove.
   * @param indexName
   *          name of index.
   */
  public void removeClusterFromIndex(String clusterName, String indexName);

  /**
   * Saves index manager data.
   * 
   * IMPORTANT! Only for internal usage.
   */
  public <RET extends ODocumentWrapper> RET save();

  /**
   * Removes index from class-property map.
   * 
   * IMPORTANT! Only for internal usage.
   * 
   * @param idx
   *          index to remove.
   */
  void removeClassPropertyIndex(OIndex<?> idx);

  /**
   * IMPORTANT! Only for internal usage.
   */
  boolean isFullCheckpointOnChange();

	/**
	 * IMPORTANT! Only for internal usage.
	 */
	void setFullCheckpointOnChange(boolean fullCheckpointOnChange);
}
