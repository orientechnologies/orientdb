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

import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.Collection;

/**
 * Interface to handle index.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public interface OIndexInternal<T> extends OIndex<T> {

  String CONFIG_KEYTYPE            = "keyType";
  String CONFIG_AUTOMATIC          = "automatic";
  String CONFIG_TYPE               = "type";
  String ALGORITHM                 = "algorithm";
  String VALUE_CONTAINER_ALGORITHM = "valueContainerAlgorithm";
  String CONFIG_NAME               = "name";
  String INDEX_DEFINITION          = "indexDefinition";
  String INDEX_DEFINITION_CLASS    = "indexDefinitionClass";
  String INDEX_VERSION             = "indexVersion";
  String METADATA                  = "metadata";

  Object getCollatingValue(final Object key);

  /**
   * Loads the index giving the configuration.
   * 
   * @param iConfig
   *          ODocument instance containing the configuration
   * 
   */
  boolean loadFromConfiguration(ODocument iConfig);

  /**
   * Saves the index configuration to disk.
   * 
   * @return The configuration as ODocument instance
   * @see #getConfiguration()
   */
  ODocument updateConfiguration();

  /**
   * Add given cluster to the list of clusters that should be automatically indexed.
   * 
   * @param iClusterName
   *          Cluster to add.
   * @return Current index instance.
   */
  OIndex<T> addCluster(final String iClusterName);

  /**
   * Remove given cluster from the list of clusters that should be automatically indexed.
   * 
   * @param iClusterName
   *          Cluster to remove.
   * @return Current index instance.
   */
  OIndex<T> removeCluster(final String iClusterName);

  /**
   * Indicates whether given index can be used to calculate result of
   * {@link com.orientechnologies.orient.core.sql.operator.OQueryOperatorEquality} operators.
   * 
   * @return {@code true} if given index can be used to calculate result of
   *         {@link com.orientechnologies.orient.core.sql.operator.OQueryOperatorEquality} operators.
   * 
   */
  boolean canBeUsedInEqualityOperators();

  boolean hasRangeQuerySupport();

  /**
   * Applies exclusive lock on keys which prevents read/modification of this keys in following methods:
   *
   * <ol>
   * <li>{@link #put(Object, com.orientechnologies.orient.core.db.record.OIdentifiable)}</li>
   * <li>{@link #checkEntry(com.orientechnologies.orient.core.db.record.OIdentifiable, Object)}</li>
   * <li>{@link #remove(Object, com.orientechnologies.orient.core.db.record.OIdentifiable)}</li>
   * <li>{@link #remove(Object)}</li>
   * </ol>
   *
   * <p>
   * If you want to lock several keys in single thread, you should pass all those keys in single method call. Several calls of this
   * method in single thread are not allowed because it may lead to deadlocks. Lock is applied only in case if there are no
   * transactions.
   * </p>
   *
   * This is internal method and cannot be used by end users.
   *
   * @param key
   *          Keys to lock.
   */
  void lockKeysForUpdateNoTx(Object... key);

  /**
   * Applies exclusive lock on keys which prevents read/modification of this keys in following methods:
   *
   * <ol>
   * <li>{@link #put(Object, com.orientechnologies.orient.core.db.record.OIdentifiable)}</li>
   * <li>{@link #checkEntry(com.orientechnologies.orient.core.db.record.OIdentifiable, Object)}</li>
   * <li>{@link #remove(Object, com.orientechnologies.orient.core.db.record.OIdentifiable)}</li>
   * <li>{@link #remove(Object)}</li>
   * </ol>
   *
   * <p>
   * If you want to lock several keys in single thread, you should pass all those keys in single method call. Several calls of this
   * method in single thread are not allowed because it may lead to deadlocks. Lock is applied only in case if there are no
   * transactions.
   * </p>
   *
   * This is internal method and cannot be used by end users.
   *
   * @param keys
   *          Keys to lock.
   */
  void lockKeysForUpdateNoTx(Collection<Object> keys);

  /**
   * Release exclusive lock on keys which prevents read/modification of this keys in following methods:
   *
   * <ol>
   * <li>{@link #put(Object, com.orientechnologies.orient.core.db.record.OIdentifiable)}</li>
   * <li>{@link #checkEntry(com.orientechnologies.orient.core.db.record.OIdentifiable, Object)}</li>
   * <li>{@link #remove(Object, com.orientechnologies.orient.core.db.record.OIdentifiable)}</li>
   * <li>{@link #remove(Object)}</li>
   * </ol>
   *
   * This is internal method and cannot be used by end users.
   *
   * @param key
   *          Keys to unlock.
   */
  void releaseKeysForUpdateNoTx(Object... key);

  /**
   * Release exclusive lock on keys which prevents read/modification of this keys in following methods:
   *
   * <ol>
   * <li>{@link #put(Object, com.orientechnologies.orient.core.db.record.OIdentifiable)}</li>
   * <li>{@link #checkEntry(com.orientechnologies.orient.core.db.record.OIdentifiable, Object)}</li>
   * <li>{@link #remove(Object, com.orientechnologies.orient.core.db.record.OIdentifiable)}</li>
   * <li>{@link #remove(Object)}</li>
   * </ol>
   *
   * This is internal method and cannot be used by end users.
   *
   * @param keys
   *          Keys to unlock.
   */
  void releaseKeysForUpdateNoTx(Collection<Object> keys);

  OIndexMetadata loadMetadata(ODocument iConfig);

  void setRebuildingFlag();

  void close();

  void preCommit();

  void addTxOperation(ODocument operationDocument);

  void commit();

  void postCommit();

  void setType(OType type);
}
