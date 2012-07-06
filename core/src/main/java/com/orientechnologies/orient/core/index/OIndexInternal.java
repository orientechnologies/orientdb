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

import java.util.Map.Entry;

import com.orientechnologies.orient.core.db.ODatabaseListener;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Interface to handle index.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public interface OIndexInternal<T> extends OIndex<T>, Iterable<Entry<Object, T>>, ODatabaseListener {

  public static final String CONFIG_KEYTYPE         = "keyType";
  public static final String CONFIG_AUTOMATIC       = "automatic";

  public static final String CONFIG_TYPE            = "type";
  public static final String CONFIG_NAME            = "name";
  public static final String INDEX_DEFINITION       = "indexDefinition";
  public static final String INDEX_DEFINITION_CLASS = "indexDefinitionClass";

  /**
   * Flushes in-memory changes to disk.
   */
  public void flush();

  /**
   * Counts the times a value is indexed in all the keys
   * 
   * @param iRecord
   *          Record to search
   * @return Times the record is found, 0 if not found at all
   */
  public int count(final OIdentifiable iRecord);

  /**
   * Loads the index giving the configuration.
   * 
   * @param iConfig
   *          ODocument instance containing the configuration
   */
  public OIndexInternal<T> loadFromConfiguration(ODocument iConfig);

  /**
   * Saves the index configuration to disk.
   * 
   * @return The configuration as ODocument instance
   * @see #getConfiguration()
   */
  public ODocument updateConfiguration();

  /**
   * Add given cluster to the list of clusters that should be automatically indexed.
   * 
   * @param iClusterName
   *          Cluster to add.
   * @return Current index instance.
   */
  public OIndex<T> addCluster(final String iClusterName);

  /**
   * Remove given cluster from the list of clusters that should be automatically indexed.
   * 
   * @param iClusterName
   *          Cluster to remove.
   * @return Current index instance.
   */
  public OIndex<T> removeCluster(final String iClusterName);

  /**
   * Indicates whether given index can be used to calculate result of
   * {@link com.orientechnologies.orient.core.sql.operator.OQueryOperatorEquality} operators.
   * 
   * @return {@code true} if given index can be used to calculate result of
   *         {@link com.orientechnologies.orient.core.sql.operator.OQueryOperatorEquality} operators.
   * 
   * @see com.orientechnologies.orient.core.sql.operator.OQueryOperatorEquals#executeIndexQuery(OIndex, java.util.List, int)
   */
  public boolean canBeUsedInEqualityOperators();

  /**
   * Prohibit index modifications. Only index read commands are allowed after this call.
   * 
   * @param throwException
   *          If <code>true</code> {@link com.orientechnologies.common.concur.lock.OModificationOperationProhibitedException}
   *          exception will be thrown in case of write command will be performed.
   */
  public void freeze(boolean throwException);

  /**
   * Allow any index modifications. Is called after {@link #freeze(boolean)} command.
   */
  public void release();

  /**
   * Is used to indicate that several index changes are going to be seen as single unit from users point of view. This command is
   * used with conjunction of {@link #freeze(boolean)} command.
   */
  public void acquireModificationLock();

  /**
   * Is used to indicate that several index changes are going to be seen as single unit from users point of view were completed.
   */
  public void releaseModificationLock();
}
