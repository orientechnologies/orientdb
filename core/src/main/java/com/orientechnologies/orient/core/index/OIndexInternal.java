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

import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.Collection;
import java.util.Set;

/**
 * Interface to handle index.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public interface OIndexInternal<T> extends OIndex<T> {

  public static final String CONFIG_KEYTYPE            = "keyType";
  public static final String CONFIG_AUTOMATIC          = "automatic";

  public static final String CONFIG_TYPE               = "type";
  public static final String ALGORITHM                 = "algorithm";
  public static final String VALUE_CONTAINER_ALGORITHM = "valueContainerAlgorithm";
  public static final String CONFIG_NAME               = "name";
  public static final String INDEX_DEFINITION          = "indexDefinition";
  public static final String INDEX_DEFINITION_CLASS    = "indexDefinitionClass";
  public static final String INDEX_VERSION             = "indexVersion";
  public static final String METADATA                  = "metadata";

  public Object getCollatingValue(final Object key);

  /**
   * Loads the index giving the configuration.
   * 
   * @param iConfig
   *          ODocument instance containing the configuration
   * 
   */
  public boolean loadFromConfiguration(ODocument iConfig);

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
   */
  public boolean canBeUsedInEqualityOperators();

  public boolean hasRangeQuerySupport();

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

  public IndexMetadata loadMetadata(ODocument iConfig);

  public void setRebuildingFlag();

  public void close();

  public void preCommit();

  void addTxOperation(ODocument operationDocument);

  public void commit();

  public void postCommit();

  public final class IndexMetadata {
    private final String           name;
    private final OIndexDefinition indexDefinition;
    private final Set<String>      clustersToIndex;
    private final String           type;
    private final String           algorithm;
    private final String           valueContainerAlgorithm;

    public IndexMetadata(String name, OIndexDefinition indexDefinition, Set<String> clustersToIndex, String type, String algorithm,
        String valueContainerAlgorithm) {
      this.name = name;
      this.indexDefinition = indexDefinition;
      this.clustersToIndex = clustersToIndex;
      this.type = type;
      this.algorithm = algorithm;
      this.valueContainerAlgorithm = valueContainerAlgorithm;
    }

    public String getName() {
      return name;
    }

    public OIndexDefinition getIndexDefinition() {
      return indexDefinition;
    }

    public Set<String> getClustersToIndex() {
      return clustersToIndex;
    }

    public String getType() {
      return type;
    }

    public String getAlgorithm() {
      return algorithm;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;

      IndexMetadata that = (IndexMetadata) o;

      if (algorithm != null ? !algorithm.equals(that.algorithm) : that.algorithm != null)
        return false;
      if (!clustersToIndex.equals(that.clustersToIndex))
        return false;
      if (indexDefinition != null ? !indexDefinition.equals(that.indexDefinition) : that.indexDefinition != null)
        return false;
      if (!name.equals(that.name))
        return false;
      if (!type.equals(that.type))
        return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = name.hashCode();
      result = 31 * result + (indexDefinition != null ? indexDefinition.hashCode() : 0);
      result = 31 * result + clustersToIndex.hashCode();
      result = 31 * result + type.hashCode();
      result = 31 * result + (algorithm != null ? algorithm.hashCode() : 0);
      return result;
    }

    public String getValueContainerAlgorithm() {
      return valueContainerAlgorithm;
    }
  }
}
