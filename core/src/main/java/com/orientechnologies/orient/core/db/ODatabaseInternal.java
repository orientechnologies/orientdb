/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */

package com.orientechnologies.orient.core.db;

import com.orientechnologies.orient.core.enterprise.OEnterpriseEndpoint;
import com.orientechnologies.orient.core.metadata.security.OSecurityUser;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.OStorageInfo;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TimerTask;

public interface ODatabaseInternal<T> extends ODatabase<T> {

  /**
   * Returns the underlying storage implementation.
   *
   * @return The underlying storage implementation
   * @see OStorage
   */
  OStorage getStorage();

  OStorageInfo getStorageInfo();

  /** Set user for current database instance. */
  void setUser(OSecurityUser user);

  void resetInitialization();

  /**
   * Returns the database owner. Used in wrapped instances to know the up level ODatabase instance.
   *
   * @return Returns the database owner.
   */
  ODatabaseInternal<?> getDatabaseOwner();

  /** Internal. Sets the database owner. */
  ODatabaseInternal<?> setDatabaseOwner(ODatabaseInternal<?> iOwner);

  /**
   * Return the underlying database. Used in wrapper instances to know the down level ODatabase
   * instance.
   *
   * @return The underlying ODatabase implementation.
   */
  <DB extends ODatabase> DB getUnderlying();

  /** Internal method. Don't call it directly unless you're building an internal component. */
  void setInternal(ATTRIBUTES attribute, Object iValue);

  OSharedContext getSharedContext();

  /**
   * returns the cluster map for current deploy. The keys of the map are node names, the values
   * contain names of clusters (data files) available on the single node.
   *
   * @return the cluster map for current deploy
   */
  default String getLocalNodeName() {
    return "local";
  }

  /**
   * returns the data center map for current deploy. The keys are data center names, the values are
   * node names per data center
   *
   * @return data center map for current deploy
   */
  default Map<String, Set<String>> getActiveDataCenterMap() {
    Map<String, Set<String>> result = new HashMap<>();
    Set<String> val = new HashSet<>();
    val.add(getLocalNodeName());
    result.put("local", val);
    return result;
  }

  /**
   * checks the cluster map and tells whether this is a sharded database (ie. a distributed DB where
   * at least two nodes contain distinct subsets of data) or not
   *
   * @return true if the database is sharded, false otherwise
   */
  default boolean isSharded() {
    return false;
  }

  /** @return an endpoint for Enterprise features. Null in Community Edition */
  default OEnterpriseEndpoint getEnterpriseEndpoint() {
    return null;
  }

  default ODatabaseStats getStats() {
    return new ODatabaseStats();
  }

  default void resetRecordLoadStats() {}

  default void addRidbagPrefetchStats(long execTimeMs) {}

  /**
   * creates an interrupt timer task for this db instance (without scheduling it!)
   *
   * @return the timer task. Null if this operation is not supported for current db impl.
   */
  default TimerTask createInterruptTimerTask() {
    return null;
  }

  /**
   * Saves an entity in the specified cluster specifying the mode. If the entity is not dirty, then
   * the operation will be ignored. For custom entity implementations assure to set the entity as
   * dirty. If the cluster does not exist, an error will be thrown.
   *
   * @param iObject The entity to save
   * @param iClusterName Name of the cluster where to save
   * @param iForceCreate Flag that indicates that record should be created. If record with current
   *     rid already exists, exception is thrown
   */
  <RET extends T> RET save(T iObject, String iClusterName, boolean forceCreate);
}
