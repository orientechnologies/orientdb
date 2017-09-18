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

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.RecordReader;
import com.orientechnologies.orient.core.db.record.OCurrentStorageComponentsFactory;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OSBTreeCollectionManager;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.hook.ORecordHook;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.OMetadataInternal;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializerFactory;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.sql.executor.OExecutionPlan;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.sql.parser.OLocalResultSetLifecycleDecorator;
import com.orientechnologies.orient.core.storage.ORecordCallback;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.core.tx.OTransactionOptimistic;

import java.util.Map;

public interface ODatabaseDocumentInternal extends ODatabaseSession, ODatabaseInternal<ORecord> {

  /**
   * Internal. Returns the factory that defines a set of components that current database should use to be compatible to current
   * version of storage. So if you open a database create with old version of OrientDB it defines a components that should be used
   * to provide backward compatibility with that version of database.
   */
  OCurrentStorageComponentsFactory getStorageVersions();

  /**
   * Internal. Gets an instance of sb-tree collection manager for current database.
   */
  OSBTreeCollectionManager getSbTreeCollectionManager();

  /**
   * @return the factory of binary serializers.
   */
  OBinarySerializerFactory getSerializerFactory();

  /**
   * @return serializer which is used for document serialization.
   */
  ORecordSerializer getSerializer();

  void setSerializer(ORecordSerializer serializer);

  int assignAndCheckCluster(ORecord record, String iClusterName);

  <RET extends ORecord> RET loadIfVersionIsNotLatest(final ORID rid, final int recordVersion, String fetchPlan, boolean ignoreCache)
      throws ORecordNotFoundException;

  void reloadUser();

  ORecordHook.RESULT callbackHooks(final ORecordHook.TYPE type, final OIdentifiable id);

  <RET extends ORecord> RET executeReadRecord(final ORecordId rid, ORecord iRecord, final int recordVersion, final String fetchPlan,
      final boolean ignoreCache, final boolean iUpdateCache, final boolean loadTombstones,
      final OStorage.LOCKING_STRATEGY lockingStrategy, RecordReader recordReader);

  <RET extends ORecord> RET executeSaveRecord(final ORecord record, String clusterName, final int ver, final OPERATION_MODE mode,
      boolean forceCreate, final ORecordCallback<? extends Number> recordCreatedCallback,
      ORecordCallback<Integer> recordUpdatedCallback);

  void executeDeleteRecord(OIdentifiable record, final int iVersion, final boolean iRequired, final OPERATION_MODE iMode,
      boolean prohibitTombstones);

  <RET extends ORecord> RET executeSaveEmptyRecord(ORecord record, String clusterName);

  void setDefaultTransactionMode();

  @Override
  OMetadataInternal getMetadata();

  ODatabaseDocumentInternal copy();

  void recycle(ORecord record);

  void checkIfActive();

  void callOnOpenListeners();

  void callOnCloseListeners();

  void callOnDropListeners();

  <DB extends ODatabase> DB setCustom(final String name, final Object iValue);

  void setPrefetchRecords(boolean prefetchRecords);

  boolean isPrefetchRecords();

  void checkForClusterPermissions(String name);

  void rawBegin(OTransaction transaction);

  default OResultSet getActiveQuery(String id) {
    throw new UnsupportedOperationException();
  }

  boolean isUseLightweightEdges();

  OEdge newLightweightEdge(String iClassName, OVertex from, OVertex to);

  void setUseLightweightEdges(boolean b);

  /**
   * Hides records content by putting tombstone on the records position but does not delete record itself.
   * <p>
   * This method is used in case of record content itself is broken and cannot be read or deleted. So it is emergence method. This
   * method can be used only if there is no active transaction in database.
   *
   * @param rid record id.
   *
   * @return <code>true</code> if record was hidden and <code>false</code> if record does not exits in database.
   *
   * @throws java.lang.UnsupportedOperationException                              In case current version of cluster does not
   *                                                                              support given operation.
   * @throws com.orientechnologies.orient.core.exception.ORecordNotFoundException if record is already deleted/hidden.
   */

  boolean hide(ORID rid);

  ODatabaseDocumentInternal cleanOutRecord(ORID rid, int version);

  default void realClose() {
    //Only implemented by pooled instances
    throw new UnsupportedOperationException();
  }

  default void reuse() {
    //Only implemented by pooled instances
    throw new UnsupportedOperationException();
  }

  /**
   * synchronizes current database instance with the rest of the cluster (if in distributed mode).
   *
   * @return true if the database was synchronized, false otherwise
   */
  default boolean sync(boolean forceDeployment, boolean tryWithDelta) {
    return false;
  }

  /**
   * synchronizes a cluster with the rest of the network (if in distributed mode).
   *
   * @return true if the database was synchronized, false otherwise
   */
  default Map<String, Object> syncCluster(String clusterName) {
    return null;
  }

  default Map<String, Object> getHaStatus(boolean servers, boolean db, boolean latency, boolean messages) {
    return null;
  }

  default boolean removeHaServer(String serverName) {
    return false;
  }

  /**
   * sends an execution plan to a remote node for a remote query execution
   *
   * @param nodeName        the node name
   * @param executionPlan   the execution plan
   * @param inputParameters the input parameters for execution
   *
   * @return an OResultSet to fetch the results of the query execution
   */
  default OResultSet queryOnNode(String nodeName, OExecutionPlan executionPlan, Map<Object, Object> inputParameters) {
    throw new UnsupportedOperationException();
  }

  /**
   * Executed the commit on the storage hiding away storage concepts from the transaction
   *
   * @param transaction
   */
  void internalCommit(OTransactionOptimistic transaction);
}
