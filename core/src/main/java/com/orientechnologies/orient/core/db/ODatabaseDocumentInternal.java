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
import com.orientechnologies.orient.core.sql.parser.OLocalResultSetLifecycleDecorator;
import com.orientechnologies.orient.core.storage.ORecordCallback;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.tx.OTransaction;

import java.util.Set;

public interface ODatabaseDocumentInternal extends ODatabaseDocument, ODatabaseInternal<ORecord> {

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

  Set<ORecord> executeReadRecords(final Set<ORecordId> iRids, final boolean ignoreCache);

  void checkIfActive();

  void callOnOpenListeners();

  void callOnCloseListeners();

  void callOnDropListeners();

  public <DB extends ODatabase> DB setCustom(final String name, final Object iValue);

  void setPrefetchRecords(boolean prefetchRecords);

  boolean isPrefetchRecords();

  void checkForClusterPermissions(String name);

  void rawBegin(OTransaction transaction);

  default OLocalResultSetLifecycleDecorator getActiveQuery(String id) {
    throw new UnsupportedOperationException();
  }

  boolean isUseLightweightEdges();

  OEdge newLightweightEdge(String iClassName, OVertex from, OVertex to);

  void setUseLightweightEdges(boolean b);
}
