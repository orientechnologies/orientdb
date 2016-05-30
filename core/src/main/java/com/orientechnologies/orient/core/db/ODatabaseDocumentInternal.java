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

package com.orientechnologies.orient.core.db;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.OCurrentStorageComponentsFactory;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OSBTreeCollectionManager;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.OMetadata;
import com.orientechnologies.orient.core.metadata.OMetadataDefault;
import com.orientechnologies.orient.core.metadata.OMetadataInternal;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializerFactory;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;

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

  int assignAndCheckCluster(ORecord record, String iClusterName);

  <RET extends ORecord> RET loadIfVersionIsNotLatest(final ORID rid, final int recordVersion, String fetchPlan, boolean ignoreCache)
      throws ORecordNotFoundException;

  void reloadUser();

  @Override
  OMetadataInternal getMetadata();
}
