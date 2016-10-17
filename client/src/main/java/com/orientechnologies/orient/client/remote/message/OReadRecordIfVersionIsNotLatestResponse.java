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
package com.orientechnologies.orient.client.remote.message;

import java.io.IOException;

import com.orientechnologies.orient.client.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.OStorageOperationResult;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;

public class OReadRecordIfVersionIsNotLatestResponse implements OBinaryResponse<OStorageOperationResult<ORawBuffer>> {
  @Override
  public OStorageOperationResult<ORawBuffer> read(OChannelBinaryAsynchClient network, OStorageRemoteSession session)
      throws IOException {

    if (network.readByte() == 0)
      return new OStorageOperationResult<ORawBuffer>(null);

    byte type = network.readByte();
    int recVersion = network.readVersion();
    byte[] bytes = network.readBytes();
    ORawBuffer buffer = new ORawBuffer(bytes, recVersion, type);

    final ODatabaseDocument database = ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();
    ORecord record;

    while (network.readByte() == 2) {
      record = (ORecord) OChannelBinaryProtocol.readIdentifiable(network);

      if (database != null)
        // PUT IN THE CLIENT LOCAL CACHE
        database.getLocalCache().updateRecord(record);
    }
    return new OStorageOperationResult<ORawBuffer>(buffer);
  }
}