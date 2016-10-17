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
import com.orientechnologies.orient.client.remote.OStorageRemote;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.tx.OTransaction;

public final class OCommitResponse implements OBinaryResponse<Void> {
  private final OTransaction iTx;

  public OCommitResponse(OTransaction iTx) {
    this.iTx = iTx;
  }

  @Override
  public Void read(OChannelBinaryAsynchClient network, OStorageRemoteSession session) throws IOException {
    final int createdRecords = network.readInt();
    ORecordId currentRid;
    ORecordId createdRid;
    for (int i = 0; i < createdRecords; i++) {
      currentRid = network.readRID();
      createdRid = network.readRID();

      iTx.updateIdentityAfterCommit(currentRid, createdRid);
    }

    final int updatedRecords = network.readInt();
    ORecordId rid;
    for (int i = 0; i < updatedRecords; ++i) {
      rid = network.readRID();
      int version = network.readVersion();
      ORecordOperation rop = iTx.getRecordEntry(rid);
      if (rop != null) {
        if (version > rop.getRecord().getVersion() + 1)
          // IN CASE OF REMOTE CONFLICT STRATEGY FORCE UNLOAD DUE TO INVALID CONTENT
          rop.getRecord().unload();
        ORecordInternal.setVersion(rop.getRecord(), version);
      }
    }

    OStorageRemote.readCollectionChanges(network, ODatabaseRecordThreadLocal.INSTANCE.get().getSbTreeCollectionManager());
    return null;
  }
}