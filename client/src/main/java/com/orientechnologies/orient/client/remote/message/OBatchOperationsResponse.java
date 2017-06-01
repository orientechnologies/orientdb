/*
 *
 *  *  Copyright 2010-2017 OrientDB LTD (http://orientdb.com)
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

import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Enrico Risa on 15/05/2017.
 */
public class OBatchOperationsResponse implements OBinaryResponse {

  private int txId;

  private List<OCommitResponse.OCreatedRecordResponse> created;
  private List<OCommitResponse.OUpdatedRecordResponse> updated;

  public OBatchOperationsResponse(int txId, List<OCommitResponse.OCreatedRecordResponse> created,
      List<OCommitResponse.OUpdatedRecordResponse> updated) {
    this.txId = txId;
    this.created = created;
    this.updated = updated;
  }

  public OBatchOperationsResponse() {
  }

  @Override
  public void write(OChannelDataOutput channel, int protocolVersion, ORecordSerializer serializer) throws IOException {
    channel.writeInt(txId);

    channel.writeInt(created.size());
    for (OCommitResponse.OCreatedRecordResponse createdRecord : created) {
      channel.writeRID(createdRecord.getCurrentRid());
      channel.writeRID(createdRecord.getCreatedRid());
    }

    channel.writeInt(updated.size());
    for (OCommitResponse.OUpdatedRecordResponse updatedRecord : updated) {
      channel.writeRID(updatedRecord.getRid());
      channel.writeVersion(updatedRecord.getVersion());
    }
  }

  @Override
  public void read(OChannelDataInput network, OStorageRemoteSession session) throws IOException {
    txId = network.readInt();

    final int createdRecords = network.readInt();
    created = new ArrayList<>(createdRecords);
    ORecordId currentRid;
    ORecordId createdRid;
    for (int i = 0; i < createdRecords; i++) {
      currentRid = network.readRID();
      createdRid = network.readRID();

      created.add(new OCommitResponse.OCreatedRecordResponse(currentRid, createdRid));
    }
    final int updatedRecords = network.readInt();
    updated = new ArrayList<>(updatedRecords);
    ORecordId rid;
    for (int i = 0; i < updatedRecords; ++i) {
      rid = network.readRID();
      int version = network.readVersion();
      updated.add(new OCommitResponse.OUpdatedRecordResponse(rid, version));
    }
  }

  public int getTxId() {
    return txId;
  }

  public List<OCommitResponse.OCreatedRecordResponse> getCreated() {
    return created;
  }

  public List<OCommitResponse.OUpdatedRecordResponse> getUpdated() {
    return updated;
  }
}
