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

/** Created by Enrico Risa on 15/05/2017. */
public class OBatchOperationsResponse implements OBinaryResponse {

  private int txId;

  private List<OCommit37Response.OCreatedRecordResponse> created;
  private List<OCommit37Response.OUpdatedRecordResponse> updated;
  private List<OCommit37Response.ODeletedRecordResponse> deleted;

  public OBatchOperationsResponse(
      int txId,
      List<OCommit37Response.OCreatedRecordResponse> created,
      List<OCommit37Response.OUpdatedRecordResponse> updated,
      List<OCommit37Response.ODeletedRecordResponse> deleted) {
    this.txId = txId;
    this.created = created;
    this.updated = updated;
    this.deleted = deleted;
  }

  public OBatchOperationsResponse() {}

  @Override
  public void write(OChannelDataOutput channel, int protocolVersion, ORecordSerializer serializer)
      throws IOException {
    channel.writeInt(txId);

    channel.writeInt(created.size());
    for (OCommit37Response.OCreatedRecordResponse createdRecord : created) {
      channel.writeRID(createdRecord.getCurrentRid());
      channel.writeRID(createdRecord.getCreatedRid());
      channel.writeVersion(createdRecord.getVersion());
    }

    channel.writeInt(updated.size());
    for (OCommit37Response.OUpdatedRecordResponse updatedRecord : updated) {
      channel.writeRID(updatedRecord.getRid());
      channel.writeVersion(updatedRecord.getVersion());
    }

    channel.writeInt(deleted.size());
    for (OCommit37Response.ODeletedRecordResponse deleteRecord : deleted) {
      channel.writeRID(deleteRecord.getRid());
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
      int version = network.readVersion();
      created.add(new OCommit37Response.OCreatedRecordResponse(currentRid, createdRid, version));
    }
    final int updatedRecords = network.readInt();

    updated = new ArrayList<>(updatedRecords);
    for (int i = 0; i < updatedRecords; ++i) {
      ORecordId rid = network.readRID();
      int version = network.readVersion();
      updated.add(new OCommit37Response.OUpdatedRecordResponse(rid, version));
    }

    final int deletedRecords = network.readInt();
    deleted = new ArrayList<>(deletedRecords);

    for (int i = 0; i < deletedRecords; ++i) {
      ORecordId rid = network.readRID();
      deleted.add(new OCommit37Response.ODeletedRecordResponse(rid));
    }
  }

  public int getTxId() {
    return txId;
  }

  public List<OCommit37Response.OCreatedRecordResponse> getCreated() {
    return created;
  }

  public List<OCommit37Response.OUpdatedRecordResponse> getUpdated() {
    return updated;
  }

  public List<OCommit37Response.ODeletedRecordResponse> getDeleted() {
    return deleted;
  }
}
