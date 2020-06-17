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

import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OBonsaiCollectionPointer;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public final class OCommit37Response implements OBinaryResponse {
  public static class OCreatedRecordResponse {
    private final ORecordId currentRid;
    private final ORecordId createdRid;
    private final int version;

    public OCreatedRecordResponse(ORecordId currentRid, ORecordId createdRid, int version) {
      this.currentRid = currentRid;
      this.createdRid = createdRid;
      this.version = version;
    }

    public ORecordId getCreatedRid() {
      return createdRid;
    }

    public ORecordId getCurrentRid() {
      return currentRid;
    }

    public int getVersion() {
      return version;
    }
  }

  public static class OUpdatedRecordResponse {
    private final ORecordId rid;
    private final int version;

    public OUpdatedRecordResponse(ORecordId rid, int version) {
      this.rid = rid;
      this.version = version;
    }

    public ORecordId getRid() {
      return rid;
    }

    public int getVersion() {
      return version;
    }
  }

  public static class ODeletedRecordResponse {
    private final ORID rid;

    public ODeletedRecordResponse(ORID rid) {
      this.rid = rid;
    }

    public ORID getRid() {
      return rid;
    }
  }

  private List<OCreatedRecordResponse> created;
  private List<OUpdatedRecordResponse> updated;
  private List<ODeletedRecordResponse> deleted;
  private Map<UUID, OBonsaiCollectionPointer> collectionChanges;

  public OCommit37Response(
      List<OCreatedRecordResponse> created,
      List<OUpdatedRecordResponse> updated,
      List<ODeletedRecordResponse> deleted,
      Map<UUID, OBonsaiCollectionPointer> collectionChanges) {
    super();
    this.created = created;
    this.updated = updated;
    this.deleted = deleted;
    this.collectionChanges = collectionChanges;
  }

  public OCommit37Response() {}

  @Override
  public void read(OChannelDataInput network, OStorageRemoteSession session) throws IOException {

    final int createdRecords = network.readInt();
    created = new ArrayList<>(createdRecords);
    ORecordId currentRid;
    ORecordId createdRid;
    for (int i = 0; i < createdRecords; i++) {
      currentRid = network.readRID();
      createdRid = network.readRID();
      int version = network.readInt();
      created.add(new OCreatedRecordResponse(currentRid, createdRid, version));
    }
    final int updatedRecords = network.readInt();
    updated = new ArrayList<>(updatedRecords);

    for (int i = 0; i < updatedRecords; ++i) {
      ORecordId rid = network.readRID();
      int version = network.readVersion();
      updated.add(new OUpdatedRecordResponse(rid, version));
    }

    final int deletedRecords = network.readInt();
    deleted = new ArrayList<>(deletedRecords);

    for (int i = 0; i < deletedRecords; ++i) {
      ORecordId rid = network.readRID();
      deleted.add(new ODeletedRecordResponse(rid));
    }

    collectionChanges = OMessageHelper.readCollectionChanges(network);
  }

  @Override
  public void write(OChannelDataOutput channel, int protocolVersion, ORecordSerializer serializer)
      throws IOException {

    channel.writeInt(created.size());
    for (OCreatedRecordResponse createdRecord : created) {
      channel.writeRID(createdRecord.currentRid);
      channel.writeRID(createdRecord.createdRid);
      channel.writeInt(createdRecord.version);
    }

    channel.writeInt(updated.size());
    for (OUpdatedRecordResponse updatedRecord : updated) {
      channel.writeRID(updatedRecord.rid);
      channel.writeVersion(updatedRecord.version);
    }

    channel.writeInt(deleted.size());
    for (ODeletedRecordResponse deleteRecord : deleted) {
      channel.writeRID(deleteRecord.rid);
    }

    OMessageHelper.writeCollectionChanges(channel, collectionChanges);
  }

  public List<OCreatedRecordResponse> getCreated() {
    return created;
  }

  public List<OUpdatedRecordResponse> getUpdated() {
    return updated;
  }

  public Map<UUID, OBonsaiCollectionPointer> getCollectionChanges() {
    return collectionChanges;
  }

  public List<ODeletedRecordResponse> getDeleted() {
    return deleted;
  }
}
