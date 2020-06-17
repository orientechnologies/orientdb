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

public final class OCommitResponse implements OBinaryResponse {
  public static class OCreatedRecordResponse {
    private final ORecordId currentRid;
    private final ORecordId createdRid;

    public OCreatedRecordResponse(ORecordId currentRid, ORecordId createdRid) {
      this.currentRid = currentRid;
      this.createdRid = createdRid;
    }

    public ORecordId getCreatedRid() {
      return createdRid;
    }

    public ORecordId getCurrentRid() {
      return currentRid;
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

  private List<OCreatedRecordResponse> created;
  private List<OUpdatedRecordResponse> updated;
  private Map<UUID, OBonsaiCollectionPointer> collectionChanges;

  public OCommitResponse(
      List<OCreatedRecordResponse> created,
      List<OUpdatedRecordResponse> updated,
      Map<UUID, OBonsaiCollectionPointer> collectionChanges) {
    super();
    this.created = created;
    this.updated = updated;
    this.collectionChanges = collectionChanges;
  }

  public OCommitResponse() {}

  @Override
  public void read(OChannelDataInput network, OStorageRemoteSession session) throws IOException {

    final int createdRecords = network.readInt();
    created = new ArrayList<>(createdRecords);
    ORecordId currentRid;
    ORecordId createdRid;
    for (int i = 0; i < createdRecords; i++) {
      currentRid = network.readRID();
      createdRid = network.readRID();

      created.add(new OCreatedRecordResponse(currentRid, createdRid));
    }
    final int updatedRecords = network.readInt();
    updated = new ArrayList<>(updatedRecords);
    ORecordId rid;
    for (int i = 0; i < updatedRecords; ++i) {
      rid = network.readRID();
      int version = network.readVersion();
      updated.add(new OUpdatedRecordResponse(rid, version));
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
    }

    channel.writeInt(updated.size());
    for (OUpdatedRecordResponse updatedRecord : updated) {
      channel.writeRID(updatedRecord.rid);
      channel.writeVersion(updatedRecord.version);
    }
    if (protocolVersion >= 20) OMessageHelper.writeCollectionChanges(channel, collectionChanges);
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
}
