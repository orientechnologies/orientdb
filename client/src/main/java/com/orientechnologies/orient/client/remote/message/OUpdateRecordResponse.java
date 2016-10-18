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
import java.util.Map;
import java.util.UUID;

import com.orientechnologies.orient.client.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OBonsaiCollectionPointer;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinary;

public class OUpdateRecordResponse implements OBinaryResponse<Integer> {

  private int                                 version;
  private Map<UUID, OBonsaiCollectionPointer> changes;

  public OUpdateRecordResponse(int version, Map<UUID, OBonsaiCollectionPointer> changes) {
    this.version = version;
    this.changes = changes;
  }

  public OUpdateRecordResponse() {
  }

  public void write(OChannelBinary channel, int protocolVersion, String recordSerializer) throws IOException {
    channel.writeVersion(version);
    if (protocolVersion >= 20)
      OBinaryProtocolHelper.writeCollectionChanges(channel, changes);
  }

  @Override
  public Integer read(OChannelBinaryAsynchClient network, OStorageRemoteSession session) throws IOException {
    version = network.readVersion();
    changes = OBinaryProtocolHelper.readCollectionChanges(network);
    return version;
  }

  public int getVersion() {
    return version;
  }

  public Map<UUID, OBonsaiCollectionPointer> getChanges() {
    return changes;
  }

}