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
import com.orientechnologies.orient.core.storage.ORecordMetadata;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;
import java.io.IOException;

public class OGetRecordMetadataResponse implements OBinaryResponse {

  private ORecordMetadata metadata;

  public OGetRecordMetadataResponse() {}

  public OGetRecordMetadataResponse(ORecordMetadata metadata) {
    this.metadata = metadata;
  }

  @Override
  public void read(OChannelDataInput network, OStorageRemoteSession session) throws IOException {
    ORecordId recordId = network.readRID();
    int version = network.readVersion();
    metadata = new ORecordMetadata(recordId, version);
  }

  public void write(OChannelDataOutput channel, int protocolVersion, ORecordSerializer serializer)
      throws IOException {
    channel.writeRID(metadata.getRecordId());
    channel.writeVersion(metadata.getVersion());
  }

  public ORecordMetadata getMetadata() {
    return metadata;
  }
}
