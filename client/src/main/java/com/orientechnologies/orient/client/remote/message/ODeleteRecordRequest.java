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

import com.orientechnologies.orient.client.binary.OBinaryRequestExecutor;
import com.orientechnologies.orient.client.remote.OBinaryAsyncRequest;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;
import java.io.IOException;

public class ODeleteRecordRequest implements OBinaryAsyncRequest<ODeleteRecordResponse> {
  private ORecordId rid;
  private int version;
  private byte mode;

  public ODeleteRecordRequest(ORecordId iRid, int iVersion) {
    this.rid = iRid;
    this.version = iVersion;
  }

  public ODeleteRecordRequest() {}

  @Override
  public byte getCommand() {
    return OChannelBinaryProtocol.REQUEST_RECORD_DELETE;
  }

  @Override
  public String getDescription() {
    return "Delete Record";
  }

  public void read(OChannelDataInput channel, int protocolVersion, ORecordSerializer serializer)
      throws IOException {
    rid = channel.readRID();
    version = channel.readVersion();
    mode = channel.readByte();
  }

  @Override
  public void write(OChannelDataOutput network, OStorageRemoteSession session) throws IOException {
    network.writeRID(rid);
    network.writeVersion(version);
    network.writeByte((byte) mode);
  }

  public byte getMode() {
    return mode;
  }

  public ORecordId getRid() {
    return rid;
  }

  public int getVersion() {
    return version;
  }

  @Override
  public void setMode(byte mode) {
    this.mode = mode;
  }

  @Override
  public ODeleteRecordResponse createResponse() {
    return new ODeleteRecordResponse();
  }

  @Override
  public OBinaryResponse execute(OBinaryRequestExecutor executor) {
    return executor.executeDeleteRecord(this);
  }
}
