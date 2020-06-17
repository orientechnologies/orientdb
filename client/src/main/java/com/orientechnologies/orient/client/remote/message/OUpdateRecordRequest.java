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
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;
import java.io.IOException;

public class OUpdateRecordRequest implements OBinaryAsyncRequest<OUpdateRecordResponse> {
  private ORecordId rid;
  private byte[] rawContent;
  private ORecord content;
  private int version;
  private boolean updateContent = true;
  private byte recordType;
  private byte mode;

  public OUpdateRecordRequest(
      ORecordId iRid, byte[] iContent, int iVersion, boolean updateContent, byte iRecordType) {
    this.rid = iRid;
    this.rawContent = iContent;
    this.version = iVersion;
    this.updateContent = updateContent;
    this.recordType = iRecordType;
  }

  public OUpdateRecordRequest(
      ORecordId iRid, ORecord iContent, int iVersion, boolean updateContent, byte iRecordType) {
    this.rid = iRid;
    this.version = iVersion;
    this.updateContent = updateContent;
    this.content = iContent;
    this.recordType = iRecordType;
  }

  public OUpdateRecordRequest() {}

  @Override
  public byte getCommand() {
    return OChannelBinaryProtocol.REQUEST_RECORD_UPDATE;
  }

  @Override
  public String getDescription() {
    return "Update Record";
  }

  public void read(OChannelDataInput channel, int protocolVersion, ORecordSerializer serializer)
      throws IOException {
    rid = channel.readRID();
    if (protocolVersion >= 23) updateContent = channel.readBoolean();
    byte[] bts = channel.readBytes();
    version = channel.readVersion();
    recordType = channel.readByte();
    mode = channel.readByte();

    content =
        Orient.instance()
            .getRecordFactoryManager()
            .newInstance(
                recordType,
                rid.getClusterId(),
                ODatabaseRecordThreadLocal.instance().getIfDefined());
    serializer.fromStream(bts, content, null);
  }

  @Override
  public void write(final OChannelDataOutput network, final OStorageRemoteSession session)
      throws IOException {
    network.writeRID(rid);
    network.writeBoolean(updateContent);
    network.writeBytes(rawContent);
    network.writeVersion(version);
    network.writeByte(recordType);
    network.writeByte((byte) mode);
  }

  public ORecord getContent() {
    return content;
  }

  public byte getMode() {
    return mode;
  }

  public byte getRecordType() {
    return recordType;
  }

  public ORecordId getRid() {
    return rid;
  }

  public int getVersion() {
    return version;
  }

  public boolean isUpdateContent() {
    return updateContent;
  }

  public void setMode(byte mode) {
    this.mode = mode;
  }

  @Override
  public OUpdateRecordResponse createResponse() {
    return new OUpdateRecordResponse();
  }

  @Override
  public OBinaryResponse execute(OBinaryRequestExecutor executor) {
    return executor.executeUpdateRecord(this);
  }
}
