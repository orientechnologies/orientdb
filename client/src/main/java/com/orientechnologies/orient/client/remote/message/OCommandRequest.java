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
import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializerFactory;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerAnyStreamable;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinary;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;

public final class OCommandRequest implements OBinaryRequest {
  private boolean             asynch;
  private OCommandRequestText query;
  private boolean             live;

  public OCommandRequest(boolean asynch, OCommandRequestText iCommand, boolean live) {
    this.asynch = asynch;
    this.query = iCommand;
    this.live = live;
  }

  public OCommandRequest() {
  }

  @Override
  public void write(OChannelBinaryAsynchClient network, OStorageRemoteSession session, int mode) throws IOException {
    if (live) {
      network.writeByte((byte) 'l');
    } else {
      network.writeByte((byte) (asynch ? 'a' : 's')); // ASYNC / SYNC
    }
    network.writeBytes(OStreamSerializerAnyStreamable.INSTANCE.toStream(query));

  }

  public void read(OChannelBinary channel, int protocolVersion, String serializerName) throws IOException {

    byte type = channel.readByte();
    if (type == (byte) 'l')
      live = true;
    if (type == (byte) 'a')
      asynch = true;
    ORecordSerializer ser = ORecordSerializerFactory.instance().getFormat(serializerName);
    query = OStreamSerializerAnyStreamable.INSTANCE.fromStream(channel.readBytes(), ser);
  }

  @Override
  public byte getCommand() {
    return OChannelBinaryProtocol.REQUEST_COMMAND;
  }

  public OCommandRequestText getQuery() {
    return query;
  }

  public boolean isAsynch() {
    return asynch;
  }

  public boolean isLive() {
    return live;
  }

}