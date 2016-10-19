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
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinary;

public class OSBTGetResponse implements OBinaryResponse<byte[]> {
  private byte[] stream;

  public OSBTGetResponse() {
  }

  public OSBTGetResponse(byte[] stream) {
    this.stream = stream;
  }

  @Override
  public byte[] read(OChannelBinaryAsynchClient network, OStorageRemoteSession session) throws IOException {
    stream = network.readBytes();
    return stream;
  }

  public void write(OChannelBinary channel, int protocolVersion, String recordSerializer) throws IOException {
    channel.writeBytes(stream);
  }

  public byte[] getStream() {
    return stream;
  }
}