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
package com.orientechnologies.orient.client.binary;

import com.orientechnologies.orient.core.config.OContextConfiguration;
import java.io.IOException;

/** Synchronous implementation of binary channel. */
public class OChannelBinarySynchClient extends OChannelBinaryClientAbstract {
  public OChannelBinarySynchClient(
      final String remoteHost,
      final int remotePort,
      final String iDatabaseName,
      final OContextConfiguration iConfig,
      final int protocolVersion)
      throws IOException {
    super(remoteHost, remotePort, iDatabaseName, iConfig, protocolVersion);
  }

  public void beginRequest(final byte iCommand, final int sessionId, final byte[] token)
      throws IOException {
    writeByte(iCommand);
    writeInt(sessionId);
    writeBytes(token);
  }

  public byte[] beginResponse(final boolean token) throws IOException {
    currentStatus = readByte();
    currentSessionId = readInt();

    byte[] tokenBytes;
    if (token) tokenBytes = this.readBytes();
    else tokenBytes = null;
    int opCode = readByte();
    handleStatus(currentStatus, currentSessionId);
    return tokenBytes;
  }
}
