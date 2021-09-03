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
package com.orientechnologies.orient.server.network.protocol;

import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializerFactory;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerBinary;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerNetwork;

/**
 * Saves all the important information about the network connection. Useful for monitoring and
 * statistics.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class ONetworkProtocolData {
  public String commandInfo = null;
  public String commandDetail = null;
  public String lastDatabase = null;
  public String lastUser = null;
  public String serverInfo = null;
  public String caller = null;
  public String driverName = null;
  public String driverVersion = null;
  public short protocolVersion = -1;
  public int sessionId = -1;
  public String clientId = null;
  public String currentUserId = null;
  private String serializationImpl = null;
  public boolean serverUser = false;
  public String serverUsername = null;
  public OCommandRequestText command = null;
  public boolean supportsLegacyPushMessages = true;
  public boolean collectStats = true;
  private ORecordSerializer serializer;

  public String getSerializationImpl() {
    return serializationImpl;
  }

  public void setSerializationImpl(String serializationImpl) {
    if (serializationImpl.equals(ORecordSerializerBinary.NAME)) {
      serializationImpl = ORecordSerializerNetwork.NAME;
    }
    this.serializationImpl = serializationImpl;
    serializer = ORecordSerializerFactory.instance().getFormat(serializationImpl);
  }

  public void setSerializer(ORecordSerializer serializer) {
    this.serializer = serializer;
    this.serializationImpl = serializer.getName();
  }

  public ORecordSerializer getSerializer() {
    return serializer;
  }
}
