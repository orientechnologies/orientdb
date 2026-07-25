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
  private String commandInfo = null;
  private String commandDetail = null;
  private String lastDatabase = null;
  private String lastUser = null;
  private String serverInfo = null;
  private String caller = null;
  private String driverName = null;
  private String driverVersion = null;
  private short protocolVersion = -1;
  private int sessionId = -1;
  private String clientId = null;
  private String currentUserId = null;
  private String serializationImpl = null;
  private boolean serverUser = false;
  private String serverUsername = null;
  private boolean collectStats = true;
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

  public boolean isCollectStats() {
    return collectStats;
  }

  public void setCollectStats(boolean collectStats) {
    this.collectStats = collectStats;
  }

  public String getServerUsername() {
    return serverUsername;
  }

  public void setServerUsername(String serverUsername) {
    this.serverUsername = serverUsername;
  }

  public boolean isServerUser() {
    return serverUser;
  }

  public void setServerUser(boolean serverUser) {
    this.serverUser = serverUser;
  }

  public String getCurrentUserId() {
    return currentUserId;
  }

  public void setCurrentUserId(String currentUserId) {
    this.currentUserId = currentUserId;
  }

  public String getClientId() {
    return clientId;
  }

  public void setClientId(String clientId) {
    this.clientId = clientId;
  }

  public int getSessionId() {
    return sessionId;
  }

  public void setSessionId(int sessionId) {
    this.sessionId = sessionId;
  }

  public short getProtocolVersion() {
    return protocolVersion;
  }

  public void setProtocolVersion(short protocolVersion) {
    this.protocolVersion = protocolVersion;
  }

  public String getDriverVersion() {
    return driverVersion;
  }

  public void setDriverVersion(String driverVersion) {
    this.driverVersion = driverVersion;
  }

  public String getDriverName() {
    return driverName;
  }

  public void setDriverName(String driverName) {
    this.driverName = driverName;
  }

  public String getCaller() {
    return caller;
  }

  public void setCaller(String caller) {
    this.caller = caller;
  }

  public String getServerInfo() {
    return serverInfo;
  }

  public void setServerInfo(String serverInfo) {
    this.serverInfo = serverInfo;
  }

  public String getLastUser() {
    return lastUser;
  }

  public void setLastUser(String lastUser) {
    this.lastUser = lastUser;
  }

  public String getLastDatabase() {
    return lastDatabase;
  }

  public void setLastDatabase(String lastDatabase) {
    this.lastDatabase = lastDatabase;
  }

  public String getCommandDetail() {
    return commandDetail;
  }

  public void setCommandDetail(String commandDetail) {
    this.commandDetail = commandDetail;
  }

  public String getCommandInfo() {
    return commandInfo;
  }

  public void setCommandInfo(String commandInfo) {
    this.commandInfo = commandInfo;
  }
}
