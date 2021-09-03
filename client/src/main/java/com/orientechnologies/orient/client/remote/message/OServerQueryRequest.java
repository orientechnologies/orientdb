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
import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemote;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;
import java.io.IOException;
import java.util.Map;

public final class OServerQueryRequest implements OBinaryRequest<OServerQueryResponse> {

  public static byte COMMAND = 0;
  public static byte QUERY = 1;
  public static byte EXECUTE = 2;

  private int recordsPerPage = Integer.MAX_VALUE;
  private ORecordSerializer serializer;
  private String language;
  private String statement;
  private byte operationType;
  private Map<String, Object> params;
  private byte[] paramsBytes;
  private boolean namedParams;

  public OServerQueryRequest(
      String language,
      String iCommand,
      Object[] positionalParams,
      byte operationType,
      ORecordSerializer serializer,
      int recordsPerPage) {
    this.language = language;
    this.statement = iCommand;
    params = OStorageRemote.paramsArrayToParamsMap(positionalParams);
    namedParams = false;
    this.serializer = serializer;
    //    this.recordsPerPage = recordsPerPage;
    //    if (this.recordsPerPage <= 0) {
    //      this.recordsPerPage = 100;
    //    }
    this.operationType = operationType;
    ODocument parms = new ODocument();
    parms.field("params", this.params);

    paramsBytes = OMessageHelper.getRecordBytes(parms, serializer);
  }

  public OServerQueryRequest(
      String language,
      String iCommand,
      Map<String, Object> namedParams,
      byte operationType,
      ORecordSerializer serializer,
      int recordsPerPage) {
    this.language = language;
    this.statement = iCommand;
    this.params = (Map) namedParams;
    ODocument parms = new ODocument();
    parms.field("params", this.params);

    paramsBytes = OMessageHelper.getRecordBytes(parms, serializer);
    this.namedParams = true;
    this.serializer = serializer;
    this.recordsPerPage = recordsPerPage;
    if (this.recordsPerPage <= 0) {
      this.recordsPerPage = 100;
    }
    this.operationType = operationType;
  }

  public OServerQueryRequest() {}

  @Override
  public void write(OChannelDataOutput network, OStorageRemoteSession session) throws IOException {
    network.writeString(language);
    network.writeString(statement);
    network.writeByte(operationType);
    network.writeInt(recordsPerPage);
    // THIS IS FOR POSSIBLE FUTURE FETCH PLAN
    network.writeString(null);

    // params
    network.writeBytes(paramsBytes);
    network.writeBoolean(namedParams);
  }

  public void read(OChannelDataInput channel, int protocolVersion, ORecordSerializer serializer)
      throws IOException {
    this.language = channel.readString();
    this.statement = channel.readString();
    this.operationType = channel.readByte();
    this.recordsPerPage = channel.readInt();
    // THIS IS FOR POSSIBLE FUTURE FETCH PLAN
    channel.readString();

    this.paramsBytes = channel.readBytes();
    this.namedParams = channel.readBoolean();
    this.serializer = serializer;
  }

  @Override
  public byte getCommand() {
    return OChannelBinaryProtocol.REQUEST_SERVER_QUERY;
  }

  @Override
  public String getDescription() {
    return "Execute remote query";
  }

  @Override
  public OServerQueryResponse createResponse() {
    return new OServerQueryResponse();
  }

  @Override
  public OBinaryResponse execute(OBinaryRequestExecutor executor) {
    return executor.executeServerQuery(this);
  }

  public String getStatement() {
    return statement;
  }

  public Map<String, Object> getParams() {
    if (params == null && this.paramsBytes != null) {
      // params
      ODocument paramsDoc = new ODocument();
      paramsDoc.setTrackingChanges(false);
      serializer.fromStream(this.paramsBytes, paramsDoc, null);
      this.params = paramsDoc.field("params");
    }
    return params;
  }

  public byte getOperationType() {
    return operationType;
  }

  public boolean isNamedParams() {
    return namedParams;
  }

  public Map getNamedParameters() {
    return getParams();
  }

  public Object[] getPositionalParameters() {
    Map<String, Object> params = getParams();
    if (params == null) return null;
    Object[] result = new Object[params.size()];
    params
        .entrySet()
        .forEach(
            e -> {
              result[Integer.parseInt(e.getKey())] = e.getValue();
            });
    return result;
  }

  public int getRecordsPerPage() {
    return recordsPerPage;
  }

  public ORecordSerializer getSerializer() {
    return serializer;
  }

  public String getLanguage() {
    return language;
  }

  public void setLanguage(String language) {
    this.language = language;
  }

  public boolean requireDatabaseSession() {
    return false;
  }
}
