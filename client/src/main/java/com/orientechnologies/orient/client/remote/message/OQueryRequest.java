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
import java.util.HashMap;
import java.util.Map;

public final class OQueryRequest implements OBinaryRequest<OQueryResponse> {

  public static byte COMMAND = 0;
  public static byte QUERY = 1;
  public static byte EXECUTE = 2;
  public static byte COMMAND_PLAN = 3;
  public static byte QUERY_PLAN = 4;
  public static byte EXECUTE_PLAN = 5;

  private int recordsPerPage = 100;
  private ORecordSerializer serializer;
  private String language;
  private String statement;
  private byte operationType;
  private byte[] paramsBytes;
  private boolean namedParams;

  public static OQueryRequest commandArray(
      String command, Object[] params, ORecordSerializer serializer, int recordsPerPage) {
    return new OQueryRequest(
        "SQL", command, paramsDoc(params), false, COMMAND, serializer, recordsPerPage);
  }

  public static OQueryRequest commandMap(
      String command,
      Map<String, Object> params,
      ORecordSerializer serializer,
      int recordsPerPage) {
    return new OQueryRequest(
        "SQL", command, paramsDoc(params), true, COMMAND, serializer, recordsPerPage);
  }

  public static OQueryRequest queryArray(
      String command, Object[] params, ORecordSerializer serializer, int recordsPerPage) {
    return new OQueryRequest(
        "SQL", command, paramsDoc(params), false, QUERY, serializer, recordsPerPage);
  }

  public static OQueryRequest queryMap(
      String command,
      Map<String, Object> params,
      ORecordSerializer serializer,
      int recordsPerPage) {
    return new OQueryRequest(
        "SQL", command, paramsDoc(params), true, QUERY, serializer, recordsPerPage);
  }

  public static OQueryRequest executeArray(
      String language,
      String command,
      Object[] params,
      ORecordSerializer serializer,
      int recordsPerPage) {
    return new OQueryRequest(
        language, command, paramsDoc(params), false, EXECUTE, serializer, recordsPerPage);
  }

  public static OQueryRequest executeMap(
      String language,
      String command,
      Map<String, Object> params,
      ORecordSerializer serializer,
      int recordsPerPage) {
    return new OQueryRequest(
        language, command, paramsDoc(params), true, EXECUTE, serializer, recordsPerPage);
  }

  private static ODocument paramsDoc(Object[] params) {
    HashMap<String, Object> pm = OStorageRemote.paramsArrayToParamsMap(params);
    ODocument pd = new ODocument();
    pd.field("params", pm);
    return pd;
  }

  private static ODocument paramsDoc(Map<String, Object> params) {
    ODocument pd = new ODocument();
    pd.field("params", params);
    return pd;
  }

  public OQueryRequest(
      String language,
      String command,
      ODocument params,
      boolean namedParams,
      byte operationType,
      ORecordSerializer serializer,
      int recordsPerPage) {
    this.language = language;
    this.statement = command;

    paramsBytes = OMessageHelper.getRecordBytes(params, serializer);
    this.namedParams = namedParams;
    this.serializer = serializer;
    this.recordsPerPage = recordsPerPage;
    if (this.recordsPerPage <= 0) {
      this.recordsPerPage = 100;
    }
    this.operationType = operationType;
  }

  public OQueryRequest() {}

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
    return OChannelBinaryProtocol.REQUEST_QUERY;
  }

  @Override
  public String getDescription() {
    return "Execute remote query";
  }

  @Override
  public OQueryResponse createResponse() {
    return new OQueryResponse();
  }

  @Override
  public OBinaryResponse execute(OBinaryRequestExecutor executor) {
    return executor.executeQuery(this);
  }

  public String getStatement() {
    return statement;
  }

  public Map<String, Object> getParams() {
    ODocument paramsDoc = new ODocument();
    paramsDoc.setTrackingChanges(false);
    serializer.fromStream(this.paramsBytes, paramsDoc, null);
    return paramsDoc.field("params");
  }

  public void setIncludePlan(boolean include) {
    if (include) {
      if (COMMAND == operationType) {
        operationType = COMMAND_PLAN;
      } else if (QUERY == operationType) {
        operationType = QUERY_PLAN;
      } else if (EXECUTE == operationType) {
        operationType = EXECUTE_PLAN;
      }
    } else {
      if (operationType == COMMAND_PLAN) {
        operationType = COMMAND;
      } else if (operationType == QUERY_PLAN) {
        operationType = QUERY;
      } else if (operationType == EXECUTE_PLAN) {
        operationType = EXECUTE;
      }
    }
  }

  public boolean isIncludePlan() {
    if (operationType == COMMAND_PLAN
        || operationType == QUERY_PLAN
        || operationType == EXECUTE_PLAN) {
      return true;
    } else {
      return false;
    }
  }

  public byte getOperationType() {
    if (COMMAND == operationType || operationType == COMMAND_PLAN) {
      return COMMAND;
    } else if (QUERY == operationType || operationType == QUERY_PLAN) {
      return QUERY;
    } else if (EXECUTE == operationType || operationType == EXECUTE_PLAN) {
      return EXECUTE;
    }
    throw new UnsupportedOperationException();
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
}
