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
import com.orientechnologies.orient.client.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializerFactory;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinary;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public final class OQueryRequest implements OBinaryRequest<OQueryResponse> {

  private int recordsPerPage = 100;
  private String  serializer;
  private String  statement;
  private boolean idempotent;
  Map<Object, Object> params;
  private boolean namedParams;

  public OQueryRequest(String iCommand, Object[] positionalParams, boolean idempotent, String serializer, int recordsPerPage) {
    this.statement = iCommand;
    params = new HashMap<>();
    if (positionalParams == null) {
      for (int i = 0; i < positionalParams.length; i++) {
        params.put(i, positionalParams[i]);
      }
    }
    namedParams = false;
    this.serializer = serializer;
    this.recordsPerPage = recordsPerPage;
    if (this.recordsPerPage <= 0) {
      this.recordsPerPage = 100;
    }
  }

  public OQueryRequest(String iCommand, Map<String, Object> namedParams, boolean idempotent, String serializer,
      int recordsPerPage) {
    this.statement = iCommand;
    this.params = (Map) namedParams;
    this.namedParams = true;
    this.serializer = serializer;
    this.recordsPerPage = recordsPerPage;
    if (this.recordsPerPage <= 0) {
      this.recordsPerPage = 100;
    }
  }

  public OQueryRequest() {
  }

  @Override public void write(OChannelBinaryAsynchClient network, OStorageRemoteSession session) throws IOException {
    network.writeString(statement);
    network.writeBoolean(idempotent);
    network.writeInt(recordsPerPage);

    //params
    ODocument parms = new ODocument();
    parms.field("params", this.params);

    byte[] bytes = OBinaryProtocolHelper.getRecordBytes(parms, serializer);
    network.writeBytes(bytes);
    network.writeBoolean(namedParams);
  }

  public void read(OChannelBinary channel, int protocolVersion, String serializerName) throws IOException {
    this.statement = channel.readString();
    this.idempotent = channel.readBoolean();
    this.recordsPerPage = channel.readInt();

    //params
    ORecordSerializer ser = ORecordSerializerFactory.instance().getFormat(serializerName);

    ODocument paramsDoc = new ODocument();
    byte[] bytes = channel.readBytes();
    ser.fromStream(bytes, paramsDoc, null);
    this.params = paramsDoc.field("params");
    this.namedParams = channel.readBoolean();
  }

  @Override public byte getCommand() {
    return OChannelBinaryProtocol.REQUEST_QUERY;
  }

  @Override public String getDescription() {
    return "Execute remote query";
  }

  @Override public OQueryResponse createResponse() {
    return new OQueryResponse();
  }

  @Override public OBinaryResponse execute(OBinaryRequestExecutor executor) {
    return executor.executeQuery(this);
  }

  public String getStatement() {
    return statement;
  }

  public Map<Object, Object> getParams() {
    return params;
  }

  public boolean isIdempotent() {
    return idempotent;
  }

  public boolean isNamedParams() {
    return namedParams;
  }

  public Map getNamedParameters() {
    return params;
  }

  public Object[] getPositionalParameters() {
    Object[] result = new Object[params.size()];
    params.entrySet().forEach(e -> {
      result[(int) e.getKey()] = e.getValue();
    });
    return result;
  }

  public int getRecordsPerPage() {
    return recordsPerPage;
  }
}