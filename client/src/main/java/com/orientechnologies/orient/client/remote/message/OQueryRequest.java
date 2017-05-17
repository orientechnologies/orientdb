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

public final class OQueryRequest implements OBinaryRequest<OQueryResponse> {

  private int recordsPerPage = 100;
  private ORecordSerializer   serializer;
  private String              language;
  private String              statement;
  private boolean             idempotent;
  private Map<String, Object> params;
  private boolean             namedParams;

  public OQueryRequest(String language, String iCommand, Object[] positionalParams, boolean idempotent,
      ORecordSerializer serializer, int recordsPerPage) {
    this.language = language;
    this.statement = iCommand;
    params = OStorageRemote.paramsArrayToParamsMap(positionalParams);
    namedParams = false;
    this.serializer = serializer;
    this.recordsPerPage = recordsPerPage;
    if (this.recordsPerPage <= 0) {
      this.recordsPerPage = 100;
    }
  }

  public OQueryRequest(String language, String iCommand, Map<String, Object> namedParams, boolean idempotent,
      ORecordSerializer serializer, int recordsPerPage) {
    this.language = language;
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

  @Override
  public void write(OChannelDataOutput network, OStorageRemoteSession session) throws IOException {
    network.writeString(language);
    network.writeString(statement);
    network.writeBoolean(idempotent);
    network.writeInt(recordsPerPage);

    // params
    ODocument parms = new ODocument();
    parms.field("params", this.params);

    byte[] bytes = OMessageHelper.getRecordBytes(parms, serializer);
    network.writeBytes(bytes);
    network.writeBoolean(namedParams);
  }

  public void read(OChannelDataInput channel, int protocolVersion, ORecordSerializer serializer) throws IOException {
    this.language = channel.readString();
    this.statement = channel.readString();
    this.idempotent = channel.readBoolean();
    this.recordsPerPage = channel.readInt();

    // params
    ODocument paramsDoc = new ODocument();
    byte[] bytes = channel.readBytes();
    serializer.fromStream(bytes, paramsDoc, null);
    this.params = paramsDoc.field("params");
    this.namedParams = channel.readBoolean();
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