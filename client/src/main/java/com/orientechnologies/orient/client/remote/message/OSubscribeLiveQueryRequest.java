package com.orientechnologies.orient.client.remote.message;

import com.orientechnologies.orient.client.binary.OBinaryRequestExecutor;
import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemote;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerNetworkV37Client;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;
import java.io.IOException;
import java.util.Map;

/** Created by tglman on 17/05/17. */
public class OSubscribeLiveQueryRequest implements OBinaryRequest<OSubscribeLiveQueryResponse> {

  private String query;
  private Map<String, Object> params;
  private boolean namedParams;

  public OSubscribeLiveQueryRequest(String query, Map<String, Object> params) {
    this.query = query;
    this.params = params;
    this.namedParams = true;
  }

  public OSubscribeLiveQueryRequest(String query, Object[] params) {
    this.query = query;
    this.params = OStorageRemote.paramsArrayToParamsMap(params);
    this.namedParams = false;
  }

  public OSubscribeLiveQueryRequest() {}

  @Override
  public void write(OChannelDataOutput network, OStorageRemoteSession session) throws IOException {
    ORecordSerializerNetworkV37Client serializer = new ORecordSerializerNetworkV37Client();
    network.writeString(query);
    // params
    ODocument parms = new ODocument();
    parms.field("params", this.params);

    byte[] bytes = OMessageHelper.getRecordBytes(parms, serializer);
    network.writeBytes(bytes);
    network.writeBoolean(namedParams);
  }

  @Override
  public void read(OChannelDataInput channel, int protocolVersion, ORecordSerializer serializer)
      throws IOException {
    this.query = channel.readString();
    ODocument paramsDoc = new ODocument();
    byte[] bytes = channel.readBytes();
    serializer.fromStream(bytes, paramsDoc, null);
    this.params = paramsDoc.field("params");
    this.namedParams = channel.readBoolean();
  }

  @Override
  public byte getCommand() {
    return OChannelBinaryProtocol.SUBSCRIBE_PUSH_LIVE_QUERY;
  }

  @Override
  public OSubscribeLiveQueryResponse createResponse() {
    return new OSubscribeLiveQueryResponse();
  }

  @Override
  public OBinaryResponse execute(OBinaryRequestExecutor executor) {
    return executor.executeSubscribeLiveQuery(this);
  }

  @Override
  public String getDescription() {
    return null;
  }

  public String getQuery() {
    return query;
  }

  public Map<String, Object> getParams() {
    return params;
  }
}
