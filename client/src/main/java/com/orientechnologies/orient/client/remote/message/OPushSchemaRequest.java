package com.orientechnologies.orient.client.remote.message;

import com.orientechnologies.orient.client.remote.ORemotePushHandler;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerNetworkV37;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;

import java.io.IOException;

import static com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol.REQUEST_PUSH_SCHEMA;

public class OPushSchemaRequest implements OBinaryPushRequest<OBinaryPushResponse> {

  private ODocument schema;

  public OPushSchemaRequest() {

  }

  public OPushSchemaRequest(ODocument schema) {
    this.schema = schema;
  }

  @Override
  public void write(OChannelDataOutput channel) throws IOException {
    channel.writeBytes(ORecordSerializerNetworkV37.INSTANCE.toStream(schema, false));
  }

  @Override
  public void read(OChannelDataInput network) throws IOException {
    byte[] bytes = network.readBytes();
    this.schema = (ODocument) ORecordSerializerNetworkV37.INSTANCE.fromStream(bytes, null, null);
  }

  @Override
  public OBinaryPushResponse execute(ORemotePushHandler pushHandler) {
    return pushHandler.executeUpdateSchema(this);
  }

  @Override
  public OBinaryPushResponse createResponse() {
    return null;
  }

  @Override
  public byte getPushCommand() {
    return REQUEST_PUSH_SCHEMA;
  }

  public ODocument getSchema() {
    return schema;
  }
}
