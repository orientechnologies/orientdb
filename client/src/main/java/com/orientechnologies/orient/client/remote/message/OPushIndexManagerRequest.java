package com.orientechnologies.orient.client.remote.message;

import com.orientechnologies.orient.client.remote.ORemotePushHandler;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerNetworkV37;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;

import java.io.IOException;

import static com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol.REQUEST_PUSH_INDEX_MANAGER;
import static com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol.REQUEST_PUSH_SCHEMA;

public class OPushIndexManagerRequest implements OBinaryPushRequest<OBinaryPushResponse> {

  private ODocument indexManager;

  public OPushIndexManagerRequest() {

  }

  public OPushIndexManagerRequest(ODocument indexManager) {
    this.indexManager = indexManager;
  }

  @Override
  public void write(OChannelDataOutput channel) throws IOException {
    channel.writeBytes(ORecordSerializerNetworkV37.INSTANCE.toStream(indexManager, false));
  }

  @Override
  public void read(OChannelDataInput network) throws IOException {
    byte[] bytes = network.readBytes();
    this.indexManager = (ODocument) ORecordSerializerNetworkV37.INSTANCE.fromStream(bytes, null, null);
  }

  @Override
  public OBinaryPushResponse execute(ORemotePushHandler pushHandler) {
    return pushHandler.executeUpdateIndexManager(this);
  }

  @Override
  public OBinaryPushResponse createResponse() {
    return null;
  }

  @Override
  public byte getPushCommand() {
    return REQUEST_PUSH_INDEX_MANAGER;
  }

  public ODocument getIndexManager() {
    return indexManager;
  }
}
