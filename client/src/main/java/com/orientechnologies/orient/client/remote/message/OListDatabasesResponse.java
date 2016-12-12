package com.orientechnologies.orient.client.remote.message;

import java.io.IOException;
import java.util.Map;

import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializerFactory;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;

public class OListDatabasesResponse implements OBinaryResponse{
  private Map<String, String> databases;

  public OListDatabasesResponse(Map<String, String> databases) {
    this.databases = databases;
  }

  public OListDatabasesResponse() {
  }

  @Override
  public void write(OChannelDataOutput channel, int protocolVersion, String recordSerializer) throws IOException {
    final ODocument result = new ODocument();
    result.field("databases", databases);
    byte[] toSend = ORecordSerializerFactory.instance().getFormat(recordSerializer).toStream(result, false);
    channel.writeBytes(toSend);
  }

  @Override
  public void read(OChannelDataInput network, OStorageRemoteSession session) throws IOException {
    final ODocument result = new ODocument();
    result.fromStream(network.readBytes());
    databases = result.field("databases");
  }

  public Map<String, String> getDatabases() {
    return databases;
  }
}