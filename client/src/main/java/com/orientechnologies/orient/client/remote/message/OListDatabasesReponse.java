package com.orientechnologies.orient.client.remote.message;

import java.io.IOException;
import java.util.Map;

import com.orientechnologies.orient.client.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializerFactory;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinary;

public class OListDatabasesReponse implements OBinaryResponse<Map<String, String>> {
  private Map<String, String> databases;

  public OListDatabasesReponse(Map<String, String> databases) {
    this.databases = databases;
  }

  public OListDatabasesReponse() {
  }

  @Override
  public void write(OChannelBinary channel, int protocolVersion, String recordSerializer) throws IOException {
    final ODocument result = new ODocument();
    result.field("databases", databases);
    byte[] toSend = ORecordSerializerFactory.instance().getFormat(recordSerializer).toStream(result, false);
    channel.writeBytes(toSend);
  }

  @Override
  public Map<String, String> read(OChannelBinaryAsynchClient network, OStorageRemoteSession session) throws IOException {
    final ODocument result = new ODocument();
    result.fromStream(network.readBytes());
    databases = result.field("databases");
    return databases;
  }

  public Map<String, String> getDatabases() {
    return databases;
  }
}