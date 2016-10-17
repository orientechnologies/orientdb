package com.orientechnologies.orient.client.remote.message;

import java.io.IOException;

import com.orientechnologies.orient.client.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.storage.ORecordMetadata;

public class OGetRecordMetadataResponse implements OBinaryResponse<ORecordMetadata> {
  @Override
  public ORecordMetadata read(OChannelBinaryAsynchClient network, OStorageRemoteSession session) throws IOException {
    final ORID responseRid = network.readRID();
    final int responseVersion = network.readVersion();

    return new ORecordMetadata(responseRid, responseVersion);
  }
}