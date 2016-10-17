package com.orientechnologies.orient.client.remote.message;

import java.io.IOException;

import com.orientechnologies.orient.client.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.OStorageOperationResult;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;

public class OReadRecordIfVersionIsNotLatestResponse implements OBinaryResponse<OStorageOperationResult<ORawBuffer>> {
  @Override
  public OStorageOperationResult<ORawBuffer> read(OChannelBinaryAsynchClient network, OStorageRemoteSession session)
      throws IOException {

    if (network.readByte() == 0)
      return new OStorageOperationResult<ORawBuffer>(null);

    byte type = network.readByte();
    int recVersion = network.readVersion();
    byte[] bytes = network.readBytes();
    ORawBuffer buffer = new ORawBuffer(bytes, recVersion, type);

    final ODatabaseDocument database = ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();
    ORecord record;

    while (network.readByte() == 2) {
      record = (ORecord) OChannelBinaryProtocol.readIdentifiable(network);

      if (database != null)
        // PUT IN THE CLIENT LOCAL CACHE
        database.getLocalCache().updateRecord(record);
    }
    return new OStorageOperationResult<ORawBuffer>(buffer);
  }
}