package com.orientechnologies.orient.client.remote.message;

import java.io.IOException;

import com.orientechnologies.orient.client.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;

public final class OHideRecordRequest implements OBinaryRequest {
  private final ORecordId recordId;

  public OHideRecordRequest(ORecordId recordId) {
    this.recordId = recordId;
  }

  @Override
  public byte getCommand() {
    return OChannelBinaryProtocol.REQUEST_RECORD_HIDE;
  }

  @Override
  public void write(final OChannelBinaryAsynchClient network, final OStorageRemoteSession session, int mode) throws IOException {
    network.writeRID(recordId);
    network.writeByte((byte) mode);
  }
}