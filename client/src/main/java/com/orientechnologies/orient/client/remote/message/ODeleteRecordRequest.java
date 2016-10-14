package com.orientechnologies.orient.client.remote.message;

import java.io.IOException;

import com.orientechnologies.orient.client.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;

public class ODeleteRecordRequest implements OBinaryRequest {
  private final ORecordId iRid;
  private final int       iVersion;
  private byte            mode;

  public ODeleteRecordRequest(ORecordId iRid, int iVersion) {
    this.iRid = iRid;
    this.iVersion = iVersion;
  }

  @Override
  public byte getCommand() {
    return OChannelBinaryProtocol.REQUEST_RECORD_DELETE;
  }

  @Override
  public void write(OChannelBinaryAsynchClient network, OStorageRemoteSession session, int mode) throws IOException {
    network.writeRID(iRid);
    network.writeVersion(iVersion);
    network.writeByte((byte) mode);
  }
}