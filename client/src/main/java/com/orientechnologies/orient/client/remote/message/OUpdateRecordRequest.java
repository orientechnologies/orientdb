package com.orientechnologies.orient.client.remote.message;

import java.io.IOException;

import com.orientechnologies.orient.client.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;

public class OUpdateRecordRequest implements OBinaryRequest {
  private final ORecordId iRid;
  private final byte[]    iContent;
  private final int       iVersion;
  private final boolean   updateContent;
  private final byte      iRecordType;

  public OUpdateRecordRequest(ORecordId iRid, byte[] iContent, int iVersion, boolean updateContent, byte iRecordType) {
    this.iRid = iRid;
    this.iContent = iContent;
    this.iVersion = iVersion;
    this.updateContent = updateContent;
    this.iRecordType = iRecordType;
  }

  @Override
  public byte getCommand() {
    return OChannelBinaryProtocol.REQUEST_RECORD_UPDATE;
  }

  @Override
  public void write(final OChannelBinaryAsynchClient network, final OStorageRemoteSession session, int mode) throws IOException {
    network.writeRID(iRid);
    network.writeBoolean(updateContent);
    network.writeBytes(iContent);
    network.writeVersion(iVersion);
    network.writeByte(iRecordType);
    network.writeByte((byte) mode);
  }
}