package com.orientechnologies.orient.client.remote.message;

import java.io.IOException;

import com.orientechnologies.orient.client.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;

public class OCleanOutRecordRequest implements OBinaryRequest {
  private int       recordVersion;
  private ORecordId recordId;
  private byte      mode;

  public OCleanOutRecordRequest(int recordVersion, ORecordId recordId) {
    this.recordVersion = recordVersion;
    this.recordId = recordId;
  }

  public byte getMode() {
    return mode;
  }

  @Override
  public byte getCommand() {
    return OChannelBinaryProtocol.REQUEST_RECORD_CLEAN_OUT;
  }

  @Override
  public void write(OChannelBinaryAsynchClient network, OStorageRemoteSession session, int mode) throws IOException {
    network.writeRID(recordId);
    network.writeVersion(recordVersion);
    network.writeByte((byte) mode);
  }
}