package com.orientechnologies.orient.client.remote.message;

import java.io.IOException;

import com.orientechnologies.orient.client.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;

public final class OReadRecordRequest implements OBinaryRequest {
  private final boolean   iIgnoreCache;
  private final ORecordId iRid;
  private final String    iFetchPlan;

  public OReadRecordRequest(boolean iIgnoreCache, ORecordId iRid, String iFetchPlan) {
    this.iIgnoreCache = iIgnoreCache;
    this.iRid = iRid;
    this.iFetchPlan = iFetchPlan;
  }

  @Override
  public void write(OChannelBinaryAsynchClient network, OStorageRemoteSession session, int mode) throws IOException {
    network.writeRID(iRid);
    network.writeString(iFetchPlan != null ? iFetchPlan : "");
    network.writeByte((byte) (iIgnoreCache ? 1 : 0));
    network.writeByte((byte) 0);
  }

  @Override
  public byte getCommand() {
    return OChannelBinaryProtocol.REQUEST_RECORD_LOAD;
  }
}