package com.orientechnologies.orient.client.remote.message;

import java.io.IOException;

import com.orientechnologies.orient.client.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;

public class OReadRecordIfVersionIsNotLatestRequest implements OBinaryRequest {
  private final ORecordId rid;
  private final int       recordVersion;
  private final String    fetchPlan;
  private final boolean   ignoreCache;

  public OReadRecordIfVersionIsNotLatestRequest(ORecordId rid, int recordVersion, String fetchPlan, boolean ignoreCache) {
    this.rid = rid;
    this.recordVersion = recordVersion;
    this.fetchPlan = fetchPlan;
    this.ignoreCache = ignoreCache;
  }

  @Override
  public void write(OChannelBinaryAsynchClient network, OStorageRemoteSession session, int mode) throws IOException {
    network.writeRID(rid);
    network.writeVersion(recordVersion);
    network.writeString(fetchPlan != null ? fetchPlan : "");
    network.writeByte((byte) (ignoreCache ? 1 : 0));
  }

  @Override
  public byte getCommand() {
    return OChannelBinaryProtocol.REQUEST_RECORD_LOAD_IF_VERSION_NOT_LATEST;
  }
}