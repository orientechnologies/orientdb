package com.orientechnologies.orient.client.remote.message;

import com.orientechnologies.orient.client.binary.OBinaryRequestExecutor;
import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;
import java.io.IOException;

public class OLockRecordRequest implements OBinaryRequest<OLockRecordResponse> {
  private ORID identity;
  private OStorage.LOCKING_STRATEGY lockingStrategy;
  private long timeout;

  public OLockRecordRequest() {}

  public OLockRecordRequest(
      ORID identity, OStorage.LOCKING_STRATEGY lockingStrategy, long timeout) {
    this.identity = identity;
    this.lockingStrategy = lockingStrategy;
    this.timeout = timeout;
  }

  @Override
  public void write(OChannelDataOutput network, OStorageRemoteSession session) throws IOException {
    network.writeRID(identity);
    if (lockingStrategy == OStorage.LOCKING_STRATEGY.SHARED_LOCK) {
      network.writeByte((byte) 1);
    } else if (lockingStrategy == OStorage.LOCKING_STRATEGY.EXCLUSIVE_LOCK) {
      network.writeByte((byte) 2);
    }
    network.writeLong(timeout);
  }

  @Override
  public void read(OChannelDataInput channel, int protocolVersion, ORecordSerializer serializer)
      throws IOException {
    identity = channel.readRID();
    byte lockKind = channel.readByte();
    if (lockKind == 1) {
      this.lockingStrategy = OStorage.LOCKING_STRATEGY.SHARED_LOCK;
    } else if (lockKind == 2) {
      this.lockingStrategy = OStorage.LOCKING_STRATEGY.EXCLUSIVE_LOCK;
    }
    timeout = channel.readLong();
  }

  @Override
  public byte getCommand() {
    return OExperimentalRequest.REQUEST_RECORD_LOCK;
  }

  @Override
  public OLockRecordResponse createResponse() {
    return new OLockRecordResponse();
  }

  @Override
  public OBinaryResponse execute(OBinaryRequestExecutor executor) {
    return executor.executeLockRecord(this);
  }

  @Override
  public String getDescription() {
    return "Lock record";
  }

  public OStorage.LOCKING_STRATEGY getLockingStrategy() {
    return lockingStrategy;
  }

  public ORID getIdentity() {
    return identity;
  }

  public long getTimeout() {
    return timeout;
  }
}
