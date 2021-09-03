package com.orientechnologies.orient.client.remote.message;

import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;
import java.io.IOException;

public class OLockRecordResponse implements OBinaryResponse {

  private byte recordType;
  private int version;
  private byte[] record;

  public OLockRecordResponse() {}

  public OLockRecordResponse(byte recordType, int version, byte[] record) {
    this.recordType = recordType;
    this.version = version;
    this.record = record;
  }

  @Override
  public void write(OChannelDataOutput channel, int protocolVersion, ORecordSerializer serializer)
      throws IOException {
    channel.writeByte(recordType);
    channel.writeVersion(version);
    channel.writeBytes(record);
  }

  @Override
  public void read(OChannelDataInput network, OStorageRemoteSession session) throws IOException {
    this.recordType = network.readByte();
    this.version = network.readVersion();
    this.record = network.readBytes();
  }

  public byte getRecordType() {
    return recordType;
  }

  public int getVersion() {
    return version;
  }

  public byte[] getRecord() {
    return record;
  }
}
