package com.orientechnologies.orient.client.remote.message;

import java.io.IOException;

import com.orientechnologies.orient.client.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinary;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;

public class OCreateRecordRequest implements OBinaryRequest {
  private byte[]    content;
  private ORecordId rid;
  private byte      recordType;
  private byte      mode;

  public OCreateRecordRequest() {
  }

  public byte getMode() {
    return mode;
  }

  @Override
  public byte getCommand() {
    return OChannelBinaryProtocol.REQUEST_RECORD_CREATE;
  }

  public OCreateRecordRequest(byte[] iContent, ORecordId iRid, byte iRecordType) {
    this.content = iContent;
    this.rid = iRid;
    this.recordType = iRecordType;
  }

  // get

  @Override
  public void write(final OChannelBinaryAsynchClient network, final OStorageRemoteSession session, int mode) throws IOException {
    network.writeShort((short) rid.clusterId);
    network.writeBytes(content);
    network.writeByte(recordType);
    network.writeByte((byte) mode);
  }

  public void read(int protocolVersion, OChannelBinary channel) throws IOException {
    final int dataSegmentId = protocolVersion < 24 ? channel.readInt() : 0;

    rid = new ORecordId(channel.readShort(), ORID.CLUSTER_POS_INVALID);
    content = channel.readBytes();
    recordType = channel.readByte();
    mode = channel.readByte();
  }

  public ORecordId getRid() {
    return rid;
  }

  public byte[] getContent() {
    return content;
  }

  public byte getRecordType() {
    return recordType;
  }

}