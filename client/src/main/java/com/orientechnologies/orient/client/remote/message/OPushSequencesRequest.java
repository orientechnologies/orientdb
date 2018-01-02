package com.orientechnologies.orient.client.remote.message;

import com.orientechnologies.orient.client.remote.ORemotePushHandler;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;

import java.io.IOException;

public class OPushSequencesRequest implements OBinaryPushRequest<OBinaryPushResponse> {

  public OPushSequencesRequest() {
  }

  @Override
  public void write(OChannelDataOutput channel) throws IOException {
  }

  @Override
  public void read(OChannelDataInput network) throws IOException {
  }

  @Override
  public OBinaryPushResponse execute(ORemotePushHandler pushHandler) {
    return pushHandler.executeUpdateSequences(this);
  }

  @Override
  public OBinaryPushResponse createResponse() {
    return null;
  }

  @Override
  public byte getPushCommand() {
    return OChannelBinaryProtocol.REQUEST_PUSH_SEQUENCES;
  }
}
