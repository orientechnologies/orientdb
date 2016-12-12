package com.orientechnologies.orient.client.remote.message;

import java.io.IOException;

import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;

public class OFreezeDatabaseResponse implements OBinaryResponse {
  @Override
  public void write(OChannelDataOutput channel, int protocolVersion, String recordSerializer) throws IOException {

  }

  @Override
  public void read(OChannelDataInput network, OStorageRemoteSession session) throws IOException {
  }
}