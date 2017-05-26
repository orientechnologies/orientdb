package com.orientechnologies.orient.client.remote.message;

import com.orientechnologies.common.exception.OErrorCode;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;

import java.io.IOException;
import java.util.Map;

/**
 * Created by tglman on 25/05/17.
 */
public class OError37Response implements OBinaryResponse {

  private OErrorCode          code;
  private Map<String, String> messages;
  private byte[]              verbose;

  public OError37Response(OErrorCode code, Map<String, String> messages, byte[] verbose) {
    this.code = code;
    this.messages = messages;
    this.verbose = verbose;
  }

  @Override
  public void write(OChannelDataOutput channel, int protocolVersion, ORecordSerializer serializer) throws IOException {

  }

  @Override
  public void read(OChannelDataInput network, OStorageRemoteSession session) throws IOException {

  }
}
