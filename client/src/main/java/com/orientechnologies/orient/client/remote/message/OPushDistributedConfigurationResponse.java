package com.orientechnologies.orient.client.remote.message;

import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;

import java.io.IOException;

/**
 * Created by tglman on 11/01/17.
 */
public class OPushDistributedConfigurationResponse implements OBinaryPushResponse {

  @Override
  public void write(OChannelDataOutput network) throws IOException {

  }

  @Override
  public void read(OChannelDataInput channel) throws IOException {

  }
}
