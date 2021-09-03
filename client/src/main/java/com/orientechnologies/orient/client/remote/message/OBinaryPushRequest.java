package com.orientechnologies.orient.client.remote.message;

import com.orientechnologies.orient.client.remote.ORemotePushHandler;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;
import java.io.IOException;

/** Created by tglman on 11/01/17. */
public interface OBinaryPushRequest<T extends OBinaryPushResponse> {
  void write(OChannelDataOutput channel) throws IOException;

  void read(final OChannelDataInput network) throws IOException;

  T execute(ORemotePushHandler remote);

  OBinaryPushResponse createResponse();

  byte getPushCommand();
}
