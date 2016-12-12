package com.orientechnologies.orient.client.remote;

import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;

import java.io.IOException;

/**
 * Created by tglman on 07/06/16.
 */
public interface OBinaryResponse {

  void write(OChannelDataOutput channel, int protocolVersion, String recordSerializer) throws IOException;

  void read(final OChannelDataInput network, OStorageRemoteSession session) throws IOException;
}
