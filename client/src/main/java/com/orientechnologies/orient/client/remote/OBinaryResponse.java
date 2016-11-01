package com.orientechnologies.orient.client.remote;

import com.orientechnologies.orient.client.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinary;

import java.io.IOException;

/**
 * Created by tglman on 07/06/16.
 */
public interface OBinaryResponse {

  void write(OChannelBinary channel, int protocolVersion, String recordSerializer) throws IOException;

  void read(final OChannelBinaryAsynchClient network, OStorageRemoteSession session) throws IOException;
}
