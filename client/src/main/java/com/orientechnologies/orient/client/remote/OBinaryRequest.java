package com.orientechnologies.orient.client.remote;

import com.orientechnologies.orient.client.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinary;

import java.io.IOException;

/**
 * Created by tglman on 07/06/16.
 */
public interface OBinaryRequest {

  void write(final OChannelBinaryAsynchClient network, OStorageRemoteSession session, int mode) throws IOException;

//  void read(OChannelBinary channel, int protocolVersion, String serializerName) throws IOException;

  byte getCommand();

}
