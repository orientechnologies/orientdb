package com.orientechnologies.orient.server.network.protocol.binary;

import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.network.protocol.ONetworkProtocol;
import com.orientechnologies.orient.server.network.protocol.ONetworkProtocolLoader;

public class ONetworkProtocolBinaryLoader implements ONetworkProtocolLoader {

  @Override
  public String getName() {
    return ONetworkProtocolBinary.class.getSimpleName();
  }

  @Override
  public String getClassName() {
    return ONetworkProtocolBinary.class.getName();
  }

  @Override
  public int getVersion() {
    return OChannelBinaryProtocol.CURRENT_PROTOCOL_VERSION;
  }

  @Override
  public ONetworkProtocol newInstance(OServer server) {
    return new ONetworkProtocolBinary(server);
  }
}
