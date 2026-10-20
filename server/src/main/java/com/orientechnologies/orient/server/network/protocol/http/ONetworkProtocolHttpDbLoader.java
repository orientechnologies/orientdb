package com.orientechnologies.orient.server.network.protocol.http;

import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.network.protocol.ONetworkProtocol;
import com.orientechnologies.orient.server.network.protocol.ONetworkProtocolLoader;

public class ONetworkProtocolHttpDbLoader implements ONetworkProtocolLoader {

  @Override
  public String getName() {
    return ONetworkProtocolHttpDb.class.getSimpleName();
  }

  @Override
  public String getClassName() {
    return ONetworkProtocolHttpDb.class.getName();
  }

  @Override
  public int getVersion() {
    return ONetworkProtocolHttpDb.CURRENT_PROTOCOL_VERSION;
  }

  @Override
  public ONetworkProtocol newInstance(OServer server) {
    return new ONetworkProtocolHttpDb(server);
  }
}
