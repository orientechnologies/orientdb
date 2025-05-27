package com.orientechnologies.orient.server;

import com.orientechnologies.orient.server.network.protocol.binary.ONetworkProtocolBinary;
import java.lang.ref.WeakReference;

public class OPushInfo {

  private final OClientConnection connection;
  private final WeakReference<ONetworkProtocolBinary> protocol;

  public OPushInfo(OClientConnection connection, WeakReference<ONetworkProtocolBinary> protocol) {
    super();
    this.connection = connection;
    this.protocol = protocol;
  }

  public OClientConnection connection() {
    return connection;
  }

  public WeakReference<ONetworkProtocolBinary> protocol() {
    return protocol;
  }
}
