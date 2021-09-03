package com.orientechnologies.orient.server;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class OClientSessions {

  private List<OClientConnection> connections =
      Collections.synchronizedList(new ArrayList<OClientConnection>());
  private byte[] binaryToken;

  public OClientSessions(byte[] binaryToken) {
    this.binaryToken = binaryToken;
  }

  public void addConnection(OClientConnection conn) {
    this.connections.add(conn);
  }

  public void removeConnection(OClientConnection conn) {
    this.connections.remove(conn);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(binaryToken);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (!(obj instanceof OClientSessions)) return false;
    return Arrays.equals(this.binaryToken, ((OClientSessions) obj).binaryToken);
  }

  public boolean isActive() {
    return !connections.isEmpty();
  }

  public List<OClientConnection> getConnections() {
    return connections;
  }
}
