package com.orientechnologies.orient.server.network;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;

class ODefaultServerSocketFactory extends OServerSocketFactory {

  ODefaultServerSocketFactory() {}

  public ServerSocket createServerSocket() throws IOException {
    return new ServerSocket();
  }

  @Override
  public ServerSocket createServerSocket(int port) throws IOException {
    return new ServerSocket(port);
  }

  @Override
  public ServerSocket createServerSocket(int port, int backlog) throws IOException {
    return new ServerSocket(port, backlog);
  }

  @Override
  public ServerSocket createServerSocket(int port, int backlog, InetAddress ifAddress)
      throws IOException {
    return new ServerSocket(port, backlog, ifAddress);
  }

  @Override
  public String getName() {
    return "default";
  }
}
