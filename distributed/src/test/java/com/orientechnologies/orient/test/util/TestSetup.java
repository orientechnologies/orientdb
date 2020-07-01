package com.orientechnologies.orient.test.util;

public interface TestSetup {
  enum PortType {
    HTTP, BINARY
  }

  void startServer(String serverId) throws OTestSetupException;

  void start() throws OTestSetupException;

  void shutdownServer(String serverId) throws OTestSetupException;

  void teardown() throws OTestSetupException;

  String getAddress(String serverId, PortType port);

  TestConfig getSetupConfig();
}
