package com.orientechnologies.orient.test.util;

public interface TestSetup {
  void startServer(String serverId) throws OTestSetupException;

  void start() throws OTestSetupException;

  void shutdownServer(String serverId) throws OTestSetupException;

  void teardown() throws OTestSetupException;

  String getRemoteAddress();

  TestConfig getSetupConfig();
}
