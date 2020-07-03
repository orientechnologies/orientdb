package com.orientechnologies.orient.test.util;

import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;

public interface TestSetup {
  enum PortType {
    HTTP,
    BINARY
  }

  void startServer(String serverId) throws OTestSetupException;

  void start() throws OTestSetupException;

  void shutdownServer(String serverId) throws OTestSetupException;

  void teardown() throws OTestSetupException;

  String getAddress(String serverId, PortType port);

  OrientDB createRemote(String serverId, OrientDBConfig config);

  OrientDB createRemote(
      String serverId, String serverUser, String serverPassword, OrientDBConfig config);
}
