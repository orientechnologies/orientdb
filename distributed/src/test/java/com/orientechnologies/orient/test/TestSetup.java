package com.orientechnologies.orient.test;

import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;

public interface TestSetup {
  enum PortType {
    HTTP,
    BINARY
  }

  void startServer(String serverId) throws TestSetupException;

  void start() throws TestSetupException;

  void shutdownServer(String serverId) throws TestSetupException;

  void teardown() throws TestSetupException;

  String getAddress(String serverId, PortType port);

  OrientDB createRemote(String serverId, OrientDBConfig config);

  OrientDB createRemote(
      String serverId, String serverUser, String serverPassword, OrientDBConfig config);
}
