package com.orientechnologies.orient.setup;

import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import java.util.Collection;

// A setup allows creating and managing a cluster of OrientDB servers.
public interface TestSetup {
  void setup() throws TestSetupException;

  void teardown() throws TestSetupException;

  void startServer(String serverId) throws TestSetupException;

  void shutdownServer(String serverId) throws TestSetupException;

  String getAddress(String serverId, PortType port);

  OrientDB createRemote(String serverId, OrientDBConfig config);

  OrientDB createRemote(
      String serverId, String serverUser, String serverPassword, OrientDBConfig config);

  OrientDB createRemote(
      Collection<String> serverIds,
      String serverUser,
      String serverPassword,
      OrientDBConfig config);

  enum PortType {
    HTTP,
    BINARY
  }
}
