package com.orientechnologies.orient.distributed.impl;

import com.orientechnologies.orient.distributed.OrientDBDistributed;
import com.orientechnologies.orient.server.distributed.ODistributedConfiguration;
import com.orientechnologies.orient.server.distributed.ORemoteServerAvailabilityCheck;
import com.orientechnologies.orient.server.distributed.ORemoteServerController;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ODistributedNetworkManager implements ODiscoveryListener {

  private final ConcurrentMap<String, ORemoteServerController> remoteServers = new ConcurrentHashMap<String, ORemoteServerController>();
  private final String                                         localNodeName;
  private final ORemoteServerAvailabilityCheck                 check;
  private final OrientDBDistributed                            orientDB;
  private final int                                            quorum;
  private       OMulticastNodeDiscoveryManager                 discoveryManager;

  public ODistributedNetworkManager(ORemoteServerAvailabilityCheck check, OrientDBDistributed orientDB,
      ONodeConfiguration config) {
    this.localNodeName = config.getNodeName();
    this.check = check;
    this.orientDB = orientDB;
    this.quorum = config.getQuorum();
  }

  public ORemoteServerController getRemoteServer(final String rNodeName) {
    return remoteServers.get(rNodeName);
  }

  public ORemoteServerController connectRemoteServer(final String rNodeName, String host, String user, String password)
      throws IOException {
    // OK
    ORemoteServerController remoteServer = new ORemoteServerController(check, localNodeName, rNodeName, host, user, password);
    final ORemoteServerController old = remoteServers.putIfAbsent(rNodeName, remoteServer);
    if (old != null) {
      remoteServer.close();
      remoteServer = old;
    }
    return remoteServer;
  }

  public void closeRemoteServer(final String node) {
    final ORemoteServerController c = remoteServers.remove(node);
    if (c != null)
      c.close();
  }

  public void closeAll() {
    for (ORemoteServerController server : remoteServers.values())
      server.close();
    remoteServers.clear();
  }

  public void startup() {
    //TODO different strategies for different infrastructures, eg. AWS

    //TODO get info from config
    String multicastIp = "230.0.0.0";
    int multicastPort = 4321;
    int[] pingPorts = { 4321 };
    String group = "default";

    discoveryManager = new OMulticastNodeDiscoveryManager(group, localNodeName, quorum, this, multicastPort, multicastIp, pingPorts,
        orientDB);
    discoveryManager.start();
  }

  public void shutdown() {
    closeAll();
    //TODO
  }

  public ODistributedConfiguration getDistributedConfiguration() {
    return null;//TODO
  }

  @Override
  public void nodeJoined(NodeData data) {

  }

  public void nodeLeft(NodeData data) {

  }
}
