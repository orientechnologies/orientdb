/*
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.setup;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.instance.HazelcastInstanceProxy;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.distributed.impl.ODistributedPlugin;
import com.orientechnologies.orient.server.network.OServerNetworkListener;
import com.orientechnologies.orient.server.network.protocol.binary.ONetworkProtocolBinary;
import java.io.File;
import java.io.IOException;

/**
 * Running server instance.
 *
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public class ServerRun {
  private final String serverId;
  private String rootPath;
  public OServer server;

  public ServerRun(final String iRootPath, final String serverId) {
    this.rootPath = iRootPath;
    this.serverId = serverId;
  }

  public static String getServerHome(final String iServerId) {
    return "target/servers/" + iServerId;
  }

  @Override
  public String toString() {
    return server.getDistributedManager().getLocalNodeName() + "(" + serverId + ")";
  }

  public OServer getServerInstance() {
    return server;
  }

  public String getServerId() {
    return serverId;
  }

  public String getRootPath() {
    return rootPath;
  }

  public String getBinaryProtocolAddress() {
    final OServerNetworkListener prot = server.getListenerByProtocol(ONetworkProtocolBinary.class);
    if (prot == null) return null;
    return prot.getListeningAddress(true);
  }

  public void deleteNode() {
    System.out.println("Deleting directory " + getServerHome() + "...");
    OFileUtils.deleteRecursively(new File(getServerHome()));
  }

  public boolean isActive() {
    return server.isActive();
  }

  public void crashServer() {
    if (server != null) {
      server.getClientConnectionManager().killAllChannels();
      ((ODistributedPlugin) server.getDistributedManager())
          .getHazelcastInstance()
          .getLifecycleService()
          .terminate();
      server.shutdown();
    }
  }

  public void disconnectFrom(final ServerRun... serverIds) {
    final Node currentNode =
        getHazelcastNode(
            ((ODistributedPlugin) server.getDistributedManager()).getHazelcastInstance());
    for (ServerRun s : serverIds) {
      ((ODistributedPlugin) server.getDistributedManager())
          .closeRemoteServer(s.server.getDistributedManager().getLocalNodeName());
      ((ODistributedPlugin) s.server.getDistributedManager())
          .closeRemoteServer(server.getDistributedManager().getLocalNodeName());

      final Node otherNode =
          getHazelcastNode(
              ((ODistributedPlugin) s.server.getDistributedManager()).getHazelcastInstance());

      currentNode.clusterService.suspectMember(
          currentNode.clusterService.getMember(otherNode.address), "test", true);
      //
      // otherNode.clusterService.suspectMember(currentNode.clusterService.getMember(currentNode.address), "test", true);
    }
  }

  public void rejoin(final ServerRun... serverIds) {
    final Node currentNode =
        getHazelcastNode(
            ((ODistributedPlugin) server.getDistributedManager()).getHazelcastInstance());
    for (ServerRun s : serverIds) {
      final Node otherNode =
          getHazelcastNode(
              ((ODistributedPlugin) s.server.getDistributedManager()).getHazelcastInstance());

      final ClusterServiceImpl clusterService = currentNode.getClusterService();
      clusterService.merge(otherNode.address);
    }
  }

  public static Node getHazelcastNode(final HazelcastInstance hz) {
    HazelcastInstanceImpl impl = getHazelcastInstanceImpl(hz);
    return impl != null ? impl.node : null;
  }

  public static HazelcastInstanceImpl getHazelcastInstanceImpl(final HazelcastInstance hz) {
    HazelcastInstanceImpl impl = null;
    if (hz instanceof HazelcastInstanceProxy) {
      impl = ((HazelcastInstanceProxy) hz).getOriginal();
    } else if (hz instanceof HazelcastInstanceImpl) {
      impl = (HazelcastInstanceImpl) hz;
    }
    return impl;
  }

  public ODatabaseDocument createDatabase(final String iName) {
    server
        .getContext()
        .execute("create database ? plocal users(admin identified by 'admin' role admin)", iName);
    return server.openDatabase(iName, "admin", "admin");
  }

  public void copyDatabase(final String iDatabaseName, final String iDestinationDirectory)
      throws IOException {
    // COPY THE DATABASE TO OTHER DIRECTORIES
    System.out.println(
        "Dropping any previous database '" + iDatabaseName + "' under: " + iDatabaseName + "...");
    OFileUtils.deleteRecursively(new File(iDestinationDirectory));

    System.out.println(
        "Copying database folder " + iDatabaseName + " to " + iDestinationDirectory + "...");
    OFileUtils.copyDirectory(
        new File(getDatabasePath(iDatabaseName)), new File(iDestinationDirectory));
  }

  public OServer startServer(final String iServerConfigFile) throws Exception {
    System.out.println(
        "Starting server with serverId " + serverId + " from " + getServerHome() + "...");

    System.setProperty("ORIENTDB_HOME", getServerHome());

    if (server == null) server = OServerMain.create(false);

    server.setServerRootDirectory(getServerHome());
    server.startup(getClass().getClassLoader().getResourceAsStream(iServerConfigFile));
    server.activate();

    return server;
  }

  public void shutdown() {
    server.shutdown();
  }

  public void shutdownServer() {
    if (server != null) {
      try {
        ((ODistributedPlugin) server.getDistributedManager()).getHazelcastInstance().shutdown();
      } catch (Exception e) {
        // IGNORE IT
      }

      try {
        server.shutdown();
      } catch (Exception e) {
        e.printStackTrace();
        // IGNORE IT
      }
    }

    //    closeStorages();
  }

  public void terminateServer() {
    if (server != null) {
      try {
        final ODistributedPlugin dm = (ODistributedPlugin) server.getDistributedManager();
        if (dm != null) {
          HazelcastInstance hz = dm.getHazelcastInstance();
          final Node node = getHazelcastNode(hz);
          if (node.getNetworkingService() != null) {
            node.getNetworkingService().shutdown();
          }
          node.shutdown(true);
          hz.getLifecycleService().terminate();
        }
      } catch (Exception e) {
        // IGNORE IT
      }

      try {
        server.shutdown();
      } catch (Exception e) {
        // IGNORE IT
      }
    }

    //    closeStorages();
  }

  public void closeStorages() {
    server.getDatabases().close();
  }

  public String getServerHome() {
    return getServerHome(serverId);
  }

  public String getDatabasePath(final String iDatabaseName) {
    return getDatabasePath(serverId, iDatabaseName);
  }

  public static String getDatabasePath(final String iServerId, final String iDatabaseName) {
    return new File(getServerHome(iServerId) + "/databases/" + iDatabaseName).getAbsolutePath();
  }

  public void forceClose(String databaseName) {
    server.getDatabases().forceDatabaseClose(databaseName);
  }
}
