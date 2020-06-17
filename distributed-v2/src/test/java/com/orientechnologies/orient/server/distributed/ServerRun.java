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
package com.orientechnologies.orient.server.distributed;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.instance.HazelcastInstanceProxy;
import com.hazelcast.instance.Node;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerMain;
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
  protected final String serverId;
  protected String rootPath;
  protected OServer server;

  public ServerRun(final String iRootPath, final String serverId) {
    this.rootPath = iRootPath;
    this.serverId = serverId;
  }

  public static String getServerHome(final String iServerId) {
    return "target/server" + iServerId;
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
      // ((OHazelcastPlugin)
      // server.getDistributedManager()).getHazelcastInstance().getLifecycleService().terminate();
      server.shutdown();
    }
  }

  public void disconnectFrom(final ServerRun... serverIds) {}

  public void rejoin(final ServerRun... serverIds) {
    /*
    final Node currentNode = getHazelcastNode(((OHazelcastPlugin) server.getDistributedManager()).getHazelcastInstance());
    for (ServerRun s : serverIds) {
      final Node otherNode = getHazelcastNode(((OHazelcastPlugin) s.server.getDistributedManager()).getHazelcastInstance());

      final ClusterServiceImpl clusterService = currentNode.getClusterService();
      clusterService.merge(otherNode.address);
    }
    */
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
    server.createDatabase(iName, ODatabaseType.PLOCAL, OrientDBConfig.defaultConfig());
    return server.openDatabase(iName, "admin", "admin");
  }

  /*
    protected ODatabaseDocument getEmbeddedDatabase(final String dbName) {
      String databasesPath = getServerHome() + "/databases";

    	 orientDB = new OrientDB("embedded:" + databasesPath, OrientDBConfig.defaultConfig());

  System.out.println("----- dbPath = " + databasesPath + ", dbName = " + dbName);
  System.out.println("----- dbPath exists() = " + new File(databasesPath + "/fred/" + dbName).exists());

  System.out.println("----- db exists = " + orientDB.exists(dbName));

    	 return orientDB.open(dbName, "admin", "admin");
    }*/

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

  public void shutdownServer() {
    if (server != null) {
      try {
        // ((OHazelcastPlugin) server.getDistributedManager()).getHazelcastInstance().shutdown();
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

  /*
    public void deleteStorages() {
      for (OStorage s : Orient.instance().getStorages()) {
        if (s instanceof OLocalPaginatedStorage && new File(((OLocalPaginatedStorage) s).getStoragePath()).getAbsolutePath()
            .startsWith(getDatabasePath(""))) {
          s.close(true, true);
          Orient.instance().unregisterStorage(s);
        }
      }
    }
  */
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
