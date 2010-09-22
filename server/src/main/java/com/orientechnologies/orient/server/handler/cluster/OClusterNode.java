/*
 * Copyright 1999-2010 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.orient.server.handler.cluster;

import java.io.IOException;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.TimerTask;

import javax.crypto.SecretKey;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.security.OSecurityManager;
import com.orientechnologies.orient.core.serialization.OBase64Utils;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerHandlerConfiguration;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.handler.OServerHandler;
import com.orientechnologies.orient.server.network.OServerNetworkListener;
import com.orientechnologies.orient.server.network.protocol.binary.ONetworkProtocolBinary;

/**
 * Cluster node handler. It starts the discovery signaler and listener and manages the cluster configuration. When a node starts
 * it's SLAVE as default. If after a configurable timeout no nodes are joined, then the node became the MASTER.
 * <p>
 * Cluster messages are sent using IP Multicast.
 * </p>
 * <p>
 * Cluster message format:
 * </p>
 * <code>
 * Orient v. &lt;orientdb-version&gt;-&lt;protocol-version&gt;-&lt;cluster-name&gt;-&lt;cluster-password&gt-&lt;callback-address&gt-&lt;callback-port&gt;
 * </code>
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * @see OClusterDiscoveryListener, OClusterDiscoverySignaler
 * 
 */
public class OClusterNode implements OServerHandler {
  protected OServer                      server;

  protected String                       name;
  protected SecretKey                    securityKey;
  protected String                       securityAlgorithm;
  protected InetAddress                  networkMulticastAddress;
  protected int                          networkMulticastPort;
  protected int                          networkMulticastHeartbeat;                              // IN MS
  protected int                          networkTimeoutMaster;                                   // IN MS
  protected int                          networkTimeoutConnectionSlaves;                         // IN MS

  private OClusterDiscoverySignaler      discoverySignaler;
  private OClusterDiscoveryListener      discoveryListener;

  private OClusterSlave                  master           = null;
  private HashMap<String, OClusterSlave> slaves           = new HashMap<String, OClusterSlave>(); ;

  static final String                    CHECKSUM         = "ChEcKsUm1976";

  static final String                    PACKET_HEADER    = "OrientDB v.";
  static final int                       PROTOCOL_VERSION = 0;

  /**
   * Parse parameters and configure services.
   */
  public void config(final OServer iServer, final OServerParameterConfiguration[] iParams) {
    server = iServer;

    try {
      name = "unknown";
      securityKey = null;
      networkMulticastAddress = InetAddress.getByName("235.1.1.1");
      networkMulticastPort = 2424;
      networkMulticastHeartbeat = 5000;
      networkTimeoutMaster = 10000;
      networkTimeoutConnectionSlaves = 2000;
      securityAlgorithm = "Blowfish";
      byte[] tempSecurityKey = null;

      if (iParams != null)
        for (OServerParameterConfiguration param : iParams) {
          if ("name".equalsIgnoreCase(param.name))
            name = param.value;
          else if ("security.algorithm".equalsIgnoreCase(param.name))
            securityAlgorithm = param.value;
          else if ("security.key".equalsIgnoreCase(param.name))
            tempSecurityKey = OBase64Utils.decode(param.value);
          else if ("network.multicast.address".equalsIgnoreCase(param.name))
            networkMulticastAddress = InetAddress.getByName(param.value);
          else if ("network.multicast.port".equalsIgnoreCase(param.name))
            networkMulticastPort = Integer.parseInt(param.value);
          else if ("network.multicast.heartbeat".equalsIgnoreCase(param.name))
            networkMulticastHeartbeat = Integer.parseInt(param.value);
          else if ("network.timeout.master".equalsIgnoreCase(param.name))
            networkTimeoutMaster = Integer.parseInt(param.value);
          else if ("network.timeout.connectionSlaves".equalsIgnoreCase(param.name))
            networkTimeoutConnectionSlaves = Integer.parseInt(param.value);
        }

      if (tempSecurityKey == null) {
        OLogManager.instance().info(this, "Generating Server security key and save it in configuration...");
        // GENERATE NEW SECURITY KEY
        securityKey = OSecurityManager.instance().generateKey(securityAlgorithm, 96);

        // CHANGE AND SAVE THE NEW CONFIGURATION
        for (OServerHandlerConfiguration handler : iServer.getConfiguration().handlers) {
          if (handler.clazz.equals(getClass().getName())) {
            handler.parameters = new OServerParameterConfiguration[iParams.length + 1];
            for (int i = 0; i < iParams.length; ++i) {
              handler.parameters[i] = iParams[i];
            }
            handler.parameters[iParams.length] = new OServerParameterConfiguration("security.key",
                OBase64Utils.encodeBytes(securityKey.getEncoded()));
          }
        }
        iServer.saveConfiguration();

      } else
        // CREATE IT FROM STRING REPRESENTATION
        securityKey = OSecurityManager.instance().createKey(securityAlgorithm, tempSecurityKey);

    } catch (Exception e) {
      throw new OConfigurationException("Can't configure OrientDB Server as Cluster Node", e);
    }
  }

  public void startup() {
    // FIND THE BINARY NETWORK LISTENER
    OServerNetworkListener found = null;
    for (OServerNetworkListener l : server.getListeners()) {
      if (l.getProtocolType().equals(ONetworkProtocolBinary.class)) {
        found = l;
        break;
      }
    }

    final OServerNetworkListener binaryNetworkListener = found;

    if (binaryNetworkListener == null)
      OLogManager.instance().error(this, "Can't find a configured network listener with binary protocol. Can't start cluster node",
          null, OConfigurationException.class);

    // START THE SIGNALER AND WAIT FOR A CONNECTION
    discoverySignaler = new OClusterDiscoverySignaler(this, binaryNetworkListener);
    Orient.getTimer().schedule(new TimerTask() {
      @Override
      public void run() {
        synchronized (this) {
          // TIMEOUT: STOP TO SEND PACKETS TO BEING DISCOVERED
          discoverySignaler.sendShutdown();

          if (master != null)
            // I'M NOT THE MASTER, DO NOTHING
            return;

          // NO NODE HAS JOINED: BECAME THE MASTER AND LISTEN FOR SLAVES
          startListener(binaryNetworkListener);
        }
      }
    }, networkTimeoutMaster);
  }

  public void shutdown() {
    if (discoverySignaler != null)
      discoverySignaler.sendShutdown();
    if (discoveryListener != null)
      discoveryListener.sendShutdown();
  }

  public String getName() {
    return "Cluster node '" + name + "'";
  }

  public void receivedClusterPresence(final String iServerAddress, final int iServerPort) {
    synchronized (slaves) {
      if (slaves.containsKey(iServerAddress))
        // ALREADY REGISTERED, IGNORE IT
        return;

      final OClusterSlave slave = new OClusterSlave(this, iServerAddress, iServerPort);
      slaves.put(iServerAddress, slave);

      OLogManager.instance().warn(this, "Discovered new cluster node %s:%d. Trying to connect...", iServerAddress, iServerPort);

      try {
        slave.connect(networkTimeoutConnectionSlaves);
      } catch (IOException e) {
        OLogManager.instance().error(this, "Can't connect to cluster slave node: %s:%d", iServerAddress, iServerPort);
      }
    }
  }

  private void startListener(OServerNetworkListener binaryNetworkListener) {
    discoveryListener = new OClusterDiscoveryListener(this, binaryNetworkListener);
  }
}
