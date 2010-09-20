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

import java.net.InetAddress;
import java.util.HashMap;

import javax.crypto.SecretKey;

import com.orientechnologies.common.log.OLogManager;
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
 * Cluster node handler. Cluster messages are sent using IP Multicast.<br/>
 * Cluster message format:<br/>
 * <br/>
 * 
 * <code>
 * Orient v. &lt;orientdb-version&gt;-&lt;protocol-version&gt;-&lt;cluster-name&gt;-&lt;cluster-password&gt-&lt;callback-address&gt-&lt;callback-port&gt;
 * </code>
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OClusterNode implements OServerHandler {
  protected OServer                         server;

  protected String                          name;
  protected SecretKey                       securityKey;
  protected String                          securityAlgorithm;
  protected InetAddress                     configNetworkMulticastAddress;
  protected int                             configNetworkMulticastPort;
  protected int                             configNetworkMulticastHeartbeat;

  private OClusterDiscoverySignaler         discoverySignaler;
  private OClusterDiscoveryListener         discoveryListener;

  private HashMap<String, OClusterNodeInfo> nodes            = new HashMap<String, OClusterNodeInfo>();

  static final String                       CHECKSUM         = "ChEcKsUm1976";

  static final String                       PACKET_HEADER    = "OrientDB v.";
  static final int                          PROTOCOL_VERSION = 0;

  /**
   * Parse parameters and configure services.
   */
  public void config(final OServer iServer, final OServerParameterConfiguration[] iParams) {
    server = iServer;

    try {
      name = "unknown";
      securityKey = null;
      configNetworkMulticastAddress = InetAddress.getByName("235.1.1.1");
      configNetworkMulticastPort = 2424;
      configNetworkMulticastHeartbeat = 10;
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
            configNetworkMulticastAddress = InetAddress.getByName(param.value);
          else if ("network.multicast.port".equalsIgnoreCase(param.name))
            configNetworkMulticastPort = Integer.parseInt(param.value);
          else if ("network.multicast.heartbeat".equalsIgnoreCase(param.name))
            configNetworkMulticastHeartbeat = Integer.parseInt(param.value);
        }

      if (tempSecurityKey == null) {
        OLogManager.instance().info(this, "Generating Server security key...");
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
    OServerNetworkListener binaryNetworkListener = null;
    for (OServerNetworkListener l : server.getListeners()) {
      if (l.getProtocolType().equals(ONetworkProtocolBinary.class)) {
        binaryNetworkListener = l;
        break;
      }
    }

    if (binaryNetworkListener == null)
      OLogManager.instance().error(this, "Can't find a configured network listener with binary protocol. Can't start cluster node",
          null, OConfigurationException.class);

    discoverySignaler = new OClusterDiscoverySignaler(this, binaryNetworkListener);
    discoveryListener = new OClusterDiscoveryListener(this, binaryNetworkListener);
  }

  public void shutdown() {
    discoverySignaler.sendShutdown();
    discoveryListener.sendShutdown();
  }

  public String getName() {
    return "Cluster node '" + name + "'";
  }

  public void receivedClusterPresence(final String iServerAddress, final int iServerPort) {
    if (nodes.containsKey(iServerAddress))
      // ALREADY REGISTERED, IGNORE IT
      return;

    final OClusterNodeInfo info = new OClusterNodeInfo(iServerAddress, iServerPort);
    nodes.put(iServerAddress, info);

    OLogManager.instance().warn(this, "Discovered new cluster node %s:%d. Trying to connect...", iServerAddress, iServerPort);

  }
}
