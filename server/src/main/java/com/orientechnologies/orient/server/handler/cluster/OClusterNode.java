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

import com.orientechnologies.orient.core.config.OParameterConfiguration;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.security.OSecurityManager;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.handler.OServerHandler;

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
  protected OServer                 server;
  
  protected String                  name;
  protected String                  password;
  protected InetAddress             networkAddress;
  protected int                     networkPort;
  protected int                     signalPresenceEverySecs;

  private OClusterDiscoverySignaler discoverySignaler;
  private OClusterDiscoveryListener discoveryListener;

  static final String               PACKET_HEADER    = "OrientDB v.";
  static final int                  PROTOCOL_VERSION = 0;

  /**
   * Parse parameters and configure services.
   */
  public void config(final OServer iServer, final OParameterConfiguration[] iParams) {
    server = iServer;
    
    try {
      name = "unknown";
      password = "";
      networkAddress = InetAddress.getByName("235.1.1.1");
      networkPort = 2424;
      signalPresenceEverySecs = 10;

      if (iParams != null)
        for (OParameterConfiguration param : iParams) {
          if ("name".equalsIgnoreCase(param.name))
            name = param.value;
          else if ("password".equalsIgnoreCase(param.name))
            password = OSecurityManager.instance().digest2String(param.value);
          else if ("networkAddress".equalsIgnoreCase(param.name))
            networkAddress = InetAddress.getByName(param.value);
          else if ("networkPort".equalsIgnoreCase(param.name))
            networkPort = Integer.parseInt(param.value);
          else if ("signalPresenceEverySecs".equalsIgnoreCase(param.name))
            signalPresenceEverySecs = Integer.parseInt(param.value);
        }
    } catch (Exception e) {
      throw new OConfigurationException("Can't configure OrientDB Server as Cluster Node", e);
    }
  }

  public void startup() {
    discoverySignaler = new OClusterDiscoverySignaler(this);
    discoveryListener = new OClusterDiscoveryListener(this);
  }

  public void shutdown() {
    discoverySignaler.sendShutdown();
    discoveryListener.sendShutdown();
  }

  public String getName() {
    return "Cluster node '" + name + "'";
  }
}
