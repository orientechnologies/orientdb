/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.server.config;

import jakarta.xml.bind.annotation.XmlAnyElement;
import jakarta.xml.bind.annotation.XmlElementRef;
import jakarta.xml.bind.annotation.XmlElementWrapper;
import jakarta.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;
import java.util.List;

@XmlRootElement(name = "network")
public class OServerNetworkConfiguration {
  @XmlElementWrapper
  @XmlAnyElement
  @XmlElementRef(type = OServerSocketFactoryConfiguration.class)
  public List<OServerSocketFactoryConfiguration> sockets;

  @XmlElementWrapper
  @XmlAnyElement
  @XmlElementRef(type = OServerNetworkProtocolConfiguration.class)
  public List<OServerNetworkProtocolConfiguration> protocols;

  @XmlElementWrapper
  @XmlAnyElement
  @XmlElementRef(type = OServerNetworkListenerConfiguration.class)
  public List<OServerNetworkListenerConfiguration> listeners;

  public OServerNetworkConfiguration() {}

  public OServerNetworkConfiguration(Object iObject) {
    protocols = new ArrayList<OServerNetworkProtocolConfiguration>();
    protocols.add(
        new OServerNetworkProtocolConfiguration(
            "binary",
            "com.orientechnologies.orient.server.network.protocol.binary.ONetworkProtocolBinary"));

    listeners = new ArrayList<OServerNetworkListenerConfiguration>();
    listeners.add(new OServerNetworkListenerConfiguration());
  }
}
