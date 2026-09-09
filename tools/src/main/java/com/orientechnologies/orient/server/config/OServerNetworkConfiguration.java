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

import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlAnyElement;
import jakarta.xml.bind.annotation.XmlElementRef;
import jakarta.xml.bind.annotation.XmlElementWrapper;
import jakarta.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;
import java.util.List;

@XmlRootElement(name = "network")
@XmlAccessorType(XmlAccessType.FIELD)
public class OServerNetworkConfiguration {
  @XmlElementWrapper
  @XmlAnyElement
  @XmlElementRef(type = OServerSocketFactoryConfiguration.class)
  private List<OServerSocketFactoryConfiguration> sockets;

  @XmlElementWrapper
  @XmlAnyElement
  @XmlElementRef(type = OServerNetworkProtocolConfiguration.class)
  private List<OServerNetworkProtocolConfiguration> protocols;

  @XmlElementWrapper
  @XmlAnyElement
  @XmlElementRef(type = OServerNetworkListenerConfiguration.class)
  private List<OServerNetworkListenerConfiguration> listeners;

  public OServerNetworkConfiguration() {}

  public OServerNetworkConfiguration(Object iObject) {
    setProtocols(new ArrayList<OServerNetworkProtocolConfiguration>());
    getProtocols()
        .add(
            new OServerNetworkProtocolConfiguration(
                "binary",
                "com.orientechnologies.orient.server.network.protocol.binary.ONetworkProtocolBinary"));

    setListeners(new ArrayList<OServerNetworkListenerConfiguration>());
    getListeners().add(new OServerNetworkListenerConfiguration());
  }

  public List<OServerNetworkListenerConfiguration> getListeners() {
    return listeners;
  }

  public void setListeners(List<OServerNetworkListenerConfiguration> listeners) {
    this.listeners = listeners;
  }

  public List<OServerNetworkProtocolConfiguration> getProtocols() {
    return protocols;
  }

  public void setProtocols(List<OServerNetworkProtocolConfiguration> protocols) {
    this.protocols = protocols;
  }

  public List<OServerSocketFactoryConfiguration> getSockets() {
    return sockets;
  }

  public void setSockets(List<OServerSocketFactoryConfiguration> sockets) {
    this.sockets = sockets;
  }
}
