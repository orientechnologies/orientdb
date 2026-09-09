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

import jakarta.xml.bind.annotation.*;

@XmlRootElement(name = "listener")
@XmlType(propOrder = {"commands", "parameters", "protocol", "socket", "portRange", "ipAddress"})
@XmlAccessorType(XmlAccessType.FIELD)
public class OServerNetworkListenerConfiguration {

  @XmlAttribute(name = "ip-address", required = true)
  private String ipAddress = "127.0.0.1";

  @XmlAttribute(name = "port-range")
  private String portRange = "2424-2430";

  @XmlAttribute private String protocol = "binary";

  @XmlAttribute private String socket = "default";

  @XmlElementWrapper
  @XmlElementRef(type = OServerParameterConfiguration.class)
  private OServerParameterConfiguration[] parameters;

  @XmlElementWrapper(required = false)
  @XmlAnyElement
  @XmlElementRef(type = OServerCommandConfiguration.class)
  private OServerCommandConfiguration[] commands;

  public OServerCommandConfiguration[] getCommands() {
    return commands;
  }

  public void setCommands(OServerCommandConfiguration[] commands) {
    this.commands = commands;
  }

  public OServerParameterConfiguration[] getParameters() {
    return parameters;
  }

  public void setParameters(OServerParameterConfiguration[] parameters) {
    this.parameters = parameters;
  }

  public String getSocket() {
    return socket;
  }

  public void setSocket(String socket) {
    this.socket = socket;
  }

  public String getProtocol() {
    return protocol;
  }

  public void setProtocol(String protocol) {
    this.protocol = protocol;
  }

  public String getPortRange() {
    return portRange;
  }

  public void setPortRange(String portRange) {
    this.portRange = portRange;
  }

  public String getIpAddress() {
    return ipAddress;
  }

  public void setIpAddress(String ipAddress) {
    this.ipAddress = ipAddress;
  }
}
