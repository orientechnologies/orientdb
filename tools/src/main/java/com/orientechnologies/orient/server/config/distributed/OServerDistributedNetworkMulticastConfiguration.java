package com.orientechnologies.orient.server.config.distributed;

import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlAttribute;
import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlElementWrapper;
import jakarta.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "multicast")
@XmlAccessorType(XmlAccessType.FIELD)
public class OServerDistributedNetworkMulticastConfiguration {

  @XmlAttribute private boolean enabled = false;

  @XmlElement(name = "address")
  private String ip = "230.0.0.0";

  @XmlElement private Integer port = 2424;

  @XmlElementWrapper(name = "discovery-ports")
  @XmlElement(name = "port")
  private int[] discoveryPorts = new int[] {2424};

  public int[] getDiscoveryPorts() {
    return discoveryPorts;
  }

  public void setDiscoveryPorts(int[] discoveryPorts) {
    this.discoveryPorts = discoveryPorts;
  }

  public Integer getPort() {
    return port;
  }

  public void setPort(Integer port) {
    this.port = port;
  }

  public String getIp() {
    return ip;
  }

  public void setIp(String ip) {
    this.ip = ip;
  }

  public boolean isEnabled() {
    return enabled;
  }

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }
}
