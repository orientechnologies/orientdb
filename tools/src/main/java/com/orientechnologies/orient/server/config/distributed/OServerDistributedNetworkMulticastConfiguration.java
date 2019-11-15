package com.orientechnologies.orient.server.config.distributed;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "multicast")
public class OServerDistributedNetworkMulticastConfiguration {

  @XmlAttribute
  public boolean enabled = false;
  @XmlElement(name = "address")
  public String  ip      = "230.0.0.0";
  @XmlElement
  public Integer port    = 2424;

  @XmlElementWrapper(name = "discovery-ports")
  @XmlElement(name = "port")
  public int[] discoveryPorts = new int[] { 2424 };

}