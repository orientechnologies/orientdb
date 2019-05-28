package com.orientechnologies.orient.server.config.distributed;

import javax.xml.bind.annotation.XmlElementRef;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "network")
public class OServerDistributedNetworkConfiguration {

  @XmlElementRef(type = OServerDistributedNetworkMulticastConfiguration.class)
  public OServerDistributedNetworkMulticastConfiguration multicast = new OServerDistributedNetworkMulticastConfiguration();
}