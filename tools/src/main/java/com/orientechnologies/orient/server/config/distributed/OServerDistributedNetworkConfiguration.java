package com.orientechnologies.orient.server.config.distributed;

import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlElementRef;
import jakarta.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "network")
@XmlAccessorType(XmlAccessType.FIELD)
public class OServerDistributedNetworkConfiguration {

  @XmlElementRef(type = OServerDistributedNetworkMulticastConfiguration.class)
  private OServerDistributedNetworkMulticastConfiguration multicast =
      new OServerDistributedNetworkMulticastConfiguration();

  public OServerDistributedNetworkMulticastConfiguration getMulticast() {
    return multicast;
  }

  public void setMulticast(OServerDistributedNetworkMulticastConfiguration multicast) {
    this.multicast = multicast;
  }
}
