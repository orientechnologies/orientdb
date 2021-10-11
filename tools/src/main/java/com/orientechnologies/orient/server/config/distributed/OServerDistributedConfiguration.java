package com.orientechnologies.orient.server.config.distributed;

import jakarta.xml.bind.annotation.XmlAttribute;
import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlElementRef;
import jakarta.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "distributed")
public class OServerDistributedConfiguration {

  @XmlAttribute public Boolean enabled = false;

  @XmlElement(name = "node-name")
  public String nodeName;

  @XmlElement public Integer quorum;

  @XmlElementRef(type = OServerDistributedNetworkConfiguration.class)
  public OServerDistributedNetworkConfiguration network =
      new OServerDistributedNetworkConfiguration();

  @XmlElementRef(type = OServerDistributedGroupConfiguration.class)
  public OServerDistributedGroupConfiguration group = new OServerDistributedGroupConfiguration();
}
