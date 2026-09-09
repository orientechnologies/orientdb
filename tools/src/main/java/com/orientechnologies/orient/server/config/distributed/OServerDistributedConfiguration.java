package com.orientechnologies.orient.server.config.distributed;

import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlAttribute;
import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlElementRef;
import jakarta.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "distributed")
@XmlAccessorType(XmlAccessType.FIELD)
public class OServerDistributedConfiguration {

  @XmlAttribute private Boolean enabled = false;

  @XmlElement(name = "node-name")
  private String nodeName;

  @XmlElement private Integer quorum;

  @XmlElementRef(type = OServerDistributedNetworkConfiguration.class)
  private OServerDistributedNetworkConfiguration network =
      new OServerDistributedNetworkConfiguration();

  @XmlElementRef(type = OServerDistributedGroupConfiguration.class)
  private OServerDistributedGroupConfiguration group = new OServerDistributedGroupConfiguration();

  public OServerDistributedGroupConfiguration getGroup() {
    return group;
  }

  public void setGroup(OServerDistributedGroupConfiguration group) {
    this.group = group;
  }

  public OServerDistributedNetworkConfiguration getNetwork() {
    return network;
  }

  public void setNetwork(OServerDistributedNetworkConfiguration network) {
    this.network = network;
  }

  public Integer getQuorum() {
    return quorum;
  }

  public void setQuorum(Integer quorum) {
    this.quorum = quorum;
  }

  public String getNodeName() {
    return nodeName;
  }

  public void setNodeName(String nodeName) {
    this.nodeName = nodeName;
  }

  public Boolean getEnabled() {
    return enabled;
  }

  public void setEnabled(Boolean enabled) {
    this.enabled = enabled;
  }
}
