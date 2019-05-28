package com.orientechnologies.orient.server.config.distributed;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "group")
public class OServerDistributedGroupConfiguration {

  @XmlElement
  public String name;
  @XmlElement
  public String password;

}