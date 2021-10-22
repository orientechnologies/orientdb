package com.orientechnologies.orient.server.config.distributed;

import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "group")
public class OServerDistributedGroupConfiguration {

  @XmlElement public String name;
  @XmlElement public String password;
}
