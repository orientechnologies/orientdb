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

@XmlRootElement(name = "security")
@XmlAccessorType(XmlAccessType.FIELD)
public class OServerSecurityConfiguration {
  @XmlElementWrapper
  @XmlAnyElement
  @XmlElementRef(type = OServerUserConfiguration.class)
  private List<OServerUserConfiguration> users;

  @XmlElementWrapper
  @XmlAnyElement
  @XmlElementRef(type = OServerNetworkListenerConfiguration.class)
  private List<OServerResourceConfiguration> resources;

  public OServerSecurityConfiguration() {}

  public OServerSecurityConfiguration(Object iObject) {
    setUsers(new ArrayList<OServerUserConfiguration>());
    setResources(new ArrayList<OServerResourceConfiguration>());
  }

  public List<OServerResourceConfiguration> getResources() {
    return resources;
  }

  public void setResources(List<OServerResourceConfiguration> resources) {
    this.resources = resources;
  }

  public List<OServerUserConfiguration> getUsers() {
    return users;
  }

  public void setUsers(List<OServerUserConfiguration> users) {
    this.users = users;
  }
}
