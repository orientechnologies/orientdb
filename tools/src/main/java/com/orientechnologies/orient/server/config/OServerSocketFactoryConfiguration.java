/*
 * Copyright 2014 Charles Baptiste (cbaptiste--at--blacksparkcorp.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.server.config;

import jakarta.xml.bind.annotation.*;

@XmlRootElement(name = "socket")
@XmlType(propOrder = {"parameters", "implementation", "name"})
@XmlAccessorType(XmlAccessType.FIELD)
public class OServerSocketFactoryConfiguration {

  @XmlAttribute(required = true)
  private String name;

  @XmlAttribute(required = true)
  private String implementation;

  @XmlElementWrapper
  @XmlElementRef(type = OServerParameterConfiguration.class)
  private OServerParameterConfiguration[] parameters;

  public OServerSocketFactoryConfiguration() {}

  public OServerSocketFactoryConfiguration(String name, String implementation) {
    this.setName(name);
    this.setImplementation(implementation);
  }

  public OServerParameterConfiguration[] getParameters() {
    return parameters;
  }

  public void setParameters(OServerParameterConfiguration[] parameters) {
    this.parameters = parameters;
  }

  public String getImplementation() {
    return implementation;
  }

  public void setImplementation(String implementation) {
    this.implementation = implementation;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }
}
