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

import jakarta.xml.bind.annotation.*;

@XmlRootElement(name = "command")
@XmlType(propOrder = {"parameters", "implementation", "pattern"})
@XmlAccessorType(XmlAccessType.FIELD)
public class OServerCommandConfiguration {
  @XmlAttribute(required = true)
  private String pattern;

  @XmlAttribute(required = true)
  private String implementation;

  @XmlAttribute(required = false)
  private boolean stateful;

  @XmlElementWrapper(required = false)
  @XmlElementRef(type = OServerEntryConfiguration.class)
  private OServerEntryConfiguration[] parameters;

  public OServerEntryConfiguration[] getParameters() {
    return parameters;
  }

  public void setParameters(OServerEntryConfiguration[] parameters) {
    this.parameters = parameters;
  }

  public boolean isStateful() {
    return stateful;
  }

  public void setStateful(boolean stateful) {
    this.stateful = stateful;
  }

  public String getImplementation() {
    return implementation;
  }

  public void setImplementation(String implementation) {
    this.implementation = implementation;
  }

  public String getPattern() {
    return pattern;
  }

  public void setPattern(String pattern) {
    this.pattern = pattern;
  }
}
