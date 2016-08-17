/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.orient.core.db;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.db.ODatabase.ATTRIBUTES;

/**
 * Created by tglman on 27/06/16.
 */
public class OrientDBConfig {

  private OrientDBConfig                parent;
  private final OContextConfiguration   configurations;
  private final Map<ATTRIBUTES, Object> attributes;
  private final Set<ODatabaseListener>  listeners;

  protected OrientDBConfig() {
    configurations = new OContextConfiguration();
    attributes = new HashMap<>();
    parent = null;
    listeners = new HashSet<>();
  }

  protected OrientDBConfig(OContextConfiguration configurations, Map<ATTRIBUTES, Object> attributes,
      Set<ODatabaseListener> listeners) {
    this.configurations = configurations;
    this.attributes = attributes;
    parent = null;
    this.listeners = listeners;
  }

  public static OrientDBConfig defaultConfig() {
    return new OrientDBConfig();
  }

  public static OrientDBConfigBuilder builder() {
    return new OrientDBConfigBuilder();
  }

  public Set<ODatabaseListener> getListeners() {
    return listeners;
  }

  public OContextConfiguration getConfigurations() {
    return configurations;
  }

  public Map<ATTRIBUTES, Object> getAttributes() {
    return attributes;
  }

  protected void setParent(OrientDBConfig parent) {
    this.parent = parent;
  }

}
