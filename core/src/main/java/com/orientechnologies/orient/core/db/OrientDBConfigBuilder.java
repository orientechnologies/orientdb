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
import java.util.Map;

import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabase.ATTRIBUTES;

public class OrientDBConfigBuilder {

  private OContextConfiguration   configurations = new OContextConfiguration();
  private Map<ATTRIBUTES, Object> attributes     = new HashMap<>();
  private Map<String, Object>     properties     = new HashMap<>();

  public OrientDBConfigBuilder fromGlobalMap(Map<OGlobalConfiguration, Object> values) {
    for (Map.Entry<OGlobalConfiguration, Object> entry : values.entrySet()) {
      addConfig(entry.getKey(), entry.getValue());
    }
    return this;
  }

  public OrientDBConfigBuilder fromMap(Map<String, Object> values) {
    for (Map.Entry<String, Object> entry : values.entrySet()) {
      addConfig(OGlobalConfiguration.valueOf(entry.getKey()), entry.getValue());
    }
    return this;
  }

  public OrientDBConfigBuilder addConfig(OGlobalConfiguration configuration, Object value) {
    configurations.setValue(configuration, value);
    return this;
  }

  public OrientDBConfigBuilder addAttribute(ATTRIBUTES attribute, Object value) {
    attributes.put(attribute, value);
    return this;
  }

  public OrientDBConfigBuilder addProperty(String property, Object value) {
    properties.put(property, value);
    return this;
  }

  public OrientDBConfig build() {
    return new OrientDBConfig(configurations, attributes, properties);
  }
}
