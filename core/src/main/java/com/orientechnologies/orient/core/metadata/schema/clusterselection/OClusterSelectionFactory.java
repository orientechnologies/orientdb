/*
 * Copyright 2010-2014 Orient Technologies LTD (info(at)orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.metadata.schema.clusterselection;

import java.lang.reflect.Field;

import com.orientechnologies.common.factory.OConfigurableStatefulFactory;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;

/**
 * Factory to get the cluster selection strategy.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public class OClusterSelectionFactory extends OConfigurableStatefulFactory<String, OClusterSelectionStrategy> {
  public OClusterSelectionFactory() {
    setDefaultClass(ORoundRobinClusterSelectionStrategy.class);

    register(ODefaultClusterSelectionStrategy.NAME, ODefaultClusterSelectionStrategy.class);
    register(ORoundRobinClusterSelectionStrategy.NAME, ORoundRobinClusterSelectionStrategy.class);
    register(OBalancedClusterSelectionStrategy.NAME, OBalancedClusterSelectionStrategy.class);
    this.registerCustomizedStrategy();
  }

  private void registerCustomizedStrategy() {
    final OGlobalConfiguration cfg = OGlobalConfiguration.findByKey("db.customized.cluster.selection");
    if(cfg != null) {
      String clzName = (String)cfg.getValue();
      try {
        Class clz = Class.forName(clzName);
        Field field = clz.getField("NAME");
        if(field != null) {
          String fieldValue = (String)field.get(clz.newInstance());
          //OLogManager.instance().info(this, "register key : " + fieldValue + " value : " + clz.getName());
          register(fieldValue, clz);
        } else {
          OLogManager.instance().error(this, "NAME field missing");
        }
      }catch(Exception ex) {
        OLogManager.instance().error(this, "failed to register class - " + clzName);
      }        
    }
  }
  
  public OClusterSelectionStrategy getStrategy(final String iStrategy) {
    return newInstance(iStrategy);
  }
}
