/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.orient.core.config.OStorageClusterConfiguration;
import com.orientechnologies.orient.core.config.OStorageMemoryClusterConfiguration;
import com.orientechnologies.orient.core.config.OStoragePhysicalClusterConfiguration;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.ODefaultClusterFactory;
import com.orientechnologies.orient.core.storage.impl.local.OClusterLocal;
import com.orientechnologies.orient.core.storage.impl.memory.OClusterMemory;

/**
 * Record cluster factory to support distribution allocation of RID.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class ODistributedRecordClusterFactory extends ODefaultClusterFactory {
  public OCluster createCluster(final String iType) {
    if (iType.equalsIgnoreCase(OClusterLocal.TYPE))
      return new ODistributedRecordClusterLocal();
    else if (iType.equalsIgnoreCase(OClusterMemory.TYPE))
      return new ODistributedRecordClusterMemory();
    else
      return super.createCluster(iType);
  }

  public OCluster createCluster(final OStorageClusterConfiguration iConfig) {
    if (iConfig instanceof OStoragePhysicalClusterConfiguration)
      return new ODistributedRecordClusterLocal();
    else if (iConfig instanceof OStorageMemoryClusterConfiguration)
      return new ODistributedRecordClusterMemory();
    else
      return super.createCluster(iConfig);
  }
}
