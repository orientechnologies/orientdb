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
package com.orientechnologies.orient.core.storage;

import java.util.Arrays;

import com.orientechnologies.orient.core.config.OStorageClusterConfiguration;
import com.orientechnologies.orient.core.config.OStorageMemoryClusterConfiguration;
import com.orientechnologies.orient.core.config.OStorageMemoryLinearHashingClusterConfiguration;
import com.orientechnologies.orient.core.config.OStoragePhysicalClusterConfigurationLocal;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OPaginatedCluster;
import com.orientechnologies.orient.core.storage.impl.memory.OClusterMemory;
import com.orientechnologies.orient.core.storage.impl.memory.OClusterMemoryArrayList;
import com.orientechnologies.orient.core.storage.impl.memory.OClusterMemoryHashing;

public class ODefaultClusterFactory implements OClusterFactory {
  protected static final String[] TYPES = { OClusterMemory.TYPE, OPaginatedCluster.TYPE };

  public OCluster createCluster(final String type) {
    if (type.equalsIgnoreCase(OClusterMemory.TYPE))
      return new OClusterMemoryArrayList();
    else
      throw new OStorageException("Cluster type '" + type + "' is not supported. Supported types are: " + Arrays.toString(TYPES));
  }

  public OCluster createCluster(final OStorageClusterConfiguration config) {
    if (config instanceof OStorageMemoryClusterConfiguration)
      return new OClusterMemoryArrayList();
    else if (config instanceof OStorageMemoryLinearHashingClusterConfiguration)
      return new OClusterMemoryHashing();
    else
      throw new OStorageException("Cluster type '" + config + "' is not supported. Supported types are: " + Arrays.toString(TYPES));
  }

  public String[] getSupported() {
    return TYPES;
  }

  @Override
  public boolean isSupported(final String iClusterType) {
    for (String type : TYPES)
      if (type.equalsIgnoreCase(iClusterType))
        return true;
    return false;
  }
}
