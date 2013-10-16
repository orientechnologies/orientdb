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
package com.orientechnologies.orient.server.hazelcast;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.orientechnologies.orient.server.distributed.ODistributedConfiguration;
import com.orientechnologies.orient.server.distributed.ODistributedPartition;

/**
 * Hazelcast implementation for distributed partition.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OHazelcastDistributionPartition implements ODistributedPartition {
  private final Set<String> nodes = new HashSet<String>(5);

  public OHazelcastDistributionPartition(final List<String> nodes) {
    for (String n : nodes)
      if (!n.equals(ODistributedConfiguration.NEW_NODE_TAG))
        this.nodes.add(n);
  }

  public Set<String> getNodes() {
    return nodes;
  }
}
