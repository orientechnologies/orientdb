/*
 * Copyright 2010-2013 Luca Garulli (l.garulli--at--orientechnologies.com)
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

import java.util.List;

import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Distributed configuration.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class ODistributedConfiguration {
  public static final String NEW_NODE_TAG = "<NEW_NODE>";
  private ODocument          configuration;

  public ODistributedConfiguration(final ODocument iConfiguration) {
    configuration = iConfiguration;
  }

  /**
   * Returns true if the replication is active, otherwise false.
   * 
   * @param iClusterName
   */
  public boolean isReplicationActive(final String iClusterName) {
    if (iClusterName != null) {
      ODocument cluster = getClusterConfiguration(iClusterName);
      if (cluster.containsField("replication"))
        return cluster.field("replication");
    }
    return configuration.field("replication");
  }

  /**
   * Returns the write quorum.
   * 
   * @param iClusterName
   */
  public int getWriteQuorum(final String iClusterName) {
    return (Integer) getClusterConfiguration(iClusterName).field("writeQuorum");
  }

  public String getDefaultPartition(final String iClusterName) {
    return (String) getPartitioningConfiguration(iClusterName).field("default");
  }

  public String getPartitionStrategy(final String iClusterName) {
    return (String) getPartitioningConfiguration(iClusterName).field("strategy");
  }

  public ODistributedConfiguration addNodeInPartition(final String iClusterName, final int iPartition, final String iNode) {
    List<String> partition = getPartition(iClusterName, iPartition);
    partition.add(iNode);
    return this;
  }

  public List<String> getPartition(final String iClusterName, final int iPartition) {
    final List<List<String>> partitions = getPartitions(iClusterName);
    return partitions.get(iPartition);
  }

  public List<List<String>> getPartitions(final String iClusterName) {
    final ODocument partition = getPartitioningConfiguration(iClusterName);
    if (partition == null)
      return null;

    return partition.field("partitions");
  }

  public ODocument getPartitioningConfiguration(final String iClusterName) {
    final ODocument cluster = getClusterConfiguration(iClusterName);
    return (ODocument) cluster.field("partitioning");
  }

  public String getClusterConfigurationName(final String iClusterName) {
    final ODocument clusters = configuration.field("clusters");

    if (clusters == null)
      throw new OConfigurationException("Cannot find 'clusters' in distributed database configuration");

    return clusters.containsField(iClusterName) ? iClusterName : "*";
  }

  public String[] getClusterNames() {
    final ODocument clusters = configuration.field("clusters");
    return clusters.fieldNames();
  }

  public ODocument getClusterConfiguration(final String iClusterName) {
    final ODocument clusters = configuration.field("clusters");

    if (clusters == null)
      throw new OConfigurationException("Cannot find 'clusters' in distributed database configuration");

    ODocument cfg = clusters.field(iClusterName);
    if (cfg == null)
      // GET DEFAULT CLUSTER
      cfg = clusters.field("*");

    return cfg;
  }

  public ODocument serialize() {
    return configuration;
  }
}
