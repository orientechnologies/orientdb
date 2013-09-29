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
    synchronized (configuration) {
      if (iClusterName != null) {
        ODocument cluster = getClusterConfiguration(iClusterName);
        if (cluster.containsField("replication"))
          return cluster.field("replication");
      }
      return configuration.field("replication");
    }
  }

  /**
   * Returns the read quorum.
   */
  public int getReadQuorum(final String iClusterName) {
    synchronized (configuration) {
      return (Integer) getClusterConfiguration(iClusterName).field("readQuorum");
    }
  }

  /**
   * Returns the write quorum.
   */
  public int getWriteQuorum(final String iClusterName) {
    synchronized (configuration) {
      return (Integer) getClusterConfiguration(iClusterName).field("writeQuorum");
    }
  }

  /**
   * Returns the write quorum.
   */
  public boolean getFailureAvailableNodesLessQuorum(final String iClusterName) {
    synchronized (configuration) {
      return (Boolean) getClusterConfiguration(iClusterName).field("failureAvailableNodesLessQuorum");
    }
  }

  /**
   * Reads your writes.
   */
  public Boolean isReadYourWrites(final String iClusterName) {
    synchronized (configuration) {
      return (Boolean) getClusterConfiguration(iClusterName).field("readYourWrites");
    }
  }

  public String getDefaultPartition(final String iClusterName) {
    synchronized (configuration) {
      return (String) getPartitioningConfiguration(iClusterName).field("default");
    }
  }

  public String getPartitionStrategy(final String iClusterName) {
    synchronized (configuration) {
      return (String) getPartitioningConfiguration(iClusterName).field("strategy");
    }
  }

  public ODistributedConfiguration addNodeInPartition(final String iClusterName, final int iPartition, final String iNode) {
    synchronized (configuration) {
      getPartition(iClusterName, iPartition).add(iNode);
    }
    return this;
  }

  public List<String> getPartition(final String iClusterName, final int iPartition) {
    synchronized (configuration) {
      final List<List<String>> partitions = getPartitions(iClusterName);
      return partitions.get(iPartition);
    }
  }

  public List<List<String>> getPartitions(final String iClusterName) {
    synchronized (configuration) {
      final ODocument partition = getPartitioningConfiguration(iClusterName);
      if (partition == null)
        return null;

      return partition.field("partitions");
    }
  }

  public ODocument getPartitioningConfiguration(final String iClusterName) {
    synchronized (configuration) {
      final ODocument cluster = getClusterConfiguration(iClusterName);
      return (ODocument) cluster.field("partitioning");
    }
  }

  public String getClusterConfigurationName(final String iClusterName) {
    synchronized (configuration) {
      final ODocument clusters = configuration.field("clusters");

      if (clusters == null)
        throw new OConfigurationException("Cannot find 'clusters' in distributed database configuration");

      return clusters.containsField(iClusterName) ? iClusterName : "*";
    }
  }

  public String[] getClusterNames() {
    synchronized (configuration) {
      final ODocument clusters = configuration.field("clusters");
      return clusters.fieldNames();
    }
  }

  public ODocument getClusterConfiguration(String iClusterName) {
    synchronized (configuration) {
      final ODocument clusters = configuration.field("clusters");

      if (clusters == null)
        throw new OConfigurationException("Cannot find 'clusters' in distributed database configuration");

      if (iClusterName == null)
        iClusterName = "*";

      ODocument cfg = clusters.field(iClusterName);
      if (cfg == null && !iClusterName.equals("*"))
        // GET DEFAULT CLUSTER
        cfg = clusters.field("*");

      return cfg;
    }
  }

  public ODocument serialize() {
    return configuration.copy();
  }
}
