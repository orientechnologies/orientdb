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

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.ArrayList;
import java.util.List;

/**
 * Distributed configuration. It uses an ODocument object to store the configuration. Every changes increment the field "version".
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
          return cluster.<Boolean> field("replication");
      }

      return configuration.<Boolean> field("replication");
    }
  }

  /**
   * Returns true if hot alignment is supported.
   * 
   * @return
   */
  public boolean isHotAlignment() {
    synchronized (configuration) {
      final Boolean value = configuration.field("hotAlignment");
      if (value != null)
        return value;
      return true;
    }
  }

  /**
   * Returns the read quorum.
   */
  public int getReadQuorum(final String iClusterName) {
    synchronized (configuration) {
      final Object value = getClusterConfiguration(iClusterName).field("readQuorum");
      if (value != null)
        return (Integer) value;
      else {
        OLogManager.instance().warn(this, "readQuorum setting not found for cluster=%s in distributed-config.json", iClusterName);
        return 1;
      }
    }
  }

  /**
   * Returns the write quorum.
   */
  public int getWriteQuorum(final String iClusterName) {
    synchronized (configuration) {
      final Object value = getClusterConfiguration(iClusterName).field("writeQuorum");
      if (value != null)
        return (Integer) value;
      else {
        OLogManager.instance().warn(this, "writeQuorum setting not found for cluster=%s in distributed-config.json", iClusterName);
        return 2;
      }
    }
  }

  /**
   * Returns the write quorum.
   */
  public boolean getFailureAvailableNodesLessQuorum(final String iClusterName) {
    synchronized (configuration) {
      final Object value = getClusterConfiguration(iClusterName).field("failureAvailableNodesLessQuorum");
      if (value != null)
        return (Boolean) value;
      else {
        OLogManager.instance().warn(this,
            "failureAvailableNodesLessQuorum setting not found for cluster=%s in distributed-config.json", iClusterName);
        return false;
      }
    }
  }

  /**
   * Reads your writes.
   */
  public Boolean isReadYourWrites(final String iClusterName) {
    synchronized (configuration) {
      final Object value = getClusterConfiguration(iClusterName).field("readYourWrites");
      if (value != null)
        return (Boolean) value;
      else {
        OLogManager.instance().warn(this, "readYourWrites setting not found for cluster=%s in distributed-config.json",
            iClusterName);
        return true;
      }
    }
  }

  /**
   * Returns maximum queue size for offline nodes. After this threshold the queue is removed and the offline server needs a complete
   * database deployment as soon as return online.
   */
  public int getOfflineMsgQueueSize() {
    synchronized (configuration) {
      final Object value = configuration.field("offlineMsgQueueSize");
      if (value != null)
        return (Integer) value;
      else {
        OLogManager.instance().debug(this, "offlineMsgQueueSize setting not found in distributed-config.json");
        return 100;
      }
    }
  }

  public int getDefaultPartition(final String iClusterName) {
    synchronized (configuration) {
      final Object value = getClusterConfiguration(iClusterName).field("default");
      if (value != null)
        return (Integer) value;
      else {
        OLogManager.instance().warn(this, "default setting not found for cluster=%s in distributed-config.json", iClusterName);
        return 0;
      }
    }
  }

  public String getPartitionStrategy(final String iClusterName) {
    synchronized (configuration) {
      final Object value = getPartitioningConfiguration(iClusterName).field("strategy");
      if (value != null)
        return (String) value;
      else {
        OLogManager.instance().warn(this, "strategy setting not found for cluster=%s in distributed-config.json", iClusterName);
        return "round-robin";
      }
    }
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
    return configuration;
  }

  public List<String> addNewNodeInPartitions(final String iNode) {
    synchronized (configuration) {
      final List<String> changedPartitions = new ArrayList<String>();
      // NOT FOUND: ADD THE NODE IN CONFIGURATION. LOOK FOR $newNode TAG
      for (String clusterName : getClusterNames()) {
        final List<List<String>> partitions = getPartitions(clusterName);
        if (partitions != null)
          for (int p = 0; p < partitions.size(); ++p) {
            List<String> partition = partitions.get(p);
            for (String node : partition)
              if (node.equalsIgnoreCase(ODistributedConfiguration.NEW_NODE_TAG) && !partition.contains(iNode)) {
                partition.add(iNode);
                changedPartitions.add(clusterName + "." + p);
                break;
              }
          }
      }

      if (!changedPartitions.isEmpty()) {
        incrementVersion();
        return changedPartitions;
      }
    }
    return null;
  }

  public List<String> removeNodeInPartition(final String iNode, final boolean iForce) {
    synchronized (configuration) {
      if (!iForce && isHotAlignment())
        // DO NOTHING
        return null;

      final List<String> changedPartitions = new ArrayList<String>();

      for (String clusterName : getClusterNames()) {
        final List<List<String>> partitions = getPartitions(clusterName);
        if (partitions != null) {
          for (int p = 0; p < partitions.size(); ++p) {
            final List<String> partition = partitions.get(p);

            for (int n = 0; n < partition.size(); ++n) {
              final String node = partition.get(n);

              if (node.equals(iNode)) {
                // FOUND: REMOVE IT
                partition.remove(n);
                changedPartitions.add(clusterName + "." + p);
                break;
              }
            }
          }
        }
      }

      if (!changedPartitions.isEmpty()) {
        incrementVersion();
        return changedPartitions;
      }
    }
    return null;
  }

  protected void incrementVersion() {
    // INCREMENT VERSION
    Integer oldVersion = configuration.field("version");
    if (oldVersion == null)
      oldVersion = 0;
    configuration.field("version", oldVersion.intValue() + 1);
  }
}
