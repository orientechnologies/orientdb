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
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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

  public boolean upgrade() {
    boolean modified = false;

    for (String c : getClusterNames()) {
      if (getOriginalServers(c) == null) {
        final ODocument clusterConfig = getClusterConfiguration(c);

        // if (clusterConfig.removeField("replication") != null)
        // modified = true;

        final ODocument partitioning = (ODocument) clusterConfig.removeField("partitioning");
        if (partitioning != null) {
          final Collection partitions = partitioning.field("partitions");
          if (partitions != null) {
            OLogManager.instance().warn(this, "Migrating distributed configuration to the new format for cluster '%s'...", c);
            final List<String> servers = new ArrayList<String>();
            for (Object p : partitions) {
              for (String node : (Collection<String>) p) {
                servers.add(node);
              }
            }
            clusterConfig.field("servers", servers, OType.EMBEDDEDLIST);
          }
          modified = true;
        }
      }
    }
    return modified;
  }

  /**
   * Returns true if the replication is active, otherwise false.
   * 
   * @param iClusterName
   *          Cluster name, or null for *
   */
  public boolean isReplicationActive(final String iClusterName) {
    synchronized (configuration) {
      final ODocument cluster = getClusterConfiguration(iClusterName);
      final Collection<String> servers = cluster.field("servers");
      return servers != null && !servers.isEmpty();
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
   * 
   * @param iClusterName
   *          Cluster name, or null for *
   */
  public int getReadQuorum(final String iClusterName) {
    synchronized (configuration) {
      Object value = getClusterConfiguration(iClusterName).field("readQuorum");
      if (value == null) {
        value = configuration.field("readQuorum");
        if (value == null) {
          OLogManager.instance().warn(this, "readQuorum setting not found for cluster=%s in distributed-config.json", iClusterName);
          return 1;
        }
      }
      return (Integer) value;
    }
  }

  /**
   * Returns the write quorum.
   * 
   * @param iClusterName
   *          Cluster name, or null for *
   */
  public int getWriteQuorum(final String iClusterName) {
    synchronized (configuration) {
      Object value = getClusterConfiguration(iClusterName).field("writeQuorum");
      if (value == null) {
        value = configuration.field("writeQuorum");
        if (value == null) {
          OLogManager.instance()
              .warn(this, "writeQuorum setting not found for cluster=%s in distributed-config.json", iClusterName);
          return 2;
        }
      }
      return (Integer) value;
    }
  }

  /**
   * Returns the write quorum.
   * 
   * @param iClusterName
   *          Cluster name, or null for *
   */
  public boolean getFailureAvailableNodesLessQuorum(final String iClusterName) {
    synchronized (configuration) {
      Object value = getClusterConfiguration(iClusterName).field("failureAvailableNodesLessQuorum");
      if (value == null) {
        value = configuration.field("failureAvailableNodesLessQuorum");
        if (value == null) {
          OLogManager.instance().warn(this,
              "failureAvailableNodesLessQuorum setting not found for cluster=%s in distributed-config.json", iClusterName);
          return false;
        }
      }
      return (Boolean) value;
    }
  }

  /**
   * Reads your writes.
   * 
   * @param iClusterName
   *          Cluster name, or null for *
   */
  public Boolean isReadYourWrites(final String iClusterName) {
    synchronized (configuration) {
      Object value = getClusterConfiguration(iClusterName).field("readYourWrites");
      if (value == null) {
        value = configuration.field("readYourWrites");
        if (value == null) {
          OLogManager.instance().warn(this, "readYourWrites setting not found for cluster=%s in distributed-config.json",
              iClusterName);
          return true;
        }
      }
      return (Boolean) value;
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

  /**
   * Returns one server per cluster involved.
   * 
   * @param iClusterNames
   *          Set of cluster names to find
   * @param iLocalNode
   *          Local node name
   */
  public Collection<String> getOneServerPerCluster(Collection<String> iClusterNames, final String iLocalNode) {
    synchronized (configuration) {
      if (iClusterNames == null || iClusterNames.isEmpty())
        iClusterNames = Collections.singleton("*");

      final Set<String> partitions = new HashSet<String>(iClusterNames.size());
      for (String p : iClusterNames) {
        final ODocument partition = getClusterConfiguration(p);
        if (partition == null)
          return null;

        final List<String> serverList = partition.field("servers");
        if (serverList != null) {
          boolean localNodeFound = false;
          // CHECK IF THE LOCAL NODE IS INVOLVED: IF YES PREFER LOCAL EXECUTION
          for (String s : serverList)
            if (s.equals(iLocalNode)) {
              // FOUND: JUST USE THIS AND CONTINUE WITH THE NEXT PARTITION
              partitions.add(s);
              localNodeFound = true;
              break;
            }

          if (!localNodeFound)
            for (String s : serverList)
              if (!s.equals(NEW_NODE_TAG)) {
                // TODO: USE A ROUND-ROBIN OR RANDOM ALGORITHM
                partitions.add(s);
                break;
              }
        }
      }
      return partitions;
    }
  }

  /**
   * Returns the set of server names involved on the passed cluster collection.
   * 
   * @param iClusterNames
   *          Set of cluster names to find
   */
  public Collection<String> getServers(Collection<String> iClusterNames) {
    synchronized (configuration) {
      if (iClusterNames == null || iClusterNames.isEmpty())
        iClusterNames = Collections.singleton("*");

      final Set<String> partitions = new HashSet<String>(iClusterNames.size());
      for (String p : iClusterNames) {
        final ODocument partition = getClusterConfiguration(p);
        if (partition == null)
          return null;

        final List<String> serverList = partition.field("servers");
        if (serverList != null) {
          for (String s : serverList)
            if (!s.equals(NEW_NODE_TAG))
              partitions.add(s);
        }
      }
      return partitions;
    }
  }

  /**
   * Returns the server list for the default (*) cluster excluding any tags like <NEW_NODES>.
   */
  public Collection<String> getServers() {
    return getServers((String) null);
  }

  /**
   * Returns the server list for the requested cluster cluster excluding any tags like <NEW_NODES>.
   * 
   * @param iClusterName
   *          Cluster name, or null for *
   */
  public Collection<String> getServers(final String iClusterName) {
    synchronized (configuration) {
      final ODocument partition = getClusterConfiguration(iClusterName);
      if (partition == null)
        return null;

      List<String> serverList = partition.field("servers");
      if (serverList != null) {
        // COPY AND REMOVE ANY NEW_NODE_TAG
        serverList = new ArrayList<String>(serverList);
        serverList.remove(NEW_NODE_TAG);
      }

      return serverList;
    }
  }

  /**
   * Returns the server list for the requested cluster.
   * 
   * @param iClusterName
   *          Cluster name, or null for *
   */
  public Collection<String> getOriginalServers(final String iClusterName) {
    synchronized (configuration) {
      final ODocument partition = getClusterConfiguration(iClusterName);
      if (partition == null)
        return null;

      return partition.field("servers");
    }
  }

  /**
   * Returns the array of configured clusters
   */
  public String[] getClusterNames() {
    synchronized (configuration) {
      final ODocument clusters = configuration.field("clusters");
      return clusters.fieldNames();
    }
  }

  /**
   * Get the document representing the cluster configuration.
   * 
   * @param iClusterName
   *          Cluster name, or null for *
   * @return
   */
  public ODocument getClusterConfiguration(String iClusterName) {
    synchronized (configuration) {
      final ODocument clusters = configuration.field("clusters");

      if (clusters == null)
        throw new OConfigurationException("Cannot find 'clusters' in distributed database configuration");

      if (iClusterName == null)
        iClusterName = "*";

      final ODocument cfg;
      if (!clusters.containsField(iClusterName))
        // NO CLUSTER IN CFG: GET THE DEFAULT ONE
        cfg = clusters.field("*");
      else
        // GET THE CLUSTER CFG
        cfg = clusters.field(iClusterName);

      if( cfg == null )
        return new ODocument();

      return cfg;
    }
  }

  public ODocument serialize() {
    return configuration;
  }

  public List<String> addNewNodeInServerList(final String iNode) {
    synchronized (configuration) {
      final List<String> changedPartitions = new ArrayList<String>();
      // NOT FOUND: ADD THE NODE IN CONFIGURATION. LOOK FOR $newNode TAG
      for (String clusterName : getClusterNames()) {
        final Collection<String> partitions = getOriginalServers(clusterName);
        if (partitions != null)
          for (String node : partitions)
            if (node.equalsIgnoreCase(ODistributedConfiguration.NEW_NODE_TAG) && !partitions.contains(iNode)) {
              partitions.add(iNode);
              changedPartitions.add(clusterName);
              break;
            }
      }

      if (!changedPartitions.isEmpty()) {
        incrementVersion();
        return changedPartitions;
      }
    }
    return null;
  }

  public List<String> removeNodeInServerList(final String iNode, final boolean iForce) {
    synchronized (configuration) {
      if (!iForce && isHotAlignment())
        // DO NOTHING
        return null;

      final List<String> changedPartitions = new ArrayList<String>();

      for (String clusterName : getClusterNames()) {
        final Collection<String> nodes = getOriginalServers(clusterName);
        if (nodes != null) {
          for (String node : nodes) {
            if (node.equals(iNode)) {
              // FOUND: REMOVE IT
              nodes.remove(node);
              changedPartitions.add(clusterName);
              break;
            }
          }
        }

        if (!changedPartitions.isEmpty()) {
          incrementVersion();
          return changedPartitions;
        }
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
