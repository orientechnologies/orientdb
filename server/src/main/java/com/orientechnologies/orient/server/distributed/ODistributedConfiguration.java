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
package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;

import java.util.*;

/**
 * Distributed configuration. It uses an ODocument object to store the configuration. Every changes increment the field "version".
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class ODistributedConfiguration {
  public static final String       NEW_NODE_TAG         = "<NEW_NODE>";
  private static final Set<String> DEFAULT_CLUSTER_NAME = Collections.singleton("*");
  private ODocument                configuration;

  public enum ROLES {
    MASTER, REPLICA
  };

  public ODistributedConfiguration(final ODocument iConfiguration) {
    configuration = iConfiguration;
  }

  /**
   * Returns true if the replication is active, otherwise false.
   * 
   * @param iClusterName
   *          Cluster name, or null for *
   */
  public boolean isReplicationActive(final String iClusterName, final String iLocalNode) {
    synchronized (configuration) {
      final Collection<String> servers = getClusterConfiguration(iClusterName).field("servers");
      if (servers != null && !servers.isEmpty()) {
        return true;
      }
      return false;
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
          OLogManager.instance().warn(this, "writeQuorum setting not found for cluster=%s in distributed-config.json",
              iClusterName);
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
   * Returns the execution mode if synchronous.
   *
   * @param iClusterName
   *          Cluster name, or null for *
   * @return true = synchronous, false = asynchronous, null = undefined
   */
  public Boolean isExecutionModeSynchronous(final String iClusterName) {
    synchronized (configuration) {
      Object value = getClusterConfiguration(iClusterName).field("executionMode");
      if (value == null) {
        value = configuration.field("executionMode");
        if (value == null)
          return null;
      }

      if (value.toString().equalsIgnoreCase("undefined"))
        return null;

      return value.toString().equalsIgnoreCase("synchronous");
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
   * Returns the list of servers that can manage a list of clusters. The algorithm makes its best to involve the less servers as it
   * can.
   * 
   * @param iClusterNames
   *          Set of cluster names to find
   * @param iLocalNode
   *          Local node name
   */
  public Map<String, Collection<String>> getServerClusterMap(Collection<String> iClusterNames, final String iLocalNode) {
    synchronized (configuration) {
      if (iClusterNames == null || iClusterNames.isEmpty())
        iClusterNames = DEFAULT_CLUSTER_NAME;

      final Map<String, Collection<String>> servers = new HashMap<String, Collection<String>>(iClusterNames.size());

      // TRY TO SEE IF IT CAN BE EXECUTED ON LOCAL NODE ONLY
      boolean canUseLocalNode = true;
      for (String p : iClusterNames) {
        final List<String> serverList = getClusterConfiguration(p).field("servers");
        if (serverList != null && !serverList.contains(iLocalNode)) {
          canUseLocalNode = false;
          break;
        }
      }

      if (canUseLocalNode) {
        // USE LOCAL NODE ONLY (MUCH FASTER)
        servers.put(iLocalNode, iClusterNames);
        return servers;
      }

      if (iClusterNames.size() == 1) {
        final List<String> serverList = getClusterConfiguration(iClusterNames.iterator().next()).field("servers");

        for (String s : serverList) {
          if (NEW_NODE_TAG.equalsIgnoreCase(s))
            continue;

          // PICK THE FIRST ONE
          servers.put(s, iClusterNames);
          return servers;
        }
      }

      // GROUP BY SERVER WITH THE NUMBER OF CLUSTERS
      final Map<String, Collection<String>> serverMap = new HashMap<String, Collection<String>>();
      for (String p : iClusterNames) {
        final List<String> serverList = getClusterConfiguration(p).field("servers");
        for (String s : serverList) {
          if (NEW_NODE_TAG.equalsIgnoreCase(s))
            continue;

          Collection<String> clustersInServer = serverMap.get(s);
          if (clustersInServer == null) {
            clustersInServer = new HashSet<String>();
            serverMap.put(s, clustersInServer);
          }
          clustersInServer.add(p);
        }
      }

      if (serverMap.size() == 1)
        // RETURN THE ONLY SERVER INVOLVED
        return serverMap;

      // ORDER BY NUMBER OF CLUSTERS
      final List<String> orderedServers = new ArrayList<String>(serverMap.keySet());
      Collections.sort(orderedServers, new Comparator<String>() {
        @Override
        public int compare(final String o1, final String o2) {
          return ((Integer) serverMap.get(o2).size()).compareTo((Integer) serverMap.get(o1).size());
        }
      });

      // BROWSER ORDERED SERVER MAP PUTTING THE MINIMUM SERVER TO COVER ALL THE CLUSTERS
      final Set<String> remainingClusters = new HashSet<String>(iClusterNames); // KEEPS THE REMAINING CLUSTER TO ADD IN FINAL
                                                                                // RESULT
      final Set<String> includedClusters = new HashSet<String>(iClusterNames.size()); // KEEPS THE COLLECTION OF ALREADY INCLUDED
                                                                                      // CLUSTERS
      for (String s : orderedServers) {
        final Collection<String> clusters = serverMap.get(s);

        if (!servers.isEmpty()) {
          // FILTER CLUSTER LIST AVOIDING TO REPEAT CLUSTERS ALREADY INCLUDED ON PREVIOUS NODES
          clusters.removeAll(includedClusters);
        }

        servers.put(s, clusters);
        remainingClusters.removeAll(clusters);
        includedClusters.addAll(clusters);

        if (remainingClusters.isEmpty())
          // FOUND ALL CLUSTERS
          break;
      }

      return servers;
    }
  }

  /**
   * Returns the local cluster. This is used when a cluster must be selected: local is always the best choice.
   * 
   * @param iClusterNames
   *          Set of cluster names
   * @param iLocalNode
   *          Local node name
   */
  public String getLocalCluster(Collection<String> iClusterNames, final String iLocalNode) {
    synchronized (configuration) {
      if (iClusterNames == null || iClusterNames.isEmpty())
        iClusterNames = DEFAULT_CLUSTER_NAME;

      for (String p : iClusterNames) {
        final String masterServer = getMasterServer(p);
        if (iLocalNode.equals(masterServer))
          // FOUND: JUST USE THIS
          return p;
      }

      // NO MASTER FOUND: RETURN THE FIRST CLUSTER NAME
      return null;
    }
  }

  /**
   * Returns the set of server names involved on the passed cluster collection.
   * 
   * @param iClusterNames
   *          Set of cluster names to find
   */
  public Set<String> getServers(Collection<String> iClusterNames) {
    synchronized (configuration) {
      if (iClusterNames == null || iClusterNames.isEmpty())
        iClusterNames = DEFAULT_CLUSTER_NAME;

      final Set<String> partitions = new HashSet<String>(iClusterNames.size());
      for (String p : iClusterNames) {
        final List<String> serverList = getClusterConfiguration(p).field("servers");
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
   * Returns the server list for the requested cluster cluster excluding any tags like <NEW_NODES> and iExclude if any.
   * 
   * @param iClusterName
   *          Cluster name, or null for *
   * @param iExclude
   *          Node to exclude
   */
  public List<String> getServers(final String iClusterName, final String iExclude) {
    synchronized (configuration) {
      final List<String> serverList = getClusterConfiguration(iClusterName).field("servers");
      if (serverList != null) {
        // COPY AND REMOVE ANY NEW_NODE_TAG
        List<String> filteredServerList = new ArrayList<String>(serverList.size());
        for (String s : serverList) {
          if (!s.equals(NEW_NODE_TAG) && (iExclude == null || !iExclude.equals(s)))
            filteredServerList.add(s);
        }
        return filteredServerList;
      }
      return Collections.EMPTY_LIST;
    }
  }

  /**
   * Returns the complete list of servers found in configuration.
   */
  public Set<String> getAllConfiguredServers() {
    synchronized (configuration) {
      final Set<String> servers = new HashSet<String>();

      for (String p : getClusterNames()) {
        final List<String> serverList = getClusterConfiguration(p).field("servers");
        if (serverList != null) {
          for (String s : serverList)
            if (!s.equals(NEW_NODE_TAG))
              servers.add(s);
        }
      }
      return servers;
    }
  }

  /**
   * Returns the set of clusters managed by a server.
   * 
   * @param iNodeName
   *          Server name
   */
  public Set<String> getClustersOnServer(final String iNodeName) {
    final Set<String> clusters = new HashSet<String>();
    for (String cl : getClusterNames()) {
      final List<String> servers = getServers(cl, null);
      if (servers.contains(iNodeName))
        clusters.add(cl);
    }
    return clusters;
  }

  /**
   * Returns the master server for the given cluster excluding the passed node. Master server is the first in server list.
   *
   * @param iClusterName
   *          Cluster name, or null for *
   */
  public String getMasterServer(final String iClusterName) {
    synchronized (configuration) {
      String master = null;

      final List<String> serverList = getClusterConfiguration(iClusterName).field("servers");
      if (serverList != null && !serverList.isEmpty()) {
        // RETURN THE FIRST ONE
        master = serverList.get(0);
        if (NEW_NODE_TAG.equals(master) && serverList.size() > 1)
          // DON'T RETURN <NEW_NODE>
          master = serverList.get(1);
      }

      return master;
    }
  }

  /**
   * Returns the server list for the requested cluster.
   * 
   * @param iClusterName
   *          Cluster name, or null for *
   */
  public List<String> getOriginalServers(final String iClusterName) {
    synchronized (configuration) {
      return getClusterConfiguration(iClusterName).field("servers");
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
   * Returns the default server role between MASTER (default) and REPLICA.
   */
  public ROLES getDefaultServerRole() {
    synchronized (configuration) {
      final ODocument servers = configuration.field("servers");
      if (servers == null)
        // DEFAULT: MASTER
        return ROLES.MASTER;

      final String role = servers.field("*");
      if (role == null)
        // DEFAULT: MASTER
        return ROLES.MASTER;

      return ROLES.valueOf(role.toUpperCase());
    }
  }

  /**
   * Returns the server role between MASTER (default) and REPLICA.
   */
  public ROLES getServerRole(final String iServerName) {
    synchronized (configuration) {
      final ODocument servers = configuration.field("servers");
      if (servers == null)
        // DEFAULT: MASTER
        return ROLES.MASTER;

      String role = servers.field(iServerName);
      if (role == null) {
        // DEFAULT: MASTER
        role = servers.field("*");
        if (role == null)
          // DEFAULT: MASTER
          return ROLES.MASTER;
      }

      return ROLES.valueOf(role.toUpperCase());
    }
  }

  /**
   * Get the document representing the cluster configuration.
   * 
   * @param iClusterName
   *          Cluster name, or null for *
   * @return Always a ODocument
   * @throws OConfigurationException
   *           in case "clusters" field is not found in configuration
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

      if (cfg == null)
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
        final List<String> partitions = getOriginalServers(clusterName);
        if (partitions != null) {
          final int newNodePos = partitions.indexOf(ODistributedConfiguration.NEW_NODE_TAG);
          if (newNodePos > -1 && !partitions.contains(iNode)) {
            partitions.add(newNodePos, iNode);
            changedPartitions.add(clusterName);
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

  /**
   * Sets the master server for the given cluster. Master server is the first in server list
   * 
   * @param iClusterName
   *          Cluster name or *. Doesn't accept null.
   */
  public void setMasterServer(final String iClusterName, final String iServerName) {
    if (iClusterName == null)
      throw new IllegalArgumentException("cluster name cannot be null");

    synchronized (configuration) {
      final ODocument clusters = configuration.field("clusters");
      ODocument cluster = clusters.field(iClusterName);

      if (cluster == null)
        // CREATE IT
        cluster = createCluster(iClusterName);

      List<String> serverList = getOriginalServers(iClusterName);
      if (serverList == null)
        serverList = initClusterServers(cluster);

      if (!serverList.isEmpty() && serverList.get(0).equals(iServerName))
        // ALREADY MASTER
        return;

      // REMOVE THE NODE IF ANY
      for (Iterator<String> it = serverList.iterator(); it.hasNext();) {
        if (it.next().equals(iServerName)) {
          it.remove();
          break;
        }
      }

      // ADD THE NODE AS FIRST OF THE LIST = MASTER
      serverList.add(0, iServerName);

      incrementVersion();
    }
  }

  public ODocument createCluster(final String iClusterName) {
    // CREATE IT
    final ODocument clusters = configuration.field("clusters");

    ODocument cluster = clusters.field(iClusterName);
    if (cluster != null)
      // ALREADY EXISTS
      return clusters;

    cluster = new ODocument();
    ODocumentInternal.addOwner(cluster, clusters);
    clusters.field(iClusterName, cluster, OType.EMBEDDED);

    initClusterServers(cluster);

    return cluster;
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
      }

      if (!changedPartitions.isEmpty()) {
        incrementVersion();
        return changedPartitions;
      }
    }
    return null;
  }

  public List<String> removeMasterServer(final String iServerName) {
    final List<String> changedPartitions = new ArrayList<String>();

    synchronized (configuration) {
      final ODocument clusters = configuration.field("clusters");
      for (String c : clusters.fieldNames()) {
        final List<String> serverList = getOriginalServers(c);
        if (serverList == null)
          continue;

        if (serverList.size() < 2)
          // CANNOT REMOVE IT BECAUSE IT'S THE ONLY AVAILABLE NODE
          continue;

        if (!serverList.get(0).equals(iServerName))
          // WASN'T MASTER
          continue;

        // PUT THE FIRST NODE AS LAST
        serverList.remove(0);
        serverList.add(iServerName);
        changedPartitions.add(c);
      }
    }

    if (!changedPartitions.isEmpty()) {
      incrementVersion();
      return changedPartitions;
    }

    return null;
  }

  protected List<String> initClusterServers(final ODocument cluster) {
    final ODocument any = getClusterConfiguration("*");

    // COPY THE SERVER LIST FROM "*"
    final List<String> anyServers = any.field("servers");
    final List<String> servers = new ArrayList<String>(anyServers);
    cluster.field("servers", servers);

    return servers;
  }

  protected void incrementVersion() {
    // INCREMENT VERSION
    Integer oldVersion = configuration.field("version");
    if (oldVersion == null)
      oldVersion = 0;
    configuration.field("version", oldVersion.intValue() + 1);
  }
}
