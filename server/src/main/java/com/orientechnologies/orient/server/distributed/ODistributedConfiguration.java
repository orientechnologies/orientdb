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
 */
public class ODistributedConfiguration {
  public static final String        NEW_NODE_TAG         = "<NEW_NODE>";
  public static final String        ALL_WILDCARD         = "*";

  public static final String        QUORUM_MAJORITY      = "majority";
  public static final String        QUORUM_ALL           = "all";
  public static final Integer       DEFAULT_READ_QUORUM  = 1;
  public static final String        DEFAULT_WRITE_QUORUM = QUORUM_MAJORITY;

  private static final List<String> DEFAULT_CLUSTER_NAME = Collections.singletonList(ALL_WILDCARD);

  private final ODocument           configuration;

  public enum ROLES {
    MASTER, REPLICA
  }

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
   * Returns the read quorum.
   *
   * @param iClusterName
   *          Cluster name, or null for *
   * @param iAvailableNodes
   *          Total node available
   */
  public int getReadQuorum(final String iClusterName, final int iAvailableNodes) {
    return getQuorum("readQuorum", iClusterName, iAvailableNodes, DEFAULT_READ_QUORUM);
  }

  /**
   * Returns the write quorum.
   *
   * @param iClusterName
   *          Cluster name, or null for *
   * @param iAvailableNodes
   *          Total node available
   */
  public int getWriteQuorum(final String iClusterName, final int iAvailableNodes) {
    return getQuorum("writeQuorum", iClusterName, iAvailableNodes, DEFAULT_WRITE_QUORUM);
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
   * Returns the clusters where a server is owner. This is used when a cluster must be selected: locality is always the best choice.
   *
   * @param iClusterNames
   *          Set of cluster names
   * @param iNode
   *          Node
   */
  public List<String> getOwnedClustersByServer(Collection<String> iClusterNames, final String iNode) {
    synchronized (configuration) {
      if (iClusterNames == null || iClusterNames.isEmpty())
        iClusterNames = DEFAULT_CLUSTER_NAME;

      final List<String> notDefinedClusters = new ArrayList<String>(5);
      final List<String> candidates = new ArrayList<String>(5);

      for (String p : iClusterNames) {
        final String masterServer = getClusterOwner(p);
        if (masterServer == null)
          notDefinedClusters.add(p);
        else if (iNode.equals(masterServer)) {
          // COLLECT AS CANDIDATE
          candidates.add(p);
        }
      }

      if (!candidates.isEmpty())
        // RETURN THE FIRST ONE
        return candidates;

      final String masterServer = getClusterOwner(ALL_WILDCARD);
      if (iNode.equals(masterServer))
        // CURRENT SERVER IS MASTER OF DEFAULT: RETURN ALL THE NON CONFIGURED CLUSTERS
        return notDefinedClusters;

      // NO MASTER FOUND, RETURN EMPTY LIST
      return candidates;
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
   * Returns true if the server has a cluster.
   * 
   * @param iServer
   *          Server name
   * @param iClusterName
   *          Cluster name
   */
  public boolean hasCluster(final String iServer, final String iClusterName) {
    synchronized (configuration) {
      final List<String> serverList = getClusterConfiguration(iClusterName).field("servers");
      if (serverList != null)
        return serverList.contains(iServer);
      return false;
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
   * Returns the set of clusters where server is the owner.
   *
   * @param iNodeName
   *          Server name
   */
  public Set<String> getClustersOwnedByServer(final String iNodeName) {
    final Set<String> clusters = new HashSet<String>();
    for (String cl : getClusterNames()) {
      if (iNodeName.equals(getClusterOwner(cl)))
        clusters.add(cl);
    }
    return clusters;
  }

  /**
   * Returns the owner server for the given cluster excluding the passed node. The Owner server is the first in server list.
   *
   * @param iClusterName
   *          Cluster name, or null for *
   */
  public String getClusterOwner(final String iClusterName) {
    synchronized (configuration) {
      String owner;

      final ODocument clusters = getConfiguredClusters();

      // GET THE CLUSTER CFG
      final ODocument cfg = clusters.field(iClusterName);

      if (cfg != null) {
        owner = cfg.field("owner");
        if (owner != null)
          return owner;

        final List<String> serverList = cfg.field("servers");
        if (serverList != null && !serverList.isEmpty()) {
          // RETURN THE FIRST ONE
          owner = serverList.get(0);
          if (NEW_NODE_TAG.equals(owner) && serverList.size() > 1)
            // DON'T RETURN <NEW_NODE>
            owner = serverList.get(1);
        }
      } else
        // RETURN THE OWNER OF *
        return getClusterOwner(ALL_WILDCARD);

      return owner;
    }
  }

  /**
   * Returns the static owner server for the given cluster.
   *
   * @param iClusterName
   *          Cluster name, or null for *
   */
  public String getConfiguredClusterOwner(final String iClusterName) {
    synchronized (configuration) {
      String owner = null;

      final ODocument clusters = getConfiguredClusters();

      // GET THE CLUSTER CFG
      final ODocument cfg = clusters.field(iClusterName);
      if (cfg != null)
        owner = cfg.field("owner");

      return owner;
    }
  }

  /**
   * Returns the server list for the requested cluster.
   *
   * @param iClusterName
   *          Cluster name, or null for *
   */
  public List<String> getServers(final String iClusterName) {
    synchronized (configuration) {
      final Collection<? extends String> list = (Collection<? extends String>) getClusterConfiguration(iClusterName)
          .field("servers");
      return list != null ? new ArrayList<String>(list) : null;
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

      final String role = servers.field(ALL_WILDCARD);
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
        role = servers.field(ALL_WILDCARD);
        if (role == null)
          // DEFAULT: MASTER
          return ROLES.MASTER;
      }

      return ROLES.valueOf(role.toUpperCase());
    }
  }

  public ODocument getDocument() {
    return configuration.copy();
  }

  /**
   * Adds a server in the configuration. It replaces all the tags &lt;NEW_NODE&gt; with the new server name<br>
   * NOTE: It must be executed in distributed database lock.
   * 
   * @param iNode
   *          Server name
   * @return
   */
  public List<String> addNewNodeInServerList(final String iNode) {
    synchronized (configuration) {
      final List<String> changedPartitions = new ArrayList<String>();
      // NOT FOUND: ADD THE NODE IN CONFIGURATION. LOOK FOR $newNode TAG
      for (String clusterName : getClusterNames()) {
        final List<String> partitions = getClusterConfiguration(clusterName).field("servers");
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
   * Sets the server as owner for the given cluster. The owner server is the first in server list.<br>
   * NOTE: It must be executed in distributed database lock.
   *
   * @param iClusterName
   *          Cluster name or *. Does not accept null.
   */
  public void setServerOwner(final String iClusterName, final String iServerName) {
    if (iClusterName == null)
      throw new IllegalArgumentException("cluster name cannot be null");

    synchronized (configuration) {
      final ODocument clusters = configuration.field("clusters");
      ODocument cluster = clusters.field(iClusterName);

      if (cluster == null)
        // CREATE IT
        cluster = createCluster(iClusterName);
      else {
        // CHECK IF THE OWNER IS ALREADY CONFIGURED
        final String owner = cluster.field("owner");
        if (owner != null && !iServerName.equalsIgnoreCase(owner))
          throw new ODistributedException("Cannot overwrite ownership of cluster '" + iClusterName + "' to the server '"
              + iServerName + "', because server '" + owner + "' was already configured as owner");
      }

      List<String> serverList = getClusterConfiguration(iClusterName).field("servers");
      if (serverList == null)
        serverList = initClusterServers(cluster);

      if (!serverList.isEmpty() && serverList.get(0).equals(iServerName))
        // ALREADY OWNER
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

  /**
   * Removes a server from the list.<br>
   * NOTE: It must be executed in distributed database lock.
   * 
   * @param iNode
   *          Server name
   * @return
   */
  public List<String> removeServer(final String iNode) {
    synchronized (configuration) {
      final List<String> changedPartitions = new ArrayList<String>();

      for (String clusterName : getClusterNames()) {
        final Collection<String> nodes = getClusterConfiguration(clusterName).field("servers");
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

  /**
   * Set a server offline. It assures the offline server is never on top of the list.<br>
   * NOTE: It must be executed in distributed database lock.
   * 
   * @param iNode
   *          Server name
   * @param newServerCoordinator
   *          New coordinator server name
   * @return
   */
  public List<String> setServerOffline(final String iNode, final String newServerCoordinator) {
    final List<String> changedPartitions = new ArrayList<String>();

    final String[] clusters = getClusterNames();
    synchronized (configuration) {
      for (String clusterName : clusters) {
        final List<String> nodes = getClusterConfiguration(clusterName).field("servers");
        if (nodes != null && nodes.size() > 1) {
          for (String node : nodes) {
            if (node.equals(iNode)) {
              // FOUND: PUT THE NODE AT THE END (BEFORE ANY TAG <NEW_NODE>)
              nodes.remove(node);

              final boolean newNodeRemoved = nodes.remove(NEW_NODE_TAG);

              nodes.add(node);

              if (newNodeRemoved)
                // REINSERT NEW NODE TAG AT THE END
                nodes.add(NEW_NODE_TAG);

              if (newServerCoordinator != null) {
                // ASSURE THE NEW COORDINATOR IS THE FIRST IN THE LIST
                if (nodes.remove(newServerCoordinator))
                  nodes.add(0, newServerCoordinator);
              }

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

  public int getVersion() {
    final Integer v = configuration.field("version");
    if (v == null)
      return 0;
    return v;
  }

  private List<String> initClusterServers(final ODocument cluster) {
    final ODocument any = getClusterConfiguration(ALL_WILDCARD);

    // COPY THE SERVER LIST FROM ALL_WILDCARD
    final List<String> anyServers = any.field("servers");
    final List<String> servers = new ArrayList<String>(anyServers);
    cluster.field("servers", servers);

    return servers;
  }

  private ODocument createCluster(final String iClusterName) {
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

  private void incrementVersion() {
    // INCREMENT VERSION
    Integer oldVersion = configuration.field("version");
    if (oldVersion == null)
      oldVersion = 0;
    configuration.field("version", oldVersion.intValue() + 1);
  }

  /**
   * Returns the read quorum.
   *
   * @param iClusterName
   *          Cluster name, or null for *
   * @param iAvailableNodes
   *          Total nodes available
   */
  private int getQuorum(final String quorumSetting, final String iClusterName, final int iAvailableNodes,
      final Object defaultValue) {
    synchronized (configuration) {
      Object value = getClusterConfiguration(iClusterName).field(quorumSetting);
      if (value == null) {
        value = configuration.field(quorumSetting);
        if (value == null) {
          OLogManager.instance().warn(this, "%s setting not found for cluster=%s in distributed-config.json", quorumSetting,
              iClusterName);
          value = defaultValue;
        }
      }

      if (value instanceof String) {
        if (value.toString().equalsIgnoreCase(ODistributedConfiguration.QUORUM_MAJORITY)) {
          value = iAvailableNodes / 2 + 1;
        } else if (value.toString().equalsIgnoreCase(ODistributedConfiguration.QUORUM_ALL))
          value = iAvailableNodes;
      }

      return (Integer) value;
    }
  }

  private ODocument getConfiguredClusters() {
    final ODocument clusters = configuration.field("clusters");
    if (clusters == null)
      throw new OConfigurationException("Cannot find 'clusters' in distributed database configuration");
    return clusters;
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
  private ODocument getClusterConfiguration(String iClusterName) {
    synchronized (configuration) {
      final ODocument clusters = getConfiguredClusters();

      if (iClusterName == null)
        iClusterName = ALL_WILDCARD;

      final ODocument cfg;
      if (!clusters.containsField(iClusterName))
        // NO CLUSTER IN CFG: GET THE DEFAULT ONE
        cfg = clusters.field(ALL_WILDCARD);
      else
        // GET THE CLUSTER CFG
        cfg = clusters.field(iClusterName);

      if (cfg == null)
        return new ODocument();

      return cfg;
    }
  }
}
