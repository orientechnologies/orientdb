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
package com.orientechnologies.orient.server.distributed.impl;

import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClassImpl;
import com.orientechnologies.orient.server.distributed.ODistributedConfiguration;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog;
import com.orientechnologies.orient.server.distributed.OModifiableDistributedConfiguration;

import java.util.*;

import static java.util.Collections.EMPTY_LIST;

/**
 * Interface to manage balancing of cluster ownership.
 *
 * @author Luca Garulli (l.garulli--at--orientdb.com)
 */
public class ODefaultClusterOwnershipAssignmentStrategy implements OClusterOwnershipAssignmentStrategy {
  private final ODistributedAbstractPlugin manager;

  public ODefaultClusterOwnershipAssignmentStrategy(final ODistributedAbstractPlugin manager) {
    this.manager = manager;
  }

  @Override
  public List<String> assignClusterOwnershipOfClass(final ODatabaseInternal iDatabase,
      final OModifiableDistributedConfiguration cfg, final OClass iClass, final Set<String> availableNodes) {

    // FILTER OUT NON MASTER SERVER
    for (Iterator<String> it = availableNodes.iterator(); it.hasNext(); ) {
      final String node = it.next();
      if (cfg.getServerRole(node) != ODistributedConfiguration.ROLES.MASTER)
        it.remove();
    }

    if (availableNodes.isEmpty())
      // NO MASTER, AVOID REASSIGNMENT
      return EMPTY_LIST;

    if (!(iClass.getClusterSelection() instanceof OLocalClusterWrapperStrategy))
      ((OClassImpl) iClass).setClusterSelectionInternal(
          new OLocalClusterWrapperStrategy(manager, iDatabase.getName(), iClass, iClass.getClusterSelection()));

    if (iClass.isAbstract())
      return EMPTY_LIST;

    final int[] clusterIds = iClass.getClusterIds();
    final Set<String> clusterNames = new HashSet<String>(clusterIds.length);
    for (int clusterId : clusterIds) {
      final String clusterName = iDatabase.getClusterNameById(clusterId);
      if (clusterName != null)
        clusterNames.add(clusterName);
    }

    // RE-BALANCE THE CLUSTER BASED ON AN AVERAGE OF NUMBER OF NODES
    final Map<String, String> clusterToAssignOwnership = reassignClusters(cfg, availableNodes, clusterNames);

    // FOUND CLUSTERS PREVIOUSLY ASSIGNED TO THE LOCAL ONE: CHANGE ASSIGNMENT TO LOCAL NODE AGAIN
    for (Map.Entry<String, String> entry : clusterToAssignOwnership.entrySet()) {
      final String cluster = entry.getKey();
      final String node = entry.getValue();

      assignClusterOwnership(iDatabase, cfg, iClass, cluster, node);
    }

    Collection<String> allClusterNames = iDatabase.getClusterNames();

    final List<String> serversToCreateANewCluster = new ArrayList<String>();
    for (String server : availableNodes) {
      final List<String> ownedClusters = cfg.getOwnedClustersByServer(clusterNames, server);
      if (ownedClusters.isEmpty()) {
        // CREATE A NEW CLUSTER WHERE THE LOCAL NODE IS THE MASTER
        String newClusterName;
        for (int i = 0; ; ++i) {
          newClusterName = iClass.getName().toLowerCase() + "_" + i;
          if (!allClusterNames.contains(newClusterName))
            break;
        }

        serversToCreateANewCluster.add(newClusterName);
        assignClusterOwnership(iDatabase, cfg, iClass, newClusterName, server);
      }
    }
    return serversToCreateANewCluster;
  }

  protected Map<String, String> reassignClusters(final ODistributedConfiguration cfg, final Set<String> availableNodes,
      final Set<String> clusterNames) {
    final Set<String> allConfiguredNodes = cfg.getServers(clusterNames);

    final List<OPair<String, List<String>>> nodeOwners = new ArrayList<OPair<String, List<String>>>(allConfiguredNodes.size());
    for (String server : allConfiguredNodes) {
      final List<String> ownedClusters = cfg.getOwnedClustersByServer(clusterNames, server);

      // FILTER ALL THE CLUSTERS WITH A STATIC OWNER CFG
      for (Iterator<String> it = ownedClusters.iterator(); it.hasNext(); ) {
        final String cluster = it.next();
        if (cfg.getConfiguredClusterOwner(cluster) != null)
          it.remove();
      }

      nodeOwners.add(new OPair<String, List<String>>(server, ownedClusters));
    }

    // ORDER BY NODES OWNING THE MORE CLUSTERS
    Collections.sort(nodeOwners, new Comparator<OPair<String, List<String>>>() {
      @Override
      public int compare(OPair<String, List<String>> o1, OPair<String, List<String>> o2) {
        return o2.getValue().size() - o1.getValue().size();
      }
    });

    final Map<String, String> clusterToAssignOwnership = new HashMap<String, String>();

    final Set<String> clustersOfClassToReassign = new HashSet<String>();

    // REASSIGN ALL THE CLUSTERS OF THE NOT AVAILABLE SERVERS
    for (OPair<String, List<String>> owner : nodeOwners) {
      final String server = owner.getKey();
      final List<String> ownedClusters = owner.getValue();
      if (!availableNodes.contains(server))
        clustersOfClassToReassign.addAll(ownedClusters);
    }

    int currentServerIndex = 0;
    int clusterAssigned = 0;
    for (OPair<String, List<String>> owner : nodeOwners) {
      final String server = owner.getKey();
      final List<String> ownedClusters = owner.getValue();

      // AT EVERY ITERATION COMPUTE THE BEST NUMBER OF CLUSTER TO ASSIGN TO THE CURRENT NODE
      final int nodesLeft = availableNodes.size() - currentServerIndex;
      int targetClustersPerNode = nodesLeft < 1 ? 1 : (clusterNames.size() - clusterAssigned) / nodesLeft;
      if (targetClustersPerNode == 0 || (nodesLeft > 0 && (clusterNames.size() - clusterAssigned) % nodesLeft > 0))
        targetClustersPerNode++;

      if (ownedClusters.size() > targetClustersPerNode) {
        // REMOVE CLUSTERS IF THERE IS NO STATIC CFG OF THE OWNER
        while (ownedClusters.size() > targetClustersPerNode) {
          clustersOfClassToReassign.add(ownedClusters.remove(ownedClusters.size() - 1));
        }

      } else if (ownedClusters.size() < targetClustersPerNode) {
        // ADD CLUSTERS
        while (ownedClusters.size() < targetClustersPerNode && !clustersOfClassToReassign.isEmpty()) {
          // POP THE FIRST ITEM
          final Iterator<String> it = clustersOfClassToReassign.iterator();

          boolean reassigned = false;
          while (it.hasNext()) {
            final String cluster = it.next();

            final List<String> serverPerClusterList = cfg.getConfiguredServers(cluster);

            if (serverPerClusterList != null && serverPerClusterList.contains(server)) {
              it.remove();
              clusterToAssignOwnership.put(cluster, server);
              ownedClusters.add(cluster);
              reassigned = true;
              break;
            }
          }

          if (!reassigned)
            // CANNOT REASSIGN CURRENT CLUSTERS ON AVAILABLE NODES (CASE OF SHARDING)
            break;
        }
      }

      clusterAssigned += ownedClusters.size();
      currentServerIndex++;
    }
    return clusterToAssignOwnership;
  }

  private void assignClusterOwnership(final ODatabaseInternal iDatabase, final OModifiableDistributedConfiguration cfg,
      final OClass iClass, final String cluster, final String node) {
    ODistributedServerLog.info(this, manager.getLocalNodeName(), null, ODistributedServerLog.DIRECTION.NONE,
        "Class '%s': change mastership of cluster '%s' (id=%d) to node '%s'", iClass, cluster,
        iDatabase.getClusterIdByName(cluster), node);
    cfg.setServerOwner(cluster, node);
  }
}
