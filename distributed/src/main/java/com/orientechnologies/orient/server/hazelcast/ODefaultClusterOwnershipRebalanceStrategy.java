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
package com.orientechnologies.orient.server.hazelcast;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.OScenarioThreadLocal;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClassImpl;
import com.orientechnologies.orient.core.sql.OCommandSQLParsingException;
import com.orientechnologies.orient.server.distributed.ODistributedConfiguration;
import com.orientechnologies.orient.server.distributed.ODistributedException;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog;
import com.orientechnologies.orient.server.distributed.OLocalClusterStrategy;

import java.util.*;

/**
 * Interface to manage balancing of cluster ownership.
 *
 * @author Luca Garulli (l.garulli--at--orientdb.com)
 */
public class ODefaultClusterOwnershipRebalanceStrategy implements OClusterOwnershipRebalanceStrategy {
  private final OHazelcastPlugin manager;

  public ODefaultClusterOwnershipRebalanceStrategy(final OHazelcastPlugin manager) {
    this.manager = manager;
  }

  @Override
  public boolean rebalanceClusterOwnershipOfClass(final ODatabaseInternal iDatabase, final ODistributedConfiguration cfg,
      final OClass iClass, final Set<String> availableNodes, final Set<String> clustersToReassign) {

    if (availableNodes.isEmpty())
      return false;

    if (!(iClass.getClusterSelection() instanceof OLocalClusterStrategy))
      ((OClassImpl) iClass).setClusterSelectionInternal(new OLocalClusterStrategy(manager, iDatabase.getName(), iClass));

    if (iClass.isAbstract())
      return false;

    final Set<String> clustersOfClassToReassign = new HashSet<String>();

    final int[] clusterIds = iClass.getClusterIds();
    final Set<String> clusterNames = new HashSet<String>(clusterIds.length);
    for (int clusterId : clusterIds) {
      final String clusterName = iDatabase.getClusterNameById(clusterId);
      clusterNames.add(clusterName);
      if (clustersToReassign.remove(clusterName))
        // MOVE THE CLUSTER TO THE REASSIGNMENT FOR THIS CLASS
        clustersOfClassToReassign.add(clusterName);
    }

    final Map<String, String> clusterToAssignOwnership = new HashMap<String, String>();

    // RE-BALANCE THE CLUSTER BASED ON AN AVERAGE OF NUMBER OF NODES
    int targetClustersPerNode = clusterNames.size() / availableNodes.size();
    if (targetClustersPerNode == 0 || clusterNames.size() % availableNodes.size() > 0)
      targetClustersPerNode += 1;

    // ORDER OWNERSHIP BY SIZE
    final List<OPair<String, List<String>>> nodeOwners = new ArrayList<OPair<String, List<String>>>(availableNodes.size());
    for (String server : availableNodes) {
      final List<String> ownedClusters = cfg.getOwnedClusters(clusterNames, server);
      nodeOwners.add(new OPair<String, List<String>>(server, ownedClusters));
    }

    Collections.sort(nodeOwners, new Comparator<OPair<String, List<String>>>() {
      @Override
      public int compare(OPair<String, List<String>> o1, OPair<String, List<String>> o2) {
        return o2.getValue().size() - o1.getValue().size();
      }
    });

    for (OPair<String, List<String>> owner : nodeOwners) {
      final String server = owner.getKey();
      final List<String> ownedClusters = owner.getValue();

      if (ownedClusters.size() > targetClustersPerNode) {
        // REMOVE CLUSTERS
        while (ownedClusters.size() > targetClustersPerNode) {
          clustersOfClassToReassign.add(ownedClusters.remove(ownedClusters.size() - 1));
        }
      } else if (ownedClusters.size() < targetClustersPerNode) {
        // ADD CLUSTERS
        while (ownedClusters.size() < targetClustersPerNode && !clustersOfClassToReassign.isEmpty()) {
          // POP THE FIRST ITEM
          final Iterator<String> it = clustersOfClassToReassign.iterator();
          final String cluster = it.next();
          it.remove();

          clusterToAssignOwnership.put(cluster, server);
          ownedClusters.add(cluster);
        }
      }
    }

    boolean cfgChanged = !clusterToAssignOwnership.isEmpty();

    // FOUND CLUSTERS PREVIOUSLY ASSIGNED TO THE LOCAL ONE: CHANGE ASSIGNMENT TO LOCAL NODE AGAIN
    for (Map.Entry<String, String> entry : clusterToAssignOwnership.entrySet()) {
      final String cluster = entry.getKey();
      final String node = entry.getValue();

      assignClusterOwnership(iDatabase, cfg, iClass, cluster, node);
    }

    // MOVE THE UNASSIGNED CLUSTERS TO THE ORIGINAL SET TO BE REALLOCATED FROM THE EXTERNAL
    clustersToReassign.addAll(clustersOfClassToReassign);

    final Collection<String> allClusterNames = iDatabase.getClusterNames();

    // CHECK OWNER AFTER RE-BALANCE AND CREATE NEW CLUSTERS IF NEEDED
    for (String server : availableNodes) {
      final List<String> ownedClusters = cfg.getOwnedClusters(clusterNames, server);
      if (ownedClusters.isEmpty()) {
        // CREATE A NEW CLUSTER WHERE LOCAL NODE IS THE MASTER
        String newClusterName;
        for (int i = 0;; ++i) {
          newClusterName = iClass.getName().toLowerCase() + "_" + i;
          if (!allClusterNames.contains(newClusterName))
            break;
        }

        ODistributedServerLog.info(this, manager.getLocalNodeName(), null, ODistributedServerLog.DIRECTION.NONE,
            "class '%s', creation of new local cluster '%s' (id=%d)", iClass, newClusterName,
            iDatabase.getClusterIdByName(newClusterName));

        final OScenarioThreadLocal.RUN_MODE currentDistributedMode = OScenarioThreadLocal.INSTANCE.get();
        if (currentDistributedMode != OScenarioThreadLocal.RUN_MODE.DEFAULT)
          OScenarioThreadLocal.INSTANCE.set(OScenarioThreadLocal.RUN_MODE.DEFAULT);

        try {
          iClass.addCluster(newClusterName);
        } catch (OCommandSQLParsingException e) {
          if (!e.getMessage().endsWith("already exists"))
            throw e;
        } catch (Exception e) {
          ODistributedServerLog.error(this, manager.getLocalNodeName(), null, ODistributedServerLog.DIRECTION.NONE,
              "error on creating cluster '%s' in class '%s': ", newClusterName, iClass, e);
          throw OException.wrapException(
              new ODistributedException("Error on creating cluster '" + newClusterName + "' in class '" + iClass + "'"), e);
        } finally {

          if (currentDistributedMode != OScenarioThreadLocal.RUN_MODE.DEFAULT)
            // RESTORE PREVIOUS MODE
            OScenarioThreadLocal.INSTANCE.set(OScenarioThreadLocal.RUN_MODE.RUNNING_DISTRIBUTED);
        }

        assignClusterOwnership(iDatabase, cfg, iClass, newClusterName, server);
        cfgChanged = true;
      }
    }

    return cfgChanged;
  }

  private void assignClusterOwnership(final ODatabaseInternal iDatabase, final ODistributedConfiguration cfg, final OClass iClass,
      final String cluster, final String node) {
    ODistributedServerLog.info(this, node, null, ODistributedServerLog.DIRECTION.NONE,
        "Class '%s': change mastership of cluster '%s' (id=%d) to node '%s'", iClass, cluster,
        iDatabase.getClusterIdByName(cluster), node);
    cfg.setServerOwner(cluster, node);
  }
}
