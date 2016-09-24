package com.orientechnologies.agent.ha;

import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.server.distributed.ODistributedConfiguration;
import com.orientechnologies.orient.server.distributed.ODistributedRequest;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.impl.ODefaultDistributedStrategy;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * EE implementation for quorum management. It extends the CE by supporting the data-center concept.
 * 
 * @author Luca Garulli
 */
public class OEnterpriseDistributedStrategy extends ODefaultDistributedStrategy {
  public void validateConfiguration(ODistributedConfiguration cfg) {
  }

  /**
   * Returns only the subset of servers that are part of the local dc.
   */
  @Override
  public Set<String> getNodesConcurInQuorum(final ODistributedServerManager manager, final ODistributedConfiguration cfg,
      final ODistributedRequest request, final Collection<String> iNodes, final Object localResult) {

    final String localNode = manager.getLocalNodeName();

    final boolean localDataCenterWriteQuorum = cfg.isLocalDataCenterWriteQuorum();
    final OCommandDistributedReplicateRequest.QUORUM_TYPE quorum = request.getTask().getQuorumType();
    final String dc = cfg.getDataCenterOfServer(localNode);

    if (dc == null || !localDataCenterWriteQuorum || quorum == OCommandDistributedReplicateRequest.QUORUM_TYPE.ALL)
      // NO DC: DEFAULT CFG
      return super.getNodesConcurInQuorum(manager, cfg, request, iNodes, localResult);

    // DC CONFIGURATION
    final List<String> dcServers = cfg.getDataCenterServers(dc);

    final Set<String> nodesConcurToTheQuorum = new HashSet<String>();
    if (quorum == OCommandDistributedReplicateRequest.QUORUM_TYPE.WRITE) {
      // ONLY MASTER NODES CONCUR TO THE MINIMUM QUORUM
      for (String node : iNodes) {
        if (cfg.getServerRole(node) == ODistributedConfiguration.ROLES.MASTER) {
          if (!localDataCenterWriteQuorum || dcServers.contains(node))
            nodesConcurToTheQuorum.add(node);
        }
      }

      if (localResult != null && cfg.getServerRole(localNode) == ODistributedConfiguration.ROLES.MASTER) {
        if (!localDataCenterWriteQuorum || dcServers.contains(localNode))
          // INCLUDE LOCAL NODE TOO
          nodesConcurToTheQuorum.add(localNode);
      }

    } else {

      // ALL NODES IN THE DC CONCUR TO THE MINIMUM QUORUM
      for (String node : iNodes) {
        if (!localDataCenterWriteQuorum || dcServers.contains(node))
          nodesConcurToTheQuorum.add(node);
      }

      if (localResult != null && (!localDataCenterWriteQuorum || dcServers.contains(localNode)))
        // INCLUDE LOCAL NODE TOO
        nodesConcurToTheQuorum.add(localNode);
    }

    return nodesConcurToTheQuorum;
  }
}
