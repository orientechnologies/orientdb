/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.server.distributed.impl;

import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.server.distributed.ODistributedConfiguration;
import com.orientechnologies.orient.server.distributed.ODistributedRequest;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.ODistributedStrategy;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Default quorum factory. This is the default for the CE. The EE supports also the concept of
 * data-centers.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class ODefaultDistributedStrategy implements ODistributedStrategy {
  @Override
  public void validateConfiguration(ODistributedConfiguration cfg) {
    if (cfg.hasDataCenterConfiguration())
      throw new OConfigurationException(
          "Data center configuration is supported only in OrientDB Enterprise Edition");

    if (cfg.isLocalDataCenterWriteQuorum())
      throw new OConfigurationException(
          "Quorum of type '"
              + ODistributedConfiguration.QUORUM_LOCAL_DC
              + "' is supported only in OrientDB Enterprise Edition");
  }

  @Override
  public Set<String> getNodesConcurInQuorum(
      final ODistributedServerManager manager,
      final ODistributedConfiguration cfg,
      final ODistributedRequest request,
      final Collection<String> iNodes,
      final Object localResult) {

    final Set<String> nodesConcurToTheQuorum = new HashSet<String>();
    if (request.getTask().getQuorumType() == OCommandDistributedReplicateRequest.QUORUM_TYPE.WRITE
        || request.getTask().getQuorumType()
            == OCommandDistributedReplicateRequest.QUORUM_TYPE.WRITE_ALL_MASTERS) {
      // ONLY MASTER NODES CONCUR TO THE MINIMUM QUORUM
      for (String node : iNodes) {
        if (cfg.getServerRole(node) == ODistributedConfiguration.ROLES.MASTER)
          nodesConcurToTheQuorum.add(node);
      }

      if (localResult != null
          && cfg.getServerRole(manager.getLocalNodeName())
              == ODistributedConfiguration.ROLES.MASTER)
        // INCLUDE LOCAL NODE TOO
        nodesConcurToTheQuorum.add(manager.getLocalNodeName());

    } else {

      // ALL NODES CONCUR TO THE MINIMUM QUORUM
      nodesConcurToTheQuorum.addAll(iNodes);

      if (localResult != null)
        // INCLUDE LOCAL NODE TOO
        nodesConcurToTheQuorum.add(manager.getLocalNodeName());
    }

    return nodesConcurToTheQuorum;
  }
}
