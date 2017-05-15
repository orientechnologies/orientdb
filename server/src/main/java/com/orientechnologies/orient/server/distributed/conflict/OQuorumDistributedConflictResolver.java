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

package com.orientechnologies.orient.server.distributed.conflict;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.server.distributed.ODistributedConfiguration;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;

import java.util.List;
import java.util.Map;

/**
 * Conflict resolver implementation based on the write quorum of results. If there is no quorum, no operation is executed.
 *
 * @author Luca Garulli
 */
public class OQuorumDistributedConflictResolver extends OAbstractDistributedConflictResolver {
  public static final String NAME = "quorum";

  @Override
  public OConflictResult onConflict(final String databaseName, final String clusterName, final ORecordId rid,
      final ODistributedServerManager dManager, final Map<Object, List<String>> candidates) {

    final OConflictResult result = new OConflictResult(candidates);

    final Object bestResult = getBestResult(candidates, null);
    if (bestResult == NOT_FOUND)
      return result;

    final ODistributedConfiguration dCfg = dManager.getDatabaseConfiguration(databaseName);
    final int writeQuorum = dCfg.getWriteQuorum(clusterName, dManager.getAvailableNodes(databaseName), dManager.getLocalNodeName());

    final int bestResultServerCount = candidates.get(bestResult).size();
    if (bestResultServerCount >= writeQuorum) {
      // BEST RESULT RESPECT THE QUORUM, IT'S DEFINITELY THE WINNER
      OLogManager.instance().debug(this,
          "Quorum Conflict Resolver decided the value '%s' is the winner for record %s, because satisfies the configured writeQuorum (%d). Servers ok=%s",
          bestResult, rid, writeQuorum, candidates.get(result.winner));
      result.winner = bestResult;
    }

    return result;
  }

  @Override
  public String getName() {
    return NAME;
  }
}
