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
 * Conflict resolver implementation based on the majority of results. If there is no majority, no operation is executed.
 *
 * @author Luca Garulli
 */
public class OMajorityDistributedConflictResolver extends OAbstractDistributedConflictResolver {
  public static final String NAME = "majority";

  public Object onConflict(final String databaseName, final String clusterName, final ORecordId rid,
      final ODistributedServerManager dManager, final Map<Object, List<String>> groupedServerValues) {

    final Object bestResult = getBestResult(groupedServerValues, null);
    if (bestResult == null)
      return null;

    final ODistributedConfiguration dCfg = dManager.getDatabaseConfiguration(databaseName);
    final int writeQuorum = dCfg.getWriteQuorum(clusterName, dManager.getAvailableNodes(databaseName), dManager.getLocalNodeName());

    final int bestResultServerCount = groupedServerValues.get(bestResult).size();
    if (bestResultServerCount >= writeQuorum) {
      // BEST RESULT RESPECT THE QUORUM, IT'S DEFINITELY THE WINNER
      OLogManager.instance().info(this,
          "Majority Conflict Resolver decided the value '%s' is the winner for record %s, because is major than the configured writeQuorum (%d)",
          bestResult, rid, writeQuorum);
      return bestResult;
    }

    // FOUND IF THERE IS AT LEAST A MAJORITY
    final Object secondBestResult = getBestResult(groupedServerValues, bestResult);
    if (bestResultServerCount > groupedServerValues.get(secondBestResult).size()) {
      OLogManager.instance().info(this,
          "Majority Conflict Resolver decided the value '%s' is the winner for the record %s because it is the majority even if under the configured writeQuorum (%d)",
          bestResult, rid, writeQuorum);
      return bestResult;
    }

    // NO MAJORITY: DON'T TAKE ANY ACTION
    OLogManager.instance().info(this, "Majority Conflict Resolver could not find a winner for the record %s: %s", rid,
        groupedServerValues);
    return null;
  }

  public String getName() {
    return NAME;
  }
}
