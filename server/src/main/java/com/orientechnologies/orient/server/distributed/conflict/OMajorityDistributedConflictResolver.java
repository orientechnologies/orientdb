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

package com.orientechnologies.orient.server.distributed.conflict;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.distributed.ODistributedConfiguration;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Conflict resolver implementation based on the majority of results. If there is no majority, no operation is executed.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OMajorityDistributedConflictResolver extends OAbstractDistributedConflictResolver {
  public static final String NAME = "majority";

  public OConflictResult onConflict(final String databaseName, final String clusterName, final ORecordId rid,
      final ODistributedServerManager dManager, final Map<Object, List<String>> candidates, final ODocument config) {

    final OConflictResult result = new OConflictResult();

    final Object bestResult = getBestResult(candidates, null);
    if (bestResult == null)
      return result;

    final ODistributedConfiguration dCfg = dManager.getDatabaseConfiguration(databaseName);
    final int writeQuorum = dCfg.getWriteQuorum(clusterName, dManager.getAvailableNodes(databaseName), dManager.getLocalNodeName());

    final int bestResultServerCount = candidates.get(bestResult).size();
    if (bestResultServerCount >= writeQuorum) {
      // BEST RESULT RESPECT THE QUORUM, IT'S DEFINITELY THE WINNER
      OLogManager.instance().debug(this,
          "Majority Conflict Resolver decided the value '%s' is the winner for record %s, because is major than the configured writeQuorum (%d). Servers ok=%s",
          bestResult, rid, writeQuorum, candidates.get(result.winner));
      result.winner = bestResult;
    } else {
      // FOUND IF THERE IS AT LEAST A MAJORITY
      final List<Object> exclude = new ArrayList<Object>();
      exclude.add(bestResult);

      final Object secondBestResult = getBestResult(candidates, exclude);
      if (bestResultServerCount > candidates.get(secondBestResult).size()) {
        OLogManager.instance().debug(this,
            "Majority Conflict Resolver decided the value '%s' is the winner for the record %s because it is the majority even if under the configured writeQuorum (%d). Servers ok=%s",
            bestResult, rid, writeQuorum, candidates.get(result.winner));

        result.winner = bestResult;
      } else {
        // NO MAJORITY: DON'T TAKE ANY ACTION
        OLogManager.instance().debug(this, "Majority Conflict Resolver could not find a winner for the record %s (candidates=%s)", rid,
            candidates);

        // COLLECT ALL THE RESULT == BEST RESULT
        result.candidates.put(bestResult, candidates.get(bestResult));
        result.candidates.put(secondBestResult, candidates.get(secondBestResult));
        exclude.add(secondBestResult);

        Object lastBestResult = getBestResult(candidates, exclude);
        int resultCount = candidates.get(secondBestResult).size();
        while (lastBestResult != null && resultCount == bestResultServerCount && exclude.size() < candidates.size()) {
          result.candidates.put(lastBestResult, candidates.get(lastBestResult));
          exclude.add(lastBestResult);
          lastBestResult = getBestResult(candidates, exclude);
          if (lastBestResult == null)
            break;
          resultCount = candidates.get(lastBestResult).size();
        }
      }
    }

    return result;
  }

  public String getName() {
    return NAME;
  }
}
