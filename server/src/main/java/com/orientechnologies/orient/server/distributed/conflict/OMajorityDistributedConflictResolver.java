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

import com.orientechnologies.orient.core.id.ORecordId;
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

  @Override
  public OConflictResult onConflict(final String databaseName, final String clusterName, final ORecordId rid,
      final ODistributedServerManager dManager, final Map<Object, List<String>> candidates) {

    final OConflictResult result = new OConflictResult(candidates);

    final Object bestResult = getBestResult(candidates, null);
    if (bestResult == NOT_FOUND)
      return result;

    final int bestResultServerCount = candidates.get(bestResult).size();

    final List<Object> exclude = new ArrayList<Object>();
    exclude.add(bestResult);

    final Object nextBestResult = getBestResult(candidates, exclude);
    if (bestResult == NOT_FOUND)
      // FIRST RESULT GROUP IS THE ONLY ONE: SELECT IT AS WINNER
      result.winner = bestResult;
    else {
      int nextResultCount = candidates.get(nextBestResult).size();
      if (nextResultCount < bestResultServerCount)
        // FIRST RESULT GROUP IS BIGGER: SELECT IT AS WINNER
        result.winner = bestResult;
    }
    return result;
  }

  @Override
  public String getName() {
    return NAME;
  }
}
