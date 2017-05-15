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
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Conflict resolver implementation based on the record version: the highest version wins.
 *
 * @author Luca Garulli
 */
public class OVersionDistributedConflictResolver extends OAbstractDistributedConflictResolver {
  public static final String NAME = "version";

  @Override
  public OConflictResult onConflict(final String databaseName, final String clusterName, final ORecordId rid,
      final ODistributedServerManager dManager, final Map<Object, List<String>> candidates) {

    final OConflictResult result = new OConflictResult(candidates);

    if (!candidates.isEmpty()) {

      int maxVersion = -1;

      // DETERMINE THE MAXIMUM VERSION FIRST
      for (Object v : candidates.keySet()) {
        if (v instanceof ORawBuffer) {
          if (((ORawBuffer) v).version > maxVersion) {
            maxVersion = ((ORawBuffer) v).version;
          }
        }
      }

      // COLLECT THE WINNERS THEN
      final List<Object> winners = new ArrayList<Object>();
      for (Object v : candidates.keySet()) {
        if (v instanceof ORawBuffer) {
          if (((ORawBuffer) v).version == maxVersion) {
            winners.add(v);
          }
        }
      }

      if (winners.size() == 1) {
        result.winner = winners.get(0);
        OLogManager.instance().debug(this,
            "Version Conflict Resolver decided the value '%s' is the winner for record %s, because its version (%d) is the highest. Servers ok=%s",
            result.winner, rid, maxVersion, candidates.get(result.winner));
      } else {
        OLogManager.instance().debug(this,
            "Version Conflict Resolver cannot decide the winner for record %s, because %d records have the highest version %d", rid,
            winners.size(), maxVersion);
      }
    }

    return result;
  }

  @Override
  public String getName() {
    return NAME;
  }
}
