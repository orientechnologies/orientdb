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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Conflict resolver implementation based on the checking for the record content: if it is the same, the major version wins.
 *
 * @author Luca Garulli
 */
public class OContentDistributedConflictResolver extends OAbstractDistributedConflictResolver {
  public static final String NAME = "content";

  @Override
  public OConflictResult onConflict(final String databaseName, final String clusterName, final ORecordId rid,
      final ODistributedServerManager dManager, final Map<Object, List<String>> candidates) {

    final OConflictResult result = new OConflictResult(candidates);

    if (!candidates.isEmpty()) {

      // REGROUP THE CANDIDATES BY STRICT CONTENT ONLY (byte[])
      final Map<ORawBuffer, List<String>> candidatesGroupedByContent = new HashMap<ORawBuffer, List<String>>();
      for (Map.Entry<Object, List<String>> entry : candidates.entrySet()) {
        final Object key = entry.getKey();

        if (key instanceof ORawBuffer) {
          boolean matched = false;
          for (Map.Entry<ORawBuffer, List<String>> matchEntry : candidatesGroupedByContent.entrySet()) {
            matched = compareRecords((ORawBuffer) key, matchEntry.getKey());
            if (matched) {
              // MATCHED, ADD SERVERS TO THE SAME LIST (SAME CONTENT)
              matchEntry.getValue().addAll(entry.getValue());
              break;
            }
          }

          if (!matched) {
            // NEW CONTENT, CREATE A NEW ENTRY
            candidatesGroupedByContent.put((ORawBuffer) key, entry.getValue());
          }
        }
      }

      if (!candidatesGroupedByContent.isEmpty()) {
        // DETERMINE THE WINNER BY MAJORITY OF THE SERVER WITH CONTENT
        int maxServerList = -1;
        for (Map.Entry<ORawBuffer, List<String>> matchEntry : candidatesGroupedByContent.entrySet()) {
          final List<String> servers = matchEntry.getValue();
          if (servers.size() > maxServerList) {
            // TEMPORARY WINNER
            maxServerList = servers.size();
          }
        }

        // COLLECT THE WINNER(S) THEN
        final List<ORawBuffer> winners = new ArrayList<ORawBuffer>();
        for (Map.Entry<ORawBuffer, List<String>> matchEntry : candidatesGroupedByContent.entrySet()) {
          final List<String> servers = matchEntry.getValue();
          if (servers.size() == maxServerList)
            // WINNER
            winners.add(matchEntry.getKey());
        }

        if (winners.size() == 1) {
          final ORawBuffer winnerContent = winners.get(0);

          // GET THE HIGHEST VERSION FROM WINNERS
          int maxVersion = -1;
          for (Map.Entry<Object, List<String>> entry : candidates.entrySet()) {
            final Object key = entry.getKey();

            if (key instanceof ORawBuffer) {
              if (compareRecords(winnerContent, (ORawBuffer) key) && ((ORawBuffer) key).version > maxVersion) {
                maxVersion = ((ORawBuffer) key).version;
                result.winner = key;
              }
            }
          }

          OLogManager.instance().debug(this,
              "Content Conflict Resolver decided the value '%s' is the winner for record %s, because the content is the majority. Assigning the highest version (%d)",
              result.winner, rid, maxVersion);
        } else {
          OLogManager.instance().debug(this,
              "Content Conflict Resolver cannot decide the winner for record %s, because there is no majority in the content", rid);
        }
      }
    }

    return result;
  }

  @Override
  public String getName() {
    return NAME;
  }
}
