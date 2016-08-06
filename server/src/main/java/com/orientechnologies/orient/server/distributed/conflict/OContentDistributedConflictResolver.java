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
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;

import java.util.*;

/**
 * Conflict resolver implementation based on the checking for the record content: if it is the same, the major version wins.
 *
 * @author Luca Garulli
 */
public class OContentDistributedConflictResolver extends OMajorityDistributedConflictResolver {
  public static final String NAME = "content";

  public OConflictResult onConflict(final String databaseName, final String clusterName, final ORecordId rid,
      final ODistributedServerManager dManager, final Map<Object, List<String>> candidates, final ODocument config) {

    final OConflictResult result = new OConflictResult();

    if (!candidates.isEmpty()) {

      // REGROUP THE CANDIDATES BY STRICT CONTENT ONLY (byte[])
      final Map<byte[], List<String>> candidatesGroupedByContent = new HashMap<byte[], List<String>>();
      for (Map.Entry<Object, List<String>> entry : candidates.entrySet()) {
        final Object key = entry.getKey();

        if (key instanceof ORawBuffer) {
          final byte[] content = ((ORawBuffer) key).buffer;

          boolean matched = false;
          for (Map.Entry<byte[], List<String>> matchEntry : candidatesGroupedByContent.entrySet()) {
            if (Arrays.equals(content, matchEntry.getKey())) {
              // MATCHED, ADD SERVERS TO THE SAME LIST (SAME CONTENT)
              matchEntry.getValue().addAll(entry.getValue());
              matched = true;
              break;
            }
          }

          if (!matched) {
            // NEW CONTENT, CREATE A NEW ENTRY
            candidatesGroupedByContent.put(content, entry.getValue());
          }
        }
      }

      if (!candidatesGroupedByContent.isEmpty()) {
        // DETERMINE THE WINNER BY MAJORITY OF THE SERVER WITH CONTENT
        int maxServerList = -1;
        for (Map.Entry<byte[], List<String>> matchEntry : candidatesGroupedByContent.entrySet()) {
          final List<String> servers = matchEntry.getValue();
          if (servers.size() > maxServerList) {
            // TEMPORARY WINNER
            maxServerList = servers.size();
          }
        }

        // COLLECT THE WINNERS THEN
        final List<byte[]> winners = new ArrayList<byte[]>();
        for (Map.Entry<byte[], List<String>> matchEntry : candidatesGroupedByContent.entrySet()) {
          final List<String> servers = matchEntry.getValue();
          if (servers.size() == maxServerList)
            // WINNER
            winners.add(matchEntry.getKey());
        }

        if (winners.size() == 1) {
          final byte[] winnerContent = winners.get(0);

          // GET THE HIGHEST VERSION FROM WINNERS
          int maxVersion = -1;
          for (Map.Entry<Object, List<String>> entry : candidates.entrySet()) {
            final Object key = entry.getKey();

            if (key instanceof ORawBuffer) {
              final byte[] content = ((ORawBuffer) key).buffer;
              if (Arrays.equals(winnerContent, content) && ((ORawBuffer) key).version > maxVersion) {
                maxVersion = ((ORawBuffer) key).version;
                result.winner = key;
              }
            }
          }

          OLogManager.instance().info(this,
              "Content Conflict Resolver decided the value '%s' is the winner for record %s, because the content is the majority. Assigning the highest version (%d)",
              result.winner, rid, maxVersion);
        } else {
          result.candidates = candidates;
          OLogManager.instance().info(this,
              "Content Conflict Resolver cannot decided the winner for record %s, because there is no majoriy in the content", rid);
        }
      }
    }

    return result;
  }

  public String getName() {
    return NAME;
  }
}
