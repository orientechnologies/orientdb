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

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;

import java.util.List;
import java.util.Map;

/**
 * Base interface to manage distributed conflicts. The onConflict() method is responsible to resolve the conflicts by providing the
 * winning result. If it's not able to do that, NULL is returned. You can configure resolver in chain, so for example you can have
 * the majority first, but in case ov even votes you can assign the winner to one data center or to a custom algorithm implemented
 * by the user. If the call chain returns NULL, no operation is done.
 *
 * @author Luca Garulli
 */
public interface ODistributedConflictResolver {
  Object NOT_FOUND = new Object();

  class OConflictResult {
    public Object winner = NOT_FOUND;
    public Map<Object, List<String>> candidates;

    public OConflictResult(final Map<Object, List<String>> candidates) {
      this.candidates = candidates;
    }
  }

  void configure(ODocument config);

  /**
   * Called on distributed conflict. It is responsible to resolve the conflicts by providing the winning result. If it's not able to
   * do that, NULL is returned.
   *
   * @param databaseName
   * @param clusterName
   * @param rid                 RecordID of the record in conflict
   * @param dManager            Current distributed manager instance
   * @param groupedServerValues All the values from the servers grouped by value. The key could also be an exception in case the
   *                            record was not found. @return The winning object
   *
   * @return The winning result object
   */
  OConflictResult onConflict(String databaseName, String clusterName, ORecordId rid, ODistributedServerManager dManager,
      Map<Object, List<String>> groupedServerValues);

  /**
   * Name to register in the factory.
   */
  String getName();
}
