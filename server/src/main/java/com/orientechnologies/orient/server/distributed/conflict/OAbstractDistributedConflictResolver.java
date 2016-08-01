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

import java.util.List;
import java.util.Map;

/**
 * Abstract class to simplify implementation of distributed conflict resolvers.
 *
 * @author Luca Garulli
 */
public abstract class OAbstractDistributedConflictResolver implements ODistributedConflictResolver {
  protected Object getBestResult(final Map<Object, List<String>> groupedResult, final Object exclude) {
    Object bestResult = null;
    int max = -1;
    for (Map.Entry<Object, List<String>> entry : groupedResult.entrySet()) {
      if (exclude != null && exclude.equals(entry.getKey()))
        // SKIP IT
        continue;

      if (entry.getValue().size() > max) {
        bestResult = entry.getKey();
        max = entry.getValue().size();
      }
    }
    return bestResult;
  }
}
