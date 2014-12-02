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

package com.orientechnologies.orient.core.fetch;

import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OFetchPlan {
  static final String        ROOT_FETCH          = "*";

  final Map<String, Integer> fetchPlan           = new HashMap<String, Integer>();
  final Map<String, Integer> fetchPlanStartsWith = new HashMap<String, Integer>();

  public OFetchPlan(final String iFetchPlan) {
    fetchPlan.put(ROOT_FETCH, 0);

    if (iFetchPlan != null && !iFetchPlan.isEmpty()) {
      // CHECK IF THERE IS SOME FETCH-DEPTH
      final List<String> planParts = OStringSerializerHelper.split(iFetchPlan, ' ');
      if (!planParts.isEmpty()) {
        for (String planPart : planParts) {
          final List<String> parts = OStringSerializerHelper.split(planPart, ':');
          if (parts.size() != 2) {
            throw new IllegalArgumentException("Wrong fetch plan: " + planPart);
          }

          final String key = parts.get(0);

          if (key.length() > 1 && key.endsWith("*")) {
            fetchPlanStartsWith.put(key.substring(0, key.length() - 1), Integer.parseInt(parts.get(1)));
          } else {
              fetchPlan.put(key, Integer.parseInt(parts.get(1)));
          }
        }
      }
    }
  }

  public int getDepthLevel(final String iFieldPath) {
    Integer depthLevel = fetchPlan.get(ROOT_FETCH);
    for (String fieldFetchDefinition : fetchPlan.keySet()) {
      if (iFieldPath.equals(fieldFetchDefinition)) {
        // GET THE FETCH PLAN FOR THE GENERIC FIELD IF SPECIFIED
        return fetchPlan.get(fieldFetchDefinition);
      } else if (fieldFetchDefinition.startsWith(iFieldPath)) {
        // SETS THE FETCH LEVEL TO 1 (LOADS ALL DOCUMENT FIELDS)
        return 1;
      }
    }

    for (Map.Entry<String, Integer> entry : fetchPlanStartsWith.entrySet()) {
      if (iFieldPath.startsWith(entry.getKey())) {
        return entry.getValue();
      }
    }

    return depthLevel.intValue();
  }

  public boolean has(final String iFieldPath) {
    for (String fieldFetchDefinition : fetchPlan.keySet()) {
      if (iFieldPath.equals(fieldFetchDefinition)) {
        // GET THE FETCH PLAN FOR THE GENERIC FIELD IF SPECIFIED
        return true;
      } else if (fieldFetchDefinition.startsWith(iFieldPath)) {
        // SETS THE FETCH LEVEL TO 1 (LOADS ALL DOCUMENT FIELDS)
        return true;
      }
    }

    for (Map.Entry<String, Integer> entry : fetchPlanStartsWith.entrySet()) {
      if (iFieldPath.startsWith(entry.getKey())) {
        return true;
      }
    }
    return false;
  }
}
