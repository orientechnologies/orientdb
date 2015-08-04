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
package com.orientechnologies.orient.core.sql.functions.coll;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This operator add an entry in a map. The entry is composed by a key and a value.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OSQLFunctionMap extends OSQLFunctionMultiValueAbstract<Map<Object, Object>> {
  public static final String NAME = "map";

  public OSQLFunctionMap() {
    super(NAME, 1, -1);
  }

  @SuppressWarnings("unchecked")
  public Object execute(Object iThis, final OIdentifiable iCurrentRecord, Object iCurrentResult, final Object[] iParams,
      OCommandContext iContext) {

    if (iParams.length > 2)
      // IN LINE MODE
      context = new HashMap<Object, Object>();

    if (iParams.length == 1) {
      if (iParams[0] == null)
        return null;

      if (iParams[0] instanceof Map<?, ?>) {
        if (context == null)
          // AGGREGATION MODE (STATEFULL)
          context = new HashMap<Object, Object>();

        // INSERT EVERY SINGLE COLLECTION ITEM
        context.putAll((Map<Object, Object>) iParams[0]);
      } else
        throw new IllegalArgumentException("Map function: expected a map or pairs of parameters as key, value");
    } else if (iParams.length % 2 != 0)
      throw new IllegalArgumentException("Map function: expected a map or pairs of parameters as key, value");
    else
      for (int i = 0; i < iParams.length; i += 2) {
        final Object key = iParams[i];
        final Object value = iParams[i + 1];

        if (value != null) {
          if (iParams.length <= 2 && context == null)
            // AGGREGATION MODE (STATEFULL)
            context = new HashMap<Object, Object>();

          context.put(key, value);
        }
      }

    return prepareResult(context);
  }

  public String getSyntax() {
    return "map(<map>|[<key>,<value>]*)";
  }

  public boolean aggregateResults(final Object[] configuredParameters) {
    return configuredParameters.length <= 2;
  }

  @Override
  public Map<Object, Object> getResult() {
    final Map<Object, Object> res = context;
    context = null;
    return prepareResult(res);
  }

  protected Map<Object, Object> prepareResult(final Map<Object, Object> res) {
    if (returnDistributedResult()) {
      final Map<String, Object> doc = new HashMap<String, Object>();
      doc.put("node", getDistributedStorageId());
      doc.put("context", res);
      return Collections.<Object, Object> singletonMap("doc", doc);
    } else {
      return res;
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public Object mergeDistributedResult(final List<Object> resultsToMerge) {
    if (returnDistributedResult()) {
      final Map<Object, Object> result = new HashMap<Object, Object>();
      for (Object iParameter : resultsToMerge) {
        final Map<String, Object> container = (Map<String, Object>) ((Map<Object, Object>) iParameter).get("doc");
        result.putAll((Map<Object, Object>) container.get("context"));
      }
      return result;
    }

    return resultsToMerge.get(0);
  }
}
