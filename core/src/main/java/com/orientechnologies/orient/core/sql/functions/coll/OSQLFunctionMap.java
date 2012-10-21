/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.sql.functions.coll;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;

/**
 * This operator add an entry in a map. The entry is composed by a key and a value.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OSQLFunctionMap extends OSQLFunctionMultiValueAbstract<Map<Object, Object>> {
  public static final String NAME = "map";

  public OSQLFunctionMap() {
    super(NAME, 1, 2);
  }

  @SuppressWarnings("unchecked")
  public Object execute(final OIdentifiable iCurrentRecord, final Object[] iParameters, OCommandContext iContext) {
    if (context == null)
      context = new HashMap<Object, Object>();

    if (iParameters.length == 1) {
      if (iParameters[0] instanceof Map<?, ?>)
        // INSERT EVERY SINGLE COLLECTION ITEM
        context.putAll((Map<Object, Object>) iParameters[0]);
      else
        throw new IllegalArgumentException("Map function: expected a map or two parameters as key, value");
    } else
      context.put(iParameters[0], iParameters[1]);
    return prepareResult(context);
  }

  public String getSyntax() {
    return "Syntax error: map(<map>|[<key>,<value>]*)";
  }

  public boolean aggregateResults(final Object[] configuredParameters) {
    return false;
  }

  @Override
  public Map<Object, Object> getResult() {
    final Map<Object, Object> res = context;
    context = null;
    return prepareResult(res);
  }

  protected Map<Object, Object> prepareResult(Map<Object, Object> res) {
    if (returnDistributedResult()) {
      final Map<String, Object> doc = new HashMap<String, Object>();
      doc.put("node", getDistributedStorageId());
      doc.put("context", res);
      return Collections.<Object, Object> singletonMap("doc", doc);
    } else {
      return res;
    }
  }

  @Override
  public Object mergeDistributedResult(List<Object> resultsToMerge) {
    final Map<Long, Map<Object, Object>> chunks = new HashMap<Long, Map<Object, Object>>();
    for (Object iParameter : resultsToMerge) {
      final Map<String, Object> container = (Map<String, Object>) ((Map<Object, Object>) iParameter).get("doc");
      chunks.put((Long) container.get("node"), (Map<Object, Object>) container.get("context"));
    }
    final Map<Object, Object> result = new HashMap<Object, Object>();
    for (Map<Object, Object> chunk : chunks.values()) {
      result.putAll(chunk);
    }
    return result;
  }
}
