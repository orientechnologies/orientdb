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

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * This operator add an item in a list. The list accepts duplicates.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OSQLFunctionList extends OSQLFunctionMultiValueAbstract<List<Object>> {
  public static final String NAME = "list";

  public OSQLFunctionList() {
    super(NAME, 1, -1);
  }

  public Object execute(Object iThis, final OIdentifiable iCurrentRecord, Object iCurrentResult, final Object[] iParams,
      OCommandContext iContext) {
    if (iParams.length > 1)
      // IN LINE MODE
      context = new ArrayList<Object>();

    for (Object value : iParams) {
      if (value != null) {
        if (iParams.length == 1 && context == null)
          // AGGREGATION MODE (STATEFULL)
          context = new ArrayList<Object>();

        if (value instanceof Map)
          context.add(value);
        else
          OMultiValue.add(context, value);
      }
    }
    return prepareResult(context);
  }

  public String getSyntax() {
    return "list(<value>*)";
  }

  public boolean aggregateResults(final Object[] configuredParameters) {
    return false;
  }

  @Override
  public List<Object> getResult() {
    final List<Object> res = context;
    context = null;
    return prepareResult(res);
  }

  @SuppressWarnings("unchecked")
  @Override
  public Object mergeDistributedResult(List<Object> resultsToMerge) {
    if (returnDistributedResult()) {
      final Collection<Object> result = new HashSet<Object>();
      for (Object iParameter : resultsToMerge) {
        final Map<String, Object> container = (Map<String, Object>) ((Collection<?>) iParameter).iterator().next();
        result.addAll((Collection<?>) container.get("context"));
      }
      return result;
    }

    return resultsToMerge.get(0);
  }

  protected List<Object> prepareResult(List<Object> res) {
    if (returnDistributedResult()) {
      final Map<String, Object> doc = new HashMap<String, Object>();
      doc.put("node", getDistributedStorageId());
      doc.put("context", res);
      return Collections.<Object> singletonList(doc);
    } else {
      return res;
    }
  }
}
