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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.impl.ODocument;

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

  public Object execute(final OIdentifiable iCurrentRecord, ODocument iCurrentResult, final Object[] iParameters, OCommandContext iContext) {
    // AGGREGATION MODE (STATEFULL)
    for (Object value : iParameters) {
      if (value != null) {
        if (context == null)
          context = new ArrayList<Object>();

        if (value instanceof Collection<?>)
          // INSERT EVERY SINGLE COLLECTION ITEM
          context.addAll((Collection<?>) value);
        else
          context.add(value);
      }
    }
    return prepareResult(context);
  }

  public String getSyntax() {
    return "Syntax error: list(<value>*)";
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

  @Override
  public Object mergeDistributedResult(List<Object> resultsToMerge) {
    final Map<Long, Collection<Object>> chunks = new HashMap<Long, Collection<Object>>();
    for (Object iParameter : resultsToMerge) {
      final Map<String, Object> container = (Map<String, Object>) ((Collection) iParameter).iterator().next();
      chunks.put((Long) container.get("node"), (Collection<Object>) container.get("context"));
    }
    final Collection<Object> result = new ArrayList<Object>();
    for (Collection<Object> chunk : chunks.values()) {
      result.addAll(chunk);
    }
    return result;
  }
}
