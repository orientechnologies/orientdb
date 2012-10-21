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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;

/**
 * This operator can work as aggregate or inline. If only one argument is passed than aggregates, otherwise executes, and returns,
 * the DIFFERENCE between the collections received as parameters. Works also with no collection values.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OSQLFunctionDifference extends OSQLFunctionMultiValueAbstract<Set<Object>> {
  public static final String NAME = "difference";

  private Set<Object>        rejected;

  public OSQLFunctionDifference() {
    super(NAME, 1, -1);
  }

  public Object execute(OIdentifiable iCurrentRecord, final Object[] iParameters, OCommandContext iContext) {
    if (iParameters[0] == null)
      return null;

    Object value = iParameters[0];

    if (iParameters.length == 1) {
      // AGGREGATION MODE (STATEFULL)
      if (context == null) {
        context = new HashSet<Object>();
        rejected = new HashSet<Object>();
      }
      if (value instanceof Collection<?>) {
        addItemsToResult((Collection<Object>) value, context, rejected);
      } else {
        addItemToResult(value, context, rejected);
      }

      return null;
    } else {
      // IN-LINE MODE (STATELESS)
      final Set<Object> result = new HashSet<Object>((Collection<?>) value);
      final Set<Object> rejected = new HashSet<Object>();

      for (Object iParameter : iParameters) {
        if (iParameter instanceof Collection<?>) {
          addItemsToResult((Collection<Object>) value, context, rejected);
        } else {
          addItemToResult(value, context, rejected);
        }
      }

      return result;
    }
  }

  @Override
  public Set<Object> getResult() {
    if (returnDistributedResult()) {
      final Map<String, Object> doc = new HashMap<String, Object>();
      doc.put("result", context);
      doc.put("rejected", rejected);
      return Collections.<Object> singleton(doc);
    } else {
      return super.getResult();
    }
  }

  private static void addItemToResult(Object o, Set<Object> accepted, Set<Object> rejected) {
    if (!accepted.contains(o) && !rejected.contains(o)) {
      accepted.add(o);
    } else {
      accepted.remove(o);
      rejected.add(o);
    }
  }

  private static void addItemsToResult(Collection<Object> co, Set<Object> accepted, Set<Object> rejected) {
    for (Object o : co) {
      addItemToResult(o, accepted, rejected);
    }
  }

  public String getSyntax() {
    return "Syntax error: difference(<field>*)";
  }

  @Override
  public Object mergeDistributedResult(List<Object> resultsToMerge) {
    final Set<Object> result = new HashSet<Object>();
    final Set<Object> rejected = new HashSet<Object>();
    for (Object item : resultsToMerge) {
      rejected.addAll(unwrap(item, "rejected"));
    }
    for (Object item : resultsToMerge) {
      addItemsToResult(unwrap(item, "result"), result, rejected);
    }
    return result;
  }

  private Set<Object> unwrap(Object obj, String field) {
    final Set<Object> objAsSet = (Set<Object>) obj;
    final Map<String, Object> objAsMap = (Map<String, Object>) objAsSet.iterator().next();
    final Set<Object> objAsField = (Set<Object>) objAsMap.get(field);
    return objAsField;
  }
}
