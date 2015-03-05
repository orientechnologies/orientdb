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
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItemVariable;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * This operator can work as aggregate or inline. If only one argument is passed than aggregates, otherwise executes, and returns,
 * the INTERSECTION of the collections received as parameters.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OSQLFunctionIntersect extends OSQLFunctionMultiValueAbstract<Set<Object>> {
  public static final String NAME = "intersect";

  public OSQLFunctionIntersect() {
    super(NAME, 1, -1);
  }

  public Object execute(Object iThis, final OIdentifiable iCurrentRecord, Object iCurrentResult, final Object[] iParams,
      OCommandContext iContext) {
    Object value = iParams[0];

    if (value instanceof OSQLFilterItemVariable) {
        value = ((OSQLFilterItemVariable) value).getValue(iCurrentRecord, iCurrentResult, iContext);
    }

    if (value == null) {
        return Collections.emptySet();
    }

    if (!(value instanceof Collection<?>)) {
        value = Arrays.asList(value);
    }

    final Collection<?> coll = (Collection<?>) value;

    if (iParams.length == 1) {
      // AGGREGATION MODE (STATEFUL)
      if (context == null) {
        // ADD ALL THE ITEMS OF THE FIRST COLLECTION
        context = new HashSet<Object>(coll);
      } else {
        // INTERSECT IT AGAINST THE CURRENT COLLECTION
        context.retainAll(coll);
      }
      return null;
    } else {
      // IN-LINE MODE (STATELESS)
      final HashSet<Object> result = new HashSet<Object>(coll);

      for (int i = 1; i < iParams.length; ++i) {
        value = iParams[i];

        if (value instanceof OSQLFilterItemVariable) {
            value = ((OSQLFilterItemVariable) value).getValue(iCurrentRecord, iCurrentResult, iContext);
        }

        if (value != null) {
          if (!(value instanceof Collection<?>)) {
              // CONVERT IT INTO A COLLECTION
              value = Arrays.asList(value);
          }

          result.retainAll((Collection<?>) value);
        } else {
            result.clear();
        }
      }

      return result;
    }
  }

  public String getSyntax() {
    return "intersect(<field>*)";
  }

  @SuppressWarnings("unchecked")
  @Override
  public Object mergeDistributedResult(List<Object> resultsToMerge) {
    final Collection<Object> result = new HashSet<Object>();
    if (!resultsToMerge.isEmpty()) {
      final Collection<Object> items = (Collection<Object>) resultsToMerge.get(0);
      if (items != null) {
        result.addAll(items);
      }
    }
    for (int i = 1; i < resultsToMerge.size(); i++) {
      final Collection<Object> items = (Collection<Object>) resultsToMerge.get(i);
      if (items != null) {
        result.retainAll(items);
      }
    }
    return result;
  }
}
