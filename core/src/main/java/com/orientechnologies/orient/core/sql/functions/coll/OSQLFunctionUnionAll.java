/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.core.sql.functions.coll;

import com.orientechnologies.common.collection.OMultiCollectionIterator;
import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItemVariable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

/**
 * This operator can work as aggregate or inline. If only one argument is passed than aggregates,
 * otherwise executes, and returns, a UNION of the collections received as parameters. Works also
 * with no collection values. Does not remove duplication from the result.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OSQLFunctionUnionAll extends OSQLFunctionMultiValueAbstract<Collection<Object>> {
  public static final String NAME = "unionAll";

  public OSQLFunctionUnionAll() {
    super(NAME, 1, -1);
  }

  public Object execute(
      final Object iThis,
      final OIdentifiable iCurrentRecord,
      final Object iCurrentResult,
      final Object[] iParams,
      OCommandContext iContext) {
    if (iParams.length == 1) {
      // AGGREGATION MODE (STATEFUL)
      Object value = iParams[0];
      if (value != null) {

        if (value instanceof OSQLFilterItemVariable)
          value =
              ((OSQLFilterItemVariable) value).getValue(iCurrentRecord, iCurrentResult, iContext);

        if (context == null) context = new ArrayList<Object>();

        OMultiValue.add(context, value);
      }

      return context;
    } else {
      // IN-LINE MODE (STATELESS)
      final OMultiCollectionIterator<OIdentifiable> result =
          new OMultiCollectionIterator<OIdentifiable>();
      for (Object value : iParams) {
        if (value != null) {
          if (value instanceof OSQLFilterItemVariable)
            value =
                ((OSQLFilterItemVariable) value).getValue(iCurrentRecord, iCurrentResult, iContext);

          result.add(value);
        }
      }

      return result;
    }
  }

  public String getSyntax() {
    return "unionAll(<field>*)";
  }

  @Override
  public Object mergeDistributedResult(List<Object> resultsToMerge) {
    final Collection<Object> result = new HashSet<Object>();
    for (Object iParameter : resultsToMerge) {
      @SuppressWarnings("unchecked")
      final Collection<Object> items = (Collection<Object>) iParameter;
      if (items != null) {
        result.addAll(items);
      }
    }
    return result;
  }
}
