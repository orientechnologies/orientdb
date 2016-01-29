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

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * This operator can work inline. Returns the DIFFERENCE between the collections received as parameters. Works also with no
 * collection values.
 *
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 */
public class OSQLFunctionDifference extends OSQLFunctionMultiValueAbstract<Set<Object>> {
  public static final String NAME = "difference";

  public OSQLFunctionDifference() {
    super(NAME, 2, -1);
  }

  @SuppressWarnings("unchecked")
  public Object execute(Object iThis, OIdentifiable iCurrentRecord, Object iCurrentResult, final Object[] iParams,
      OCommandContext iContext) {
    if (iParams[0] == null)
      return null;

    // IN-LINE MODE (STATELESS)
    final Set<Object> result = new HashSet<Object>();

    boolean first = true;
    for (Object iParameter : iParams) {
      if (first) {
        if (iParameter instanceof Collection<?>) {
          result.addAll((Collection<Object>) iParameter);
        } else {
          result.add(iParameter);
        }
      } else {
        if (iParameter instanceof Collection<?>) {
          result.removeAll((Collection<Object>) iParameter);
        } else {
          result.remove(iParameter);
        }
      }

      first = false;
    }

    return result;

  }

  public String getSyntax() {
    return "difference(<field>, <field> [, <field]*)";
  }
}
