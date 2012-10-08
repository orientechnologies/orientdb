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
import java.util.HashSet;
import java.util.Set;

import com.orientechnologies.orient.core.command.OCommandExecutor;
import com.orientechnologies.orient.core.db.record.OIdentifiable;

/**
 * This operator add an item in a set. The set doesn't accept duplicates, so adding multiple times the same value has no effect: the
 * value is contained only once.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OSQLFunctionSet extends OSQLFunctionMultiValueAbstract<Set<Object>> {
  public static final String NAME = "set";

  public OSQLFunctionSet() {
    super(NAME, 1, -1);
  }

  public Object execute(final OIdentifiable iCurrentRecord, final Object[] iParameters, OCommandExecutor iRequester) {
    // AGGREGATION MODE (STATEFULL)
    for (Object value : iParameters) {
      if (value != null) {
        if (context == null)
          context = new HashSet<Object>();

        if (value instanceof Collection<?>)
          // INSERT EVERY SINGLE COLLECTION ITEM
          context.addAll((Collection<?>) value);
        else
          context.add(value);
      }
    }
    return context;
  }

  public String getSyntax() {
    return "Syntax error: set(<value>*)";
  }

  public boolean aggregateResults(final Object[] configuredParameters) {
    return false;
  }

  @Override
  public Set<Object> getResult() {
    final Set<Object> res = (Set<Object>) context;
    context = null;
    return res;
  }
}
