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

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;

/**
 * Returns a traversed element from the stack. Use it with SQL traverse only.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OSQLFunctionTraversedEdge extends OSQLFunctionTraversedElement {
  public static final String NAME = "traversedEdge";

  public OSQLFunctionTraversedEdge() {
    super(NAME);
  }

  public Object execute(
      Object iThis,
      final OIdentifiable iCurrentRecord,
      Object iCurrentResult,
      final Object[] iParams,
      final OCommandContext iContext) {
    return evaluate(iThis, iParams, iContext, "E");
  }
}
