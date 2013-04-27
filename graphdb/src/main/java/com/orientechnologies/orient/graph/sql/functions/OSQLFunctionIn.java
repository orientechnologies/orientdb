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
package com.orientechnologies.orient.graph.sql.functions;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.tinkerpop.blueprints.Direction;

/**
 * Hi-level function that return the incoming connections. If the current element is a vertex, then will be returned edges otherwise
 * vertices.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OSQLFunctionIn extends OSQLFunctionMove {
  public static final String NAME = "in";

  public OSQLFunctionIn() {
    super(NAME, 0, 1);
  }

  public Object execute(OIdentifiable iCurrentRecord, Object iCurrentResult, final Object[] iParameters,
      final OCommandContext iContext) {
    return executeSub(iCurrentRecord, iCurrentResult, iParameters, iContext, Direction.IN);
  }

  public String getSyntax() {
    return "Syntax error: in([<labels>])";
  }
}
