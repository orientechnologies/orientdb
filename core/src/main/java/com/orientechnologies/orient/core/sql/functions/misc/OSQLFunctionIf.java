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
package com.orientechnologies.orient.core.sql.functions.misc;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionAbstract;

/**
 * Returns different values based on the condition. If it's true the first value is returned, otherwise the second one.
 * 
 * <p>
 * Syntax: <blockquote>
 * 
 * <pre>
 * if(&lt;field|value|expression&gt;, &lt;return_value_if_true&gt; [,&lt;return_value_if_false&gt;])
 * </pre>
 * 
 * </blockquote>
 * 
 * <p>
 * Examples: <blockquote>
 * 
 * <pre>
 * SELECT <b>if(rich, 'rich', 'poor')</b> FROM ...
 * <br/>
 * SELECT <b>if( eval( 'salary > 1000000' ), 'rich', 'poor')</b> FROM ...
 * </pre>
 * 
 * </blockquote>
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */

public class OSQLFunctionIf extends OSQLFunctionAbstract {

  public static final String NAME = "if";

  public OSQLFunctionIf() {
    super(NAME, 2, 3);
  }

  @Override
  public Object execute(final OIdentifiable iCurrentRecord, final ODocument iCurrentResult, final Object[] iFuncParams,
      final OCommandContext iContext) {

    boolean result;

    try {
      Object condition = iFuncParams[0];
      if (condition instanceof Boolean)
        result = (Boolean) condition;
      else if (condition instanceof String)
        result = Boolean.parseBoolean(condition.toString());
      else if (condition instanceof Number)
        result = ((Number) condition).intValue() > 0;
      else
        return null;

      return result ? iFuncParams[1] : iFuncParams[2];

    } catch (Exception e) {
      return null;
    }
  }

  @Override
  public String getSyntax() {
    return "Syntax error: if(<field|value|expression>, <return_value_if_true> [,<return_value_if_false>])";
  }
}
