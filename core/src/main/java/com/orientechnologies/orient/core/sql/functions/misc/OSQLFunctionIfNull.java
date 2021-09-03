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
package com.orientechnologies.orient.core.sql.functions.misc;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionAbstract;

/**
 * Returns the passed <code>field/value</code> (or optional parameter <code>return_value_if_not_null
 * </code>) if <code>field/value</code> is <b>not</b> null; otherwise it returns <code>
 * return_value_if_null</code>.
 *
 * <p>Syntax:
 *
 * <blockquote>
 *
 * <pre>
 * ifnull(&lt;field|value&gt;, &lt;return_value_if_null&gt; [,&lt;return_value_if_not_null&gt;])
 * </pre>
 *
 * </blockquote>
 *
 * <p>Examples:
 *
 * <blockquote>
 *
 * <pre>
 * SELECT <b>ifnull('a', 'b')</b> FROM ...
 *  -> 'a'
 *
 * SELECT <b>ifnull('a', 'b', 'c')</b> FROM ...
 *  -> 'c'
 *
 * SELECT <b>ifnull(null, 'b')</b> FROM ...
 *  -> 'b'
 *
 * SELECT <b>ifnull(null, 'b', 'c')</b> FROM ...
 *  -> 'b'
 * </pre>
 *
 * </blockquote>
 *
 * @author Mark Bigler
 */
public class OSQLFunctionIfNull extends OSQLFunctionAbstract {

  public static final String NAME = "ifnull";

  public OSQLFunctionIfNull() {
    super(NAME, 2, 3);
  }

  @Override
  public Object execute(
      Object iThis,
      final OIdentifiable iCurrentRecord,
      final Object iCurrentResult,
      final Object[] iParams,
      final OCommandContext iContext) {
    /*
     * iFuncParams [0] field/value to check for null [1] return value if [0] is null [2] optional return value if [0] is not null
     */
    if (iParams[0] != null) {
      if (iParams.length == 3) {
        return iParams[2];
      }
      return iParams[0];
    }
    return iParams[1];
  }

  @Override
  public String getSyntax() {
    return "Syntax error: ifnull(<field|value>, <return_value_if_null> [,<return_value_if_not_null>])";
  }
}
