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
 * Returns the first <code>field/value</code> not null parameter. if no <code>field/value</code> is
 * <b>not</b> null, returns null.
 *
 * <p>Syntax:
 *
 * <blockquote>
 *
 * <pre>
 * coalesce(&lt;field|value&gt;[,&lt;field|value&gt;]*)
 * </pre>
 *
 * </blockquote>
 *
 * <p>Examples:
 *
 * <blockquote>
 *
 * <pre>
 * SELECT <b>coalesce('a', 'b')</b> FROM ...
 *  -> 'a'
 *
 * SELECT <b>coalesce(null, 'b')</b> FROM ...
 *  -> 'b'
 *
 * SELECT <b>coalesce(null, null, 'c')</b> FROM ...
 *  -> 'c'
 *
 * SELECT <b>coalesce(null, null)</b> FROM ...
 *  -> null
 *
 * </pre>
 *
 * </blockquote>
 *
 * @author Claudio Tesoriero
 */
public class OSQLFunctionCoalesce extends OSQLFunctionAbstract {
  public static final String NAME = "coalesce";

  public OSQLFunctionCoalesce() {
    super(NAME, 1, 1000);
  }

  @Override
  public Object execute(
      Object iThis,
      OIdentifiable iCurrentRecord,
      Object iCurrentResult,
      final Object[] iParams,
      OCommandContext iContext) {
    int length = iParams.length;
    for (int i = 0; i < length; i++) {
      if (iParams[i] != null) return iParams[i];
    }
    return null;
  }

  @Override
  public String getSyntax() {
    return "Returns the first not-null parameter or null if all parameters are null. Syntax: coalesce(<field|value> [,<field|value>]*)";
  }
}
