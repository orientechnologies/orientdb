/*
 * Copyright 2013 Orient Technologies.
 * Copyright 2013 Geomatys.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.sql.functions.text;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionAbstract;

/**
 * Appends strings. Acts as a concatenation.
 * 
 * @author Johann Sorel (Geomatys)
 * @author Luca Garulli
 */
public class OSQLFunctionAppend extends OSQLFunctionAbstract {

  public static final String NAME = "append";

  public OSQLFunctionAppend() {
    super(NAME, 2, -1);
  }

  @Override
  public Object execute(OIdentifiable iCurrentRecord, Object iCurrentResult, Object[] iFuncParams, OCommandContext iContext) {
    if (iFuncParams[0] == null)
      return null;

    final StringBuilder buffer = new StringBuilder(iFuncParams[0].toString());
    for (int i = 1; i < iFuncParams.length; ++i)
      if (iFuncParams[i] != null)
        buffer.append(iFuncParams[i]);

    return buffer.toString();
  }

  @Override
  public String getSyntax() {
    return "append(<value|expression|field>, [,<value|expression|field>]*)";
  }

}
