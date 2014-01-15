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
 * Extracts a sub string from the original.
 * 
 * @author Johann Sorel (Geomatys)
 * @author Luca Garulli
 */
public class OSQLFunctionSubString extends OSQLFunctionAbstract {

  public static final String NAME = "substring";

  public OSQLFunctionSubString() {
    super(NAME, 2, 3);
  }

  @Override
  public Object execute(OIdentifiable iCurrentRecord, Object iCurrentResult, Object[] iFuncParams, OCommandContext iContext) {
    if (iFuncParams[0] == null || iFuncParams[1] == null)
      return null;

    final Object value = iFuncParams[0];

    if (iFuncParams.length > 2)
      return value.toString().substring(Integer.parseInt(iFuncParams[1].toString()), Integer.parseInt(iFuncParams[2].toString()));
    else
      return value.toString().substring(Integer.parseInt(iFuncParams[1].toString()));
  }

  @Override
  public String getSyntax() {
    return "subString(<value|expression|field>, <from-index> [,<to-index>])";
  }
}
