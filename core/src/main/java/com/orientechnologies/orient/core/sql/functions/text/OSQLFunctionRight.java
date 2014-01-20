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
 * Returns the first characters from the end of the string.
 * 
 * @author Johann Sorel (Geomatys)
 * @author Luca Garulli
 */
public class OSQLFunctionRight extends OSQLFunctionAbstract {

  public static final String NAME = "right";

  public OSQLFunctionRight() {
    super(NAME, 2, 2);
  }

  @Override
  public Object execute(OIdentifiable iCurrentRecord, Object iCurrentResult, Object[] iFuncParams, OCommandContext iContext) {
    if (iFuncParams[0] == null || iFuncParams[1] == null)
      return null;

    final Object value = iFuncParams[0];
    final String valueAsString = value.toString();

    final int offset = Integer.parseInt(iFuncParams[1].toString());
    return valueAsString.substring(offset < valueAsString.length() ? valueAsString.length() - offset : 0);
  }

  @Override
  public String getSyntax() {
    return "right(<value|expression|field>, <characters>)";
  }

}
