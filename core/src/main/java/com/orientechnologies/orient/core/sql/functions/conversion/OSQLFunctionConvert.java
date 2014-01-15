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
package com.orientechnologies.orient.core.sql.functions.conversion;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionAbstract;

/**
 * Converts a value to another type in Java or OrientDB's supported types.
 * 
 * @author Luca Garulli
 */
public class OSQLFunctionConvert extends OSQLFunctionAbstract {

  public static final String NAME = "convert";

  public OSQLFunctionConvert() {
    super(NAME, 2, 2);
  }

  @Override
  public String getSyntax() {
    return "convert(<value|expression|field>, <type>)";
  }

  @Override
  public Object execute(OIdentifiable iCurrentRecord, Object iCurrentResult, Object[] iFuncParams, OCommandContext iContext) {
    if (iFuncParams[0] == null || iFuncParams[1] == null)
      return null;

    final String destType = iFuncParams[1].toString();

    if (destType.contains("."))
      try {
        return OType.convert(iFuncParams[0], Class.forName(destType));
      } catch (ClassNotFoundException e) {
      }
    else {
      final OType orientType = OType.valueOf(destType.toUpperCase());
      if (orientType != null)
        return OType.convert(iFuncParams[0], orientType.getDefaultJavaType());
    }

    return null;
  }
}
