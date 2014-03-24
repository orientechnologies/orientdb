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
import com.orientechnologies.orient.core.sql.method.misc.OAbstractSQLMethod;

/**
 * Extracts a sub string from the original.
 * 
 * @author Johann Sorel (Geomatys)
 * @author Luca Garulli
 */
public class OSQLMethodSubString extends OAbstractSQLMethod {

  public static final String NAME = "substring";

  public OSQLMethodSubString() {
    super(NAME, 1, 2);
  }

  @Override
  public String getSyntax() {
    return "subString(<from-index> [,<to-index>])";
  }

  @Override
  public Object execute(Object iThis, OIdentifiable iCurrentRecord, OCommandContext iContext, Object ioResult, Object[] iParams) {
    if (iThis == null || iParams[0] == null) {
      return null;
    }

    if (iParams.length>1) {
      return iThis.toString().substring(Integer.parseInt(iParams[0].toString()), Integer.parseInt(iParams[1].toString()));
    } else {
      return iThis.toString().substring(Integer.parseInt(iParams[0].toString()));
    }
  }
}
