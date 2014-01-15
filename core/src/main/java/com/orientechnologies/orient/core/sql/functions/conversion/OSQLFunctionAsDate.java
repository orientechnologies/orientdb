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

import java.text.ParseException;
import java.util.Date;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionAbstract;

/**
 * Transforms a value to date. If the conversion is not possible, null is returned.
 * 
 * @author Johann Sorel (Geomatys)
 * @author Luca Garulli
 */
public class OSQLFunctionAsDate extends OSQLFunctionAbstract {

  public static final String NAME = "asdate";

  public OSQLFunctionAsDate() {
    super(NAME, 1, 1);
  }

  @Override
  public Object execute(final OIdentifiable iCurrentRecord, final Object iCurrentResult, final Object[] iFuncParams,
      final OCommandContext iContext) {
    final Object value = iFuncParams[0];

    if (value != null) {
      if (value instanceof Number) {
        return new Date(((Number) value).longValue());
      } else if (!(value instanceof Date)) {
        try {
          return ODatabaseRecordThreadLocal.INSTANCE.get().getStorage().getConfiguration().getDateFormatInstance()
              .parse(value.toString());
        } catch (ParseException e) {
          // IGNORE IT: RETURN NULL
        }
      }
    }
    return null;
  }

  @Override
  public String getSyntax() {
    return "asDate(<value|expression|field>)";
  }
}
