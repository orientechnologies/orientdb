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
package com.orientechnologies.orient.core.sql.method.misc;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.util.ODateHelper;

/**
 * 
 * @author Johann Sorel (Geomatys)
 * @author Luca Garulli
 */
public class OSQLMethodFormat extends OAbstractSQLMethod {

  public static final String NAME = "format";

  public OSQLMethodFormat() {
    super(NAME, 1, 2);
  }

  @Override
  public Object execute(OIdentifiable iRecord, OCommandContext iContext, Object ioResult, Object[] iMethodParams) {

    final Object v = getParameterValue(iRecord, iMethodParams[0].toString());
    if (v != null) {
      if (ioResult instanceof Date) {
        final SimpleDateFormat format = new SimpleDateFormat(v.toString());
        if (iMethodParams.length > 1)
          format.setTimeZone(TimeZone.getTimeZone(iMethodParams[1].toString()));
        else
          format.setTimeZone(ODateHelper.getDatabaseTimeZone());
        ioResult = format.format(ioResult);
      } else {
        ioResult = ioResult != null ? String.format(v.toString(), ioResult) : null;
      }
    }

    return ioResult;
  }
}
