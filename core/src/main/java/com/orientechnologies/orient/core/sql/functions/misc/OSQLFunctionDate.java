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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OQueryParsingException;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionAbstract;
import com.orientechnologies.orient.core.util.ODateHelper;

/**
 * Builds a date object from the format passed. If no arguments are passed, than the system date is built (like sysdate() function)
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * @see OSQLFunctionSysdate
 * 
 */
public class OSQLFunctionDate extends OSQLFunctionAbstract {
  public static final String NAME = "date";

  private Date               date;
  private SimpleDateFormat   format;

  /**
   * Get the date at construction to have the same date for all the iteration.
   */
  public OSQLFunctionDate() {
    super(NAME, 0, 3);
    date = new Date();
  }

  public Object execute(final OIdentifiable iCurrentRecord, final Object iCurrentResult, final Object[] iParameters,
      OCommandContext iContext) {
    if (iParameters.length == 0)
      return date;

    if (iParameters[0] instanceof Number)
      return new Date(((Number) iParameters[0]).longValue());

    if (format == null) {
      if (iParameters.length > 1) {
        format = new SimpleDateFormat((String) iParameters[1]);
        format.setTimeZone(ODateHelper.getDatabaseTimeZone());
      } else
        format = ODatabaseRecordThreadLocal.INSTANCE.get().getStorage().getConfiguration().getDateTimeFormatInstance();

      if (iParameters.length == 3)
        format.setTimeZone(TimeZone.getTimeZone(iParameters[2].toString()));
    }

    try {
      return format.parse((String) iParameters[0]);
    } catch (ParseException e) {
      throw new OQueryParsingException("Error on formatting date '" + iParameters[0] + "' using the format: " + format, e);
    }
  }

  public boolean aggregateResults(final Object[] configuredParameters) {
    return false;
  }

  public String getSyntax() {
    return "Syntax error: date([<date-as-string>] [,<format>] [,<timezone>])";
  }

  @Override
  public Object getResult() {
    format = null;
    return null;
  }
}
