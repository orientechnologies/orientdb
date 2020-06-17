/*
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.util.ODateHelper;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.TimeZone;

/**
 * @author Johann Sorel (Geomatys)
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OSQLMethodFormat extends OAbstractSQLMethod {

  public static final String NAME = "format";

  public OSQLMethodFormat() {
    super(NAME, 1, 2);
  }

  @Override
  public Object execute(
      final Object iThis,
      final OIdentifiable iRecord,
      final OCommandContext iContext,
      Object ioResult,
      final Object[] iParams) {

    // TRY TO RESOLVE AS DYNAMIC VALUE
    Object v = getParameterValue(iRecord, iParams[0].toString());
    if (v == null)
      // USE STATIC ONE
      v = iParams[0].toString();

    if (v != null) {
      if (isCollectionOfDates(ioResult)) {
        List<String> result = new ArrayList<String>();
        Iterator<Object> iterator = OMultiValue.getMultiValueIterator(ioResult);
        final SimpleDateFormat format = new SimpleDateFormat(v.toString());
        if (iParams.length > 1) {
          format.setTimeZone(TimeZone.getTimeZone(iParams[1].toString()));
        } else {
          format.setTimeZone(ODateHelper.getDatabaseTimeZone());
        }
        while (iterator.hasNext()) {
          result.add(format.format(iterator.next()));
        }
        ioResult = result;
      } else if (ioResult instanceof Date) {
        final SimpleDateFormat format = new SimpleDateFormat(v.toString());
        if (iParams.length > 1) {
          format.setTimeZone(TimeZone.getTimeZone(iParams[1].toString()));
        } else {
          format.setTimeZone(ODateHelper.getDatabaseTimeZone());
        }
        ioResult = format.format(ioResult);
      } else {
        ioResult = ioResult != null ? String.format(v.toString(), ioResult) : null;
      }
    }

    return ioResult;
  }

  private boolean isCollectionOfDates(Object ioResult) {
    if (OMultiValue.isMultiValue(ioResult)) {
      Iterator<Object> iterator = OMultiValue.getMultiValueIterator(ioResult);
      while (iterator.hasNext()) {
        Object item = iterator.next();
        if (item != null && !(item instanceof Date)) {
          return false;
        }
      }
      return true;
    }
    return false;
  }
}
