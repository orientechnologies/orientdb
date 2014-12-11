/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
 *  * For more information: http://www.orientechnologies.com
 *
 */

package com.orientechnologies.orient.core.util;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;

public class ODateHelper {
  public static final String DEF_DATE_FORMAT     = "yyyy-MM-dd";
  public static final String DEF_DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss:SSS";

  public static Calendar getDatabaseCalendar() {
    return Calendar.getInstance(getDatabaseTimeZone());
  }

  public static TimeZone getDatabaseTimeZone() {
    final ODatabaseDocument db = ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();
    if (db != null && !db.isClosed())
      return ODatabaseRecordThreadLocal.INSTANCE.get().getStorage().getConfiguration().getTimeZone();
    return TimeZone.getDefault();
  }

  public static DateFormat getDateFormatInstance() {
    final ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();
    if (db != null && !db.isClosed())
      return db.getStorage().getConfiguration().getDateFormatInstance();
    else
      return new SimpleDateFormat(DEF_DATE_FORMAT);
  }

  public static String getDateFormat() {
    final ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();
    if (db != null && !db.isClosed())
      return db.getStorage().getConfiguration().getDateFormat();
    else
      return DEF_DATE_FORMAT;
  }

  public static DateFormat getDateTimeFormatInstance() {
    final ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();
    if (db != null && !db.isClosed())
      return db.getStorage().getConfiguration().getDateTimeFormatInstance();
    else
      return new SimpleDateFormat(DEF_DATETIME_FORMAT);
  }

  public static String getDateTimeFormat() {
    final ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();
    if (db != null && !db.isClosed())
      return db.getStorage().getConfiguration().getDateTimeFormat();
    else
      return DEF_DATETIME_FORMAT;
  }

  public static Date now() {
    return getDatabaseCalendar().getTime();
  }
}
