package com.orientechnologies.orient.core.util;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;

public class ODateHelper {
  public static final String DEF_DATE_FORMAT     = "yyyy-MM-dd";
  public static final String DEF_DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss:SSS";

  public static Calendar getDatabaseCalendar() {
    return Calendar.getInstance(getDatabaseTimeZone());
  }

  public static TimeZone getDatabaseTimeZone() {
    final ODatabaseRecord db = ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();
    if (db != null && !db.isClosed())
      return ODatabaseRecordThreadLocal.INSTANCE.get().getStorage().getConfiguration().getTimeZone();
    return TimeZone.getDefault();
  }

  public static DateFormat getDateFormatInstance() {
    final ODatabaseRecord db = ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();
    if (db != null && !db.isClosed())
      return db.getStorage().getConfiguration().getDateFormatInstance();
    else
      return new SimpleDateFormat(DEF_DATE_FORMAT);
  }

  public static String getDateFormat() {
    final ODatabaseRecord db = ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();
    if (db != null && !db.isClosed())
      return db.getStorage().getConfiguration().getDateFormat();
    else
      return DEF_DATE_FORMAT;
  }

  public static DateFormat getDateTimeFormatInstance() {
    final ODatabaseRecord db = ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();
    if (db != null && !db.isClosed())
      return db.getStorage().getConfiguration().getDateTimeFormatInstance();
    else
      return new SimpleDateFormat(DEF_DATETIME_FORMAT);
  }

  public static String getDateTimeFormat() {
    final ODatabaseRecord db = ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();
    if (db != null && !db.isClosed())
      return db.getStorage().getConfiguration().getDateTimeFormat();
    else
      return DEF_DATETIME_FORMAT;
  }

  public static Date now() {
    return getDatabaseCalendar().getTime();
  }
}
