package com.orientechnologies.orient.core.util;

import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;

public class ODateHelper {
  public static final String   DEFAULT_TIMEZONE = "UTC";
  public static final TimeZone UTC_TIMEZONE     = TimeZone.getTimeZone(DEFAULT_TIMEZONE);

  public static Calendar getDatabaseCalendar() {
    return Calendar.getInstance(getDatabaseTimeZone());
  }

  public static TimeZone getDatabaseTimeZone() {
    return ODatabaseRecordThreadLocal.INSTANCE.get().getStorage().getConfiguration().getTimeZone();
  }

  public static Date now() {
    return getDatabaseCalendar().getTime();
  }
}
