package com.orientechnologies.common.util;

import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

public class ODateHelper {
  private static final String   DEFAULT_TIMEZONE = "UTC";
  private static final TimeZone UTC_TIMEZONE     = TimeZone.getTimeZone(DEFAULT_TIMEZONE);

  public static Calendar nowAsCalendar() {
    return Calendar.getInstance(UTC_TIMEZONE);
  }

  public static Date now() {
    return nowAsCalendar().getTime();
  }

  public static TimeZone getDefaultTimeZone() {
    return UTC_TIMEZONE;
  }

  public static String getDefaultTimeZoneName() {
    return DEFAULT_TIMEZONE;
  }
}
