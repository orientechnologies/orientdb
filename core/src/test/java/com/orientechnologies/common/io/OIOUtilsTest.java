package com.orientechnologies.common.io;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;

public class OIOUtilsTest {

  @Test
  public void shouldGetTimeAsMilis() {
    assertGetTimeAsMilis("2h", 2 * 3600 * 1000);
    assertGetTimeAsMilis("500ms", 500);
    assertGetTimeAsMilis("4d", 4 * 24 * 3600 * 1000);
    assertGetTimeAsMilis("6w", 6l * 7 * 24 * 3600 * 1000);

  }

  private void assertGetTimeAsMilis(String data, long expected) {
    assertEquals(OIOUtils.getTimeAsMillisecs(data), expected);
  }

  @Test
  public void shoudGetRightTimeFromString() throws ParseException {
    Calendar calendar = Calendar.getInstance();
    calendar.set(Calendar.HOUR_OF_DAY, 5);
    calendar.set(Calendar.MINUTE, 10);
    calendar.set(Calendar.SECOND, 0);
    calendar.set(Calendar.MILLISECOND, 0);
    Date d = OIOUtils.getTodayWithTime("05:10:00");
    assertEquals(calendar.getTime(), d);
  }

}
