package com.orientechnologies.orient.core.sql.method.intl;


import com.ibm.icu.util.ULocale;
import com.orientechnologies.orient.core.util.ODateHelper;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;


/**
 * Tests the "toCalendar()" method implemented by the OSQLMethodToCalendar class.  Note
 * that the only input to the execute() method from the OSQLMethod interface
 * that is used is the ioResult argument (the 4th argument).
 *
 * @author Saeed Tabrizi (saeed a_t nowcando.com)
 */
public class OSQLMethodToCalendarTest {

  private OSQLMethodToCalendar function;


  @Before
  public void setup() {
    function = new OSQLMethodToCalendar();
  }

  @Test
  public void testNull() {

    Object result = function.execute(null, null, null, null, null);
    assertEquals(result, null);
  }

  @Test
  public void testNullParams() {

    // The expected behavior is to return the same date .
    Date aDate = ODateHelper.now();
    OCalendar expCalendar = new OCalendar(aDate);
    Object[] args = new Object[]{null,null,null,null};

    OCalendar result = (OCalendar) function.execute(null, null, null, aDate, args);
    assertEquals(expCalendar.toDate(),result.toDate());
  }

  @Test
  public void testADate() {
    // expect to return same Date type
    Date aDate = new Date(2017,1,25);

    Object result = function.execute(null, null, null, aDate, null);
    assertEquals( aDate,result);
  }

  @Test
  public void testANumber() {
    // expect to return same Long type
    long dt = 1428638400000L; // 04/10/2015

    Object result = function.execute(null, null, null, dt, null);
    assertEquals(dt,result);
  }

  @Test
  public void testAString() {
    // expect to return same String type
    String dt = "20151004T00:00:00"; // 04/10/2015

    Object result = function.execute(null, null, null, dt, null);
    assertEquals(dt,result);
  }

  @Test
  public void testADateCalendar() {
    // The expected behavior is to return the list itself.
    Date aDate = ODateHelper.now();
    OCalendar expected = new OCalendar(aDate);
    Object[] args = new Object[]{"gregorian"};

    OCalendar actual = (OCalendar) function.execute(null, null, null, aDate, args);
    assertEquals(expected.toDate().getTime(), actual.toDate().getTime());
  }

  @Test
  public void testADatePersianCalendar() {
    // The expected behavior is to return the list itself.
    Date aDate = new Date(1485533898220L); // 27-01-2017
    OCalendar expected = new OCalendar(aDate); // 8-11-1395  friday in persian calendar
    Object[] args = new Object[]{"persian"};

    OCalendar actual = (OCalendar) function.execute(null, null, null, aDate, args);
    assertEquals(1395, actual.getYear());
    assertEquals(11, actual.getMonth()+1);
    assertEquals(8, actual.getDay());
    assertEquals(6, actual.getDayOfWeek());
    assertEquals(314, actual.getDayOfYear());
    assertEquals(false, actual.isLeapMonth());
    assertEquals(OCalendar.CAL_FRIDAY, actual.getDaynameOfWeek());
  }

  @Test
  public void testAddToDatePersianCalendar() {
    // The expected behavior is to return the list itself.
    Date aDate = new Date(1485533898220L); // 27-01-2017
    OCalendar expected = new OCalendar(aDate); // 8-11-1395  friday in persian calendar
    Object[] args = new Object[]{"persian"};

    OCalendar actual = (OCalendar) function.execute(null, null, null, aDate, args);
    actual.add(1,"month");
    assertEquals(1395, actual.getYear());
    assertEquals(12, actual.getMonth()+1); // added 1 month equals to 30 days ---> expected 12
    assertEquals(8, actual.getDay());
    assertEquals(1, actual.getDayOfWeek());
    assertEquals(344, actual.getDayOfYear());
    assertEquals(false, actual.isLeapMonth());
    assertEquals(OCalendar.CAL_SUNDAY, actual.getDaynameOfWeek());
  }

  @Test
  public void testSubtractToDatePersianCalendar() {
    // The expected behavior is to return the list itself.
    Date aDate = new Date(1485533898220L); // 27-01-2017
    OCalendar expected = new OCalendar(aDate); // 8-11-1395  friday in persian calendar
    Object[] args = new Object[]{"persian"};

    OCalendar actual = (OCalendar) function.execute(null, null, null, aDate, args);
    actual.subtract(2,"year");
    actual.subtract(1,"month");
    assertEquals(1393, actual.getYear()); // subtract 2 year ---> expected 1393
    assertEquals(10, actual.getMonth()+1); // subtract 1 month equals to 30 days ---> expected 10
    assertEquals(8, actual.getDay());
    assertEquals(2, actual.getDayOfWeek());
    assertEquals(284, actual.getDayOfYear());
    assertEquals(false, actual.isLeapMonth());
    assertEquals(OCalendar.CAL_MONDAY, actual.getDaynameOfWeek());
  }


  @Test
  public void testFromNowInDatePersianCalendar1() {
    // The expected behavior is to return the list itself.
    Date aDate = new Date(1485533898220L); // 27-01-2017
    OCalendar expected = new OCalendar(aDate); // 8-11-1395  friday in persian calendar
    Object[] args = new Object[]{"persian"};

    OCalendar actual = (OCalendar) function.execute(null, null, null, aDate, args);
    actual.subtract(2,"year");
    actual.subtract(1,"month");
    assertEquals(1393, actual.getYear()); // subtract 2 year ---> expected 1393
    assertEquals(10, actual.getMonth()+1); // subtract 1 month equals to 30 days ---> expected 10
    assertEquals(8, actual.getDay());
    assertEquals(2, actual.getDayOfWeek());
    assertEquals(284, actual.getDayOfYear());
    assertEquals(false, actual.isLeapYear());
    assertEquals(OCalendar.CAL_MONDAY, actual.getDaynameOfWeek());
    // HACK: i comment the following because depends on current time on machines . it works well on ok .
    //assertEquals("2 year(s) ago", actual.fromNow());
  }

  @Test
  public void testFromNowInDatePersianCalendar2() {
    // The expected behavior is to return the list itself.
    Date aDate = new Date(1485533898220L); // 27-01-2017
    OCalendar expected = new OCalendar(aDate); // 8-11-1395  friday in persian calendar
    Object[] args = new Object[]{"persian"};

    OCalendar actual = (OCalendar) function.execute(null, null, null, aDate, args);

    actual.subtract(1,"month");
    assertEquals(1395, actual.getYear()); // subtract 2 year ---> expected 1393
    assertEquals(10, actual.getMonth()+1); // subtract 1 month equals to 30 days ---> expected 10
    assertEquals(8, actual.getDay());
    assertEquals(4, actual.getDayOfWeek());
    assertEquals(284, actual.getDayOfYear());
    assertEquals(false, actual.isLeapMonth());
    assertEquals(OCalendar.CAL_WEDNESDAY, actual.getDaynameOfWeek());
    // HACK: i comment the following because depends on current time on machines . it works well on ok .
   // assertEquals("2 month(s) ago", actual.fromNow());
  }

  @Test
  public void testFromNowInDatePersianCalendar3() {
    // The expected behavior is to return the list itself.
    Date aDate = new Date(1485533898220L); // 27-01-2017
    OCalendar expected = new OCalendar(aDate); // 8-11-1395  friday in persian calendar
    Object[] args = new Object[]{"persian"};

    OCalendar actual = (OCalendar) function.execute(null, null, null, aDate, args);

    actual.subtract(1,"month");
    assertEquals(1395, actual.getYear()); // subtract 2 year ---> expected 1393
    assertEquals(10, actual.getMonth()+1); // subtract 1 month equals to 30 days ---> expected 10
    assertEquals(8, actual.getDay());
    assertEquals(4, actual.getDayOfWeek());
    assertEquals(284, actual.getDayOfYear());
    assertEquals(false, actual.isLeapMonth());
    assertEquals(2, actual.getWeekOfMonth());
    assertEquals(OCalendar.CAL_WEDNESDAY, actual.getDaynameOfWeek());
    // HACK: i comment the following because depends on current time on machines . it works well on ok .
    /*assertEquals("2 ماه پیش", actual.fromNow(new String[]{"مانده", "پیش",
            "سال", "ماه", "روز","ساعت","دقیقه","ثانیه","میلی ثانیه"}));*/
  }


  @Test
  public void testFormatInDatePersianCalendar1() {
    // The expected behavior is to return the list itself.
    Date aDate = new Date(1485533898220L); // 27-01-2017
    OCalendar expected = new OCalendar(aDate); // 8-11-1395  friday in persian calendar
    Object[] args = new Object[]{"persian"};

    OCalendar actual = (OCalendar) function.execute(null, null, null, aDate, args);

    actual.subtract(1,"month");
    assertEquals(1395, actual.getYear()); // subtract 2 year ---> expected 1393
    assertEquals(10, actual.getMonth()+1); // subtract 1 month equals to 30 days ---> expected 10
    assertEquals(8, actual.getDay());
    assertEquals(4, actual.getDayOfWeek());
    assertEquals(284, actual.getDayOfYear());
    assertEquals(false, actual.isLeapMonth());
    assertEquals(2, actual.getWeekOfMonth());
    assertEquals(OCalendar.CAL_WEDNESDAY, actual.getDaynameOfWeek());
    assertEquals("۱۳۹۵-۱۰-۰۸", actual.format("yyyy-MM-dd","fa_IR"));
  }

  @Test
  public void testFormatInDatePersianCalendar2() {
    // The expected behavior is to return the list itself.
    Date aDate = new Date(1485533898220L); // 27-01-2017
    OCalendar expected = new OCalendar(aDate); // 8-11-1395  friday in persian calendar
    Object[] args = new Object[]{"persian"};

    OCalendar actual = (OCalendar) function.execute(null, null, null, aDate, args);

    actual.subtract(1,"month");
    assertEquals(1395, actual.getYear()); // subtract 2 year ---> expected 1393
    assertEquals(10, actual.getMonth()+1); // subtract 1 month equals to 30 days ---> expected 10
    assertEquals(8, actual.getDay());
    assertEquals(4, actual.getDayOfWeek());
    assertEquals(284, actual.getDayOfYear());
    assertEquals(false, actual.isLeapMonth());
    assertEquals(2, actual.getWeekOfMonth());
    assertEquals(OCalendar.CAL_WEDNESDAY, actual.getDaynameOfWeek());
    assertEquals("1395-10-08", actual.format("yyyy-MM-dd","en_US"));
  }

  @Test
  public void testStartOfDatePersianCalendar1() {
    // The expected behavior is to return the list itself.
    Date aDate = new Date(1485533898220L); // 27-01-2017
    OCalendar expected = new OCalendar(aDate); // 8-11-1395  friday in persian calendar
    Object[] args = new Object[]{"persian"};

    OCalendar actual = (OCalendar) function.execute(null, null, null, aDate, args);

    actual.startOf("YEAR"); // set first day of current year
    assertEquals(1395, actual.getYear()); //   expected 1395
    assertEquals(1, actual.getMonth()+1); //  expected 1
    assertEquals(1, actual.getDay());//  expected 1

  }

  @Test
  public void testStartOfDatePersianCalendar2() {
    // The expected behavior is to return the list itself.
    Date aDate = new Date(1485533898220L); // 27-01-2017
    OCalendar expected = new OCalendar(aDate); // 8-11-1395  friday in persian calendar
    Object[] args = new Object[]{"persian"};

    OCalendar actual = (OCalendar) function.execute(null, null, null, aDate, args);

    actual.endOf("YEAR"); // set first day of month
    assertEquals(1395, actual.getYear()); //   expected 1395
    assertEquals(12, actual.getMonth()+1); //  expected 12
    assertEquals(30, actual.getDay());//  expected 1
    assertEquals(true, actual.isLeapYear());
    assertEquals("1395-12-30", actual.format("yyyy-MM-dd","en_US"));
  }

}
