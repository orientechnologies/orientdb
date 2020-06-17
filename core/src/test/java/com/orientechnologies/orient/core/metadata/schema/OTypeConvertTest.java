package com.orientechnologies.orient.core.metadata.schema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.Test;

/**
 * Test the covert method of the OType class.
 *
 * @author Michael MacFadden
 */
public class OTypeConvertTest {

  //
  // General cases
  //

  @Test
  public void testSameType() {
    ArrayList<Object> aList = new ArrayList<Object>();
    aList.add(1);
    aList.add("2");
    Object result = OType.convert(aList, ArrayList.class);

    assertEquals(result, aList);
  }

  @Test
  public void testAssignableType() {
    ArrayList<Object> aList = new ArrayList<Object>();
    aList.add(1);
    aList.add("2");
    Object result = OType.convert(aList, List.class);

    assertEquals(result, aList);
  }

  @Test
  public void testNull() {
    Object result = OType.convert(null, Boolean.class);
    assertEquals(result, null);
  }

  @Test
  public void testCannotConvert() {
    // Expected behavior is to not convert and return null
    Object result = OType.convert(true, Long.class);
    assertEquals(result, null);
  }

  //
  // To String
  //

  @Test
  public void testToStringFromString() {
    Object result = OType.convert("foo", String.class);
    assertEquals(result, "foo");
  }

  @Test
  public void testToStringFromNumber() {
    Object result = OType.convert(10, String.class);
    assertEquals(result, "10");
  }

  //
  // To Byte
  //

  @Test
  public void testToBytePrimitiveFromByte() {
    Object result = OType.convert((byte) 10, Byte.TYPE);
    assertEquals(result, (byte) 10);
  }

  @Test
  public void testToByteFromByte() {
    Object result = OType.convert((byte) 10, Byte.class);
    assertEquals(result, (byte) 10);
  }

  @Test
  public void testToByteFromString() {
    Object result = OType.convert("10", Byte.class);
    assertEquals(result, (byte) 10);
  }

  @Test
  public void testToByteFromNumber() {
    Object result = OType.convert(10.0D, Byte.class);
    assertEquals(result, (byte) 10);
  }

  //
  // To Short
  //

  @Test
  public void testToShortPrmitveFromShort() {
    Object result = OType.convert((short) 10, Short.TYPE);
    assertEquals(result, (short) 10);
  }

  @Test
  public void testToShortFromShort() {
    Object result = OType.convert((short) 10, Short.class);
    assertEquals(result, (short) 10);
  }

  @Test
  public void testToShortFromString() {
    Object result = OType.convert("10", Short.class);
    assertEquals(result, (short) 10);
  }

  @Test
  public void testToShortFromNumber() {
    Object result = OType.convert(10.0D, Short.class);
    assertEquals(result, (short) 10);
  }

  //
  // To Integer
  //

  @Test
  public void testToIntegerPrimitveFromInteger() {
    Object result = OType.convert(10, Integer.TYPE);
    assertEquals(result, 10);
  }

  @Test
  public void testToIntegerFromInteger() {
    Object result = OType.convert(10, Integer.class);
    assertEquals(result, 10);
  }

  @Test
  public void testToIntegerFromString() {
    Object result = OType.convert("10", Integer.class);
    assertEquals(result, 10);
  }

  @Test
  public void testToIntegerFromNumber() {
    Object result = OType.convert(10.0D, Integer.class);
    assertEquals(result, 10);
  }

  //
  // To Long
  //

  @Test
  public void testToLongPrimitiveFromLong() {
    Object result = OType.convert(10L, Long.TYPE);
    assertEquals(result, 10L);
  }

  @Test
  public void testToLongFromLong() {
    Object result = OType.convert(10L, Long.class);
    assertEquals(result, 10L);
  }

  @Test
  public void testToLongFromString() {
    Object result = OType.convert("10", Long.class);
    assertEquals(result, 10L);
  }

  @Test
  public void testToLongFromNumber() {
    Object result = OType.convert(10.0D, Long.class);
    assertEquals(result, 10L);
  }

  //
  // To Float
  //

  @Test
  public void testToFloatPrimitiveFromFloat() {
    Object result = OType.convert(10.65f, Float.TYPE);
    assertEquals(result, 10.65f);
  }

  @Test
  public void testToFloatFromFloat() {
    Object result = OType.convert(10.65f, Float.class);
    assertEquals(result, 10.65f);
  }

  @Test
  public void testToFloatFromString() {
    Object result = OType.convert("10.65", Float.class);
    assertEquals(result, 10.65f);
  }

  @Test
  public void testToFloatFromNumber() {
    Object result = OType.convert(4, Float.class);
    assertEquals(result, 4f);
  }

  //
  // To BigDecimal
  //

  @Test
  public void testToBigDecimalFromBigDecimal() {
    Object result = OType.convert(new BigDecimal("10.65"), BigDecimal.class);
    assertEquals(result, new BigDecimal("10.65"));
  }

  @Test
  public void testToBigDecimalFromString() {
    Object result = OType.convert("10.65", BigDecimal.class);
    assertEquals(result, new BigDecimal("10.65"));
  }

  @Test
  public void testToBigDecimalFromNumber() {
    Object result = OType.convert(4.98D, BigDecimal.class);
    assertEquals(result, new BigDecimal("4.98"));
  }

  //
  // To Double
  //

  @Test
  public void testToDoublePrimitiveFromDouble() {
    Object result = OType.convert(5.4D, Double.TYPE);
    assertEquals(result, 5.4D);
  }

  @Test
  public void testToDoubleFromDouble() {
    Object result = OType.convert(5.4D, Double.class);
    assertEquals(result, 5.4D);
  }

  @Test
  public void testToDoubleFromString() {
    Object result = OType.convert("5.4", Double.class);
    assertEquals(result, 5.4D);
  }

  @Test
  public void testToDoubleFromFloat() {
    Object result = OType.convert(5.4f, Double.class);
    assertEquals(result, 5.4D);
  }

  @Test
  public void testToDoubleFromNonFloatNumber() {
    Object result = OType.convert(5, Double.class);
    assertEquals(result, 5D);
  }

  //
  // To Boolean
  //

  @Test
  public void testToBooleanPrimitiveFromBoolean() {
    Object result = OType.convert(true, Boolean.TYPE);
    assertEquals(result, true);
  }

  @Test
  public void testToBooleanFromBoolean() {
    Object result = OType.convert(true, Boolean.class);
    assertEquals(result, true);
  }

  @Test
  public void testToBooleanFromFalseString() {
    Object result = OType.convert("false", Boolean.class);
    assertEquals(result, false);
  }

  @Test
  public void testToBooleanFromTrueString() {
    Object result = OType.convert("true", Boolean.class);
    assertEquals(result, true);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testToBooleanFromInvalidString() {
    OType.convert("invalid", Boolean.class);
  }

  @Test
  public void testToBooleanFromZeroNumber() {
    Object result = OType.convert(0, Boolean.class);
    assertEquals(result, false);
  }

  @Test
  public void testToBooleanFromNonZeroNumber() {
    Object result = OType.convert(1, Boolean.class);
    assertEquals(result, true);
  }

  //
  // To Date
  //

  @Test
  public void testToDateFromDate() {
    Date d = Calendar.getInstance().getTime();
    Object result = OType.convert(d, Date.class);
    assertEquals(result, d);
  }

  @Test
  public void testToDateFromNumber() {
    Long time = System.currentTimeMillis();
    Object result = OType.convert(time, Date.class);
    assertEquals(result, new Date(time));
  }

  @Test
  public void testToDateFromLongString() {
    Long time = System.currentTimeMillis();
    Object result = OType.convert(time.toString(), Date.class);
    assertEquals(result, new Date(time));
  }

  @Test
  public void testToDateFromDateString() {
    Long time = System.currentTimeMillis();
    Object result = OType.convert(time.toString(), Date.class);
    assertEquals(result, new Date(time));
  }

  //
  // To Set
  //

  @Test
  public void testToSetFromSet() {
    HashSet<Object> set = new HashSet<Object>();
    set.add(1);
    set.add("2");
    Object result = OType.convert(set, Set.class);
    assertEquals(result, set);
  }

  @Test
  public void testToSetFromCollection() {
    ArrayList<Object> list = new ArrayList<Object>();
    list.add(1);
    list.add("2");

    Object result = OType.convert(list, Set.class);

    HashSet<Object> expected = new HashSet<Object>();
    expected.add(1);
    expected.add("2");
    assertEquals(result, expected);
  }

  @Test
  public void testToSetFromNonCollection() {
    HashSet<Object> set = new HashSet<Object>();
    set.add(1);
    Object result = OType.convert(1, Set.class);
    assertEquals(result, set);
  }

  //
  // To List
  //

  @Test
  public void testToListFromList() {
    ArrayList<Object> list = new ArrayList<Object>();
    list.add(1);
    list.add("2");
    Object result = OType.convert(list, List.class);
    assertEquals(result, list);
  }

  @Test
  public void testToListFromCollection() {
    HashSet<Object> set = new HashSet<Object>();
    set.add(1);
    set.add("2");

    @SuppressWarnings("unchecked")
    List<Object> result = (List<Object>) OType.convert(set, List.class);

    assertEquals(result.size(), 2);
    assertTrue(result.containsAll(set));
  }

  @Test
  public void testToListFromNonCollection() {
    ArrayList<Object> expected = new ArrayList<Object>();
    expected.add(1);
    Object result = OType.convert(1, List.class);
    assertEquals(result, expected);
  }

  //
  // To List
  //

  @Test
  public void testToCollectionFromList() {
    ArrayList<Object> list = new ArrayList<Object>();
    list.add(1);
    list.add("2");
    Object result = OType.convert(list, Collection.class);
    assertEquals(result, list);
  }

  @Test
  public void testToCollectionFromCollection() {
    HashSet<Object> set = new HashSet<Object>();
    set.add(1);
    set.add("2");

    @SuppressWarnings("unchecked")
    Collection<Object> result = (Collection<Object>) OType.convert(set, Collection.class);

    assertEquals(result.size(), 2);
    assertTrue(result.containsAll(set));
  }

  @Test
  public void testToCollectionFromNonCollection() {
    @SuppressWarnings("unchecked")
    Collection<Object> result = (Collection<Object>) OType.convert(1, Collection.class);

    assertEquals(result.size(), 1);
    assertTrue(result.contains(1));
  }
}
