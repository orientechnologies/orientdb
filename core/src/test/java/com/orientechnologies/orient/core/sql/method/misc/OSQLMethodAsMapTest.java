package com.orientechnologies.orient.core.sql.method.misc;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.ArrayList;
import java.util.HashMap;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests the "asMap()" method implemented by the OSQLMethodAsMap class. Note that the only input to
 * the execute() method from the OSQLMethod interface that is used is the ioResult argument (the 4th
 * argument).
 *
 * @author Michael MacFadden
 */
public class OSQLMethodAsMapTest {

  private OSQLMethodAsMap function;

  @Before
  public void setup() {
    function = new OSQLMethodAsMap();
  }

  @Test
  public void testMap() {
    // The expected behavior is to return the map itself.
    HashMap<Object, Object> aMap = new HashMap<Object, Object>();
    aMap.put("p1", 1);
    aMap.put("p2", 2);
    Object result = function.execute(null, null, null, aMap, null);
    assertEquals(result, aMap);
  }

  @Test
  public void testNull() {
    // The expected behavior is to return an empty map.
    Object result = function.execute(null, null, null, null, null);
    assertEquals(result, new HashMap<Object, Object>());
  }

  public void testODocument() {
    // The expected behavior is to return a map that has the field names mapped
    // to the field values of the ODocument.
    ODocument doc = new ODocument();
    doc.field("f1", 1);
    doc.field("f2", 2);

    Object result = function.execute(null, null, null, doc, null);

    assertEquals(result, doc.toMap());
  }

  @Test
  public void testIterable() {
    // The expected behavior is to return a map where the even values (0th,
    // 2nd, 4th, etc) are keys and the odd values (1st, 3rd, etc.) are
    // property values.
    ArrayList<Object> aCollection = new ArrayList<Object>();
    aCollection.add("p1");
    aCollection.add(1);
    aCollection.add("p2");
    aCollection.add(2);

    Object result = function.execute(null, null, null, aCollection, null);

    HashMap<Object, Object> expected = new HashMap<Object, Object>();
    expected.put("p1", 1);
    expected.put("p2", 2);
    assertEquals(result, expected);
  }

  @Test
  public void testIterator() {
    // The expected behavior is to return a map where the even values (0th,
    // 2nd, 4th, etc) are keys and the odd values (1st, 3rd, etc.) are
    // property values.
    ArrayList<Object> aCollection = new ArrayList<Object>();
    aCollection.add("p1");
    aCollection.add(1);
    aCollection.add("p2");
    aCollection.add(2);

    Object result = function.execute(null, null, null, aCollection.iterator(), null);

    HashMap<Object, Object> expected = new HashMap<Object, Object>();
    expected.put("p1", 1);
    expected.put("p2", 2);
    assertEquals(result, expected);
  }

  public void testOtherValue() {
    // The expected behavior is to return null.
    Object result = function.execute(null, null, null, new Integer(4), null);
    assertEquals(result, null);
  }
}
