package com.orientechnologies.orient.core.sql.method.misc;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.ArrayList;
import java.util.HashSet;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests the "asSet()" method implemented by the OSQLMethodAsSet class. Note that the only input to
 * the execute() method from the OSQLMethod interface that is used is the ioResult argument (the 4th
 * argument).
 *
 * @author Michael MacFadden
 */
public class OSQLMethodAsSetTest {

  private OSQLMethodAsSet function;

  @Before
  public void setup() {
    function = new OSQLMethodAsSet();
  }

  @Test
  public void testSet() {
    // The expected behavior is to return the set itself.
    HashSet<Object> aSet = new HashSet<Object>();
    aSet.add(1);
    aSet.add("2");
    Object result = function.execute(null, null, null, aSet, null);
    assertEquals(result, aSet);
  }

  @Test
  public void testNull() {
    // The expected behavior is to return an empty set.
    Object result = function.execute(null, null, null, null, null);
    assertEquals(result, new HashSet<Object>());
  }

  @Test
  public void testCollection() {
    // The expected behavior is to return a set with all of the elements
    // of the collection in it.
    ArrayList<Object> aCollection = new ArrayList<Object>();
    aCollection.add(1);
    aCollection.add("2");
    Object result = function.execute(null, null, null, aCollection, null);

    HashSet<Object> expected = new HashSet<Object>();
    expected.add(1);
    expected.add("2");
    assertEquals(result, expected);
  }

  public void testIterable() {
    // The expected behavior is to return a set with all of the elements
    // of the iterable in it.
    ArrayList<Object> values = new ArrayList<Object>();
    values.add(1);
    values.add("2");

    TestIterable<Object> anIterable = new TestIterable<Object>(values);
    Object result = function.execute(null, null, null, anIterable, null);

    HashSet<Object> expected = new HashSet<Object>();
    expected.add(1);
    expected.add("2");

    assertEquals(result, expected);
  }

  public void testIterator() {
    // The expected behavior is to return a set with all of the elements
    // of the iterator in it.
    ArrayList<Object> values = new ArrayList<Object>();
    values.add(1);
    values.add("2");

    TestIterable<Object> anIterable = new TestIterable<Object>(values);
    Object result = function.execute(null, null, null, anIterable.iterator(), null);

    HashSet<Object> expected = new HashSet<Object>();
    expected.add(1);
    expected.add("2");

    assertEquals(result, expected);
  }

  public void testODocument() {
    // The expected behavior is to return a set with only the single
    // ODocument in it.
    ODocument doc = new ODocument();
    doc.field("f1", 1);
    doc.field("f2", 2);

    Object result = function.execute(null, null, null, doc, null);

    HashSet<Object> expected = new HashSet<Object>();
    expected.add(doc);

    assertEquals(result, expected);
  }

  public void testOtherSingleValue() {
    // The expected behavior is to return a set with only the single
    // element in it.

    Object result = function.execute(null, null, null, new Integer(4), null);
    HashSet<Object> expected = new HashSet<Object>();
    expected.add(new Integer(4));
    assertEquals(result, expected);
  }
}
