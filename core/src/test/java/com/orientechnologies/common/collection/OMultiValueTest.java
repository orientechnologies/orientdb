package com.orientechnologies.common.collection;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Luigi Dell'Aquila
 */
@Test
public class OMultiValueTest {

  @Test
  public void testListSize() {
    List<String> collection = new ArrayList<String>();
    OMultiValue.add(collection, "foo");
    OMultiValue.add(collection, "bar");
    OMultiValue.add(collection, "baz");

    Assert.assertEquals(OMultiValue.getSize(collection), 3);
  }

  @Test
  public void testArraySize() {
    String[] collection = new String[] { "foo", "bar", "baz" };
    Assert.assertEquals(OMultiValue.getSize(collection), 3);
  }

  @Test
  public void testListFirstLast() {
    List<String> collection = new ArrayList<String>();
    OMultiValue.add(collection, "foo");
    OMultiValue.add(collection, "bar");
    OMultiValue.add(collection, "baz");

    Assert.assertEquals(OMultiValue.getFirstValue(collection), "foo");
    Assert.assertEquals(OMultiValue.getLastValue(collection), "baz");
  }

  @Test
  public void testArrayFirstLast() {
    String[] collection = new String[] { "foo", "bar", "baz" };
    Assert.assertEquals(OMultiValue.getFirstValue(collection), "foo");
    Assert.assertEquals(OMultiValue.getLastValue(collection), "baz");
  }

  @Test
  public void testListValue() {
    Assert.assertNull(OMultiValue.getValue(null, 0));
    List<String> collection = new ArrayList<String>();
    OMultiValue.add(collection, "foo");
    OMultiValue.add(collection, "bar");
    OMultiValue.add(collection, "baz");

    Assert.assertNull(OMultiValue.getValue(new Object(), 0));

    Assert.assertEquals(OMultiValue.getValue(collection, 0), "foo");
    Assert.assertEquals(OMultiValue.getValue(collection, 2), "baz");
    Assert.assertNull(OMultiValue.getValue(new Object(), 3));

  }

  @Test
  public void testListRemove() {
    Assert.assertNull(OMultiValue.getValue(null, 0));
    List<String> collection = new ArrayList<String>();
    OMultiValue.add(collection, "foo");
    OMultiValue.add(collection, "bar");
    OMultiValue.add(collection, "baz");

    OMultiValue.remove(collection, "bar", true);
    Assert.assertEquals(collection.size(), 2);


  }

  public void testToString() {
    List<String> collection = new ArrayList<String>();
    OMultiValue.add(collection, 1);
    OMultiValue.add(collection, 2);
    OMultiValue.add(collection, 3);
    Assert.assertEquals(OMultiValue.toString(collection), "[1, 2, 3]");
  }
}
