package com.orientechnologies.orient.core.sql.functions.coll;

import com.orientechnologies.orient.core.record.impl.ODocument;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static org.testng.Assert.*;

@Test
public class OSQLFunctionDifferenceTest {

  private OSQLFunctionDifference function;

  @BeforeMethod
  public void setup() {
    function = new OSQLFunctionDifference() {
      @Override
      protected boolean returnDistributedResult() {
        return false;
      }
    };
  }

  @Test
  public void testEmpty() {
    Object result = function.getResult();
    assertNull(result);
  }

  @Test
  public void testMultiMode() {
    Integer[] coll1 = { 1, 2, 3 };
    Integer[] coll2 = { 2, 4, 6 };

    function.execute(null, null, null, new Object[] { Arrays.asList(coll1), Arrays.asList(coll2) }, null);

    Set<?> result = function.getResult();

    assertTrue(result.contains(1));
    assertTrue(!result.contains(2));
    assertEquals(result.size(), 2);
  }

  @Test
  public void testSingleMode() {
    Integer[] coll1 = { 1, 2, 3 };
    Integer[] coll2 = { 2, 4, 6 };

    List<List<?>> param = new ArrayList<List<?>>();
    param.add(Arrays.asList(coll1));
    param.add(Arrays.asList(coll2));

    function.execute(null, null, null, new Object[] { param }, null);

    Set<?> result = function.getResult();

    assertTrue(result.contains(1));
    assertTrue(!result.contains(2));
    assertEquals(result.size(), 2);
  }

  @Test
  public void testMultiModeDocs() {
    List<ODocument> coll1 = new ArrayList<ODocument>();
    coll1.add(createDoc(1, "a", "foo"));
    coll1.add(createDoc(1, "b", "bar"));
    coll1.add(createDoc(1, "b", "baz"));

    List<ODocument> coll2 = new ArrayList<ODocument>();
    coll2.add(createDoc(1, "a", "foo"));
    coll2.add(createDoc(1, "b", "asdfad"));

    function.execute(null, null, null, new Object[] { coll1, coll2 }, null);

    Set<?> result = function.getResult();

    assertEquals(result.size(), 2);
    for (Object o : result) {
      assertTrue(o instanceof ODocument);
      assertEquals(((ODocument) o).field("b"), "b");
    }
  }

  @Test
  public void testSingleModeDocs() {
    List<ODocument> coll1 = new ArrayList<ODocument>();
    coll1.add(createDoc(1, "a", "foo"));
    coll1.add(createDoc(1, "b", "bar"));
    coll1.add(createDoc(1, "b", "baz"));

    List<ODocument> coll2 = new ArrayList<ODocument>();
    coll2.add(createDoc(1, "a", "foo"));
    coll2.add(createDoc(1, "b", "asdfad"));

    List<List<?>> param = new ArrayList<List<?>>();
    param.add(Arrays.asList(coll1));
    param.add(Arrays.asList(coll2));

    for(List<?> p:param){
      function.execute(null, null, null, new Object[] { p }, null);
    }

//    function.execute(null, null, null, new Object[] { param }, null);

    Set<?> result = function.getResult();

    assertEquals(result.size(), 2);
    for (Object o : result) {
      assertTrue(o instanceof ODocument);
      assertEquals(((ODocument) o).field("b"), "b");
    }
  }

  private ODocument createDoc(int a, String b, String c) {
    ODocument uno = new ODocument();
    uno.field("a", a);
    uno.field("b", b);
    uno.field("c", c);

    return uno;
  }

}