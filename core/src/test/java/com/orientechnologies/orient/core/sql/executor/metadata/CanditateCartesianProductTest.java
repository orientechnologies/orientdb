package com.orientechnologies.orient.core.sql.executor.metadata;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

public class CanditateCartesianProductTest {

  @Test
  public void one() {

    List<List<Object>> base = new ArrayList<>();
    List<Object> one = new ArrayList<>();
    one.add("a");
    base.add(one);
    List<List<Object>> result = OIndexCandidateOne.cartesianProduct(base);
    assertEquals(1, result.size());
    assertEquals(1, result.get(0).size());
    assertEquals("a", result.get(0).get(0));
  }

  @Test
  public void two() {
    List<List<Object>> base = new ArrayList<>();
    List<Object> two = new ArrayList<>();
    two.add("a");
    two.add("b");
    base.add(two);
    List<List<Object>> result = OIndexCandidateOne.cartesianProduct(base);
    assertEquals(2, result.size());
    assertEquals(1, result.get(0).size());
    assertEquals("a", result.get(0).get(0));
    assertEquals("b", result.get(1).get(0));
  }

  @Test
  public void twoOne() {
    List<List<Object>> base = new ArrayList<>();
    List<Object> one = new ArrayList<>();
    one.add("a");
    base.add(one);
    List<Object> twoOne = new ArrayList<>();
    twoOne.add("b");
    base.add(twoOne);
    List<List<Object>> result = OIndexCandidateOne.cartesianProduct(base);
    assertEquals(1, result.size());
    assertEquals(2, result.get(0).size());
    assertEquals("a", result.get(0).get(0));
    assertEquals("b", result.get(0).get(1));
  }

  @Test
  public void twoTwo() {

    List<List<Object>> base = new ArrayList<>();
    List<Object> two = new ArrayList<>();
    two.add("a");
    two.add("b");
    base.add(two);
    List<Object> twotwo = new ArrayList<>();
    twotwo.add("c");
    twotwo.add("d");
    base.add(twotwo);
    List<List<Object>> result = OIndexCandidateOne.cartesianProduct(base);
    assertEquals(4, result.size());
    assertEquals(2, result.get(0).size());
    assertEquals("a", result.get(0).get(0));
    assertEquals("c", result.get(0).get(1));
    assertEquals(2, result.get(1).size());
    assertEquals("a", result.get(1).get(0));
    assertEquals("d", result.get(1).get(1));
    assertEquals(2, result.get(2).size());
    assertEquals("b", result.get(2).get(0));
    assertEquals("c", result.get(2).get(1));
    assertEquals(2, result.get(3).size());
    assertEquals("b", result.get(3).get(0));
    assertEquals("d", result.get(3).get(1));
  }

  @Test
  public void threeThree() {

    List<List<Object>> base = new ArrayList<>();
    List<Object> three = new ArrayList<>();
    three.add("a");
    three.add("b");
    three.add("c");
    base.add(three);
    List<Object> threetwo = new ArrayList<>();
    threetwo.add("d");
    threetwo.add("e");
    threetwo.add("f");
    base.add(threetwo);
    List<Object> threethree = new ArrayList<>();
    threethree.add("g");
    threethree.add("h");
    threethree.add("i");
    base.add(threethree);
    List<List<Object>> result = OIndexCandidateOne.cartesianProduct(base);
    assertEquals(result.size(), 27);
    for (List<Object> ele : result) {
      assertEquals(3, ele.size());
    }
    assertEquals("a", result.get(0).get(0));
    assertEquals("d", result.get(0).get(1));
    assertEquals("g", result.get(0).get(2));

    assertEquals("a", result.get(1).get(0));
    assertEquals("d", result.get(1).get(1));
    assertEquals("h", result.get(1).get(2));

    assertEquals("a", result.get(2).get(0));
    assertEquals("d", result.get(2).get(1));
    assertEquals("i", result.get(2).get(2));

    assertEquals("a", result.get(3).get(0));
    assertEquals("e", result.get(3).get(1));
    assertEquals("g", result.get(3).get(2));

    assertEquals("a", result.get(4).get(0));
    assertEquals("e", result.get(4).get(1));
    assertEquals("h", result.get(4).get(2));

    assertEquals("a", result.get(5).get(0));
    assertEquals("e", result.get(5).get(1));
    assertEquals("i", result.get(5).get(2));

    assertEquals("a", result.get(6).get(0));
    assertEquals("f", result.get(6).get(1));
    assertEquals("g", result.get(6).get(2));

    assertEquals("a", result.get(7).get(0));
    assertEquals("f", result.get(7).get(1));
    assertEquals("h", result.get(7).get(2));

    assertEquals("a", result.get(8).get(0));
    assertEquals("f", result.get(8).get(1));
    assertEquals("i", result.get(8).get(2));

    assertEquals("b", result.get(13).get(0));
    assertEquals("e", result.get(13).get(1));
    assertEquals("h", result.get(13).get(2));

    assertEquals("b", result.get(14).get(0));
    assertEquals("e", result.get(14).get(1));
    assertEquals("i", result.get(14).get(2));

    assertEquals("c", result.get(26).get(0));
    assertEquals("f", result.get(26).get(1));
    assertEquals("i", result.get(26).get(2));
  }

  @Test
  public void twoFour() {

    List<List<Object>> base = new ArrayList<>();
    List<Object> four = new ArrayList<>();
    four.add("a");
    four.add("b");
    four.add("c");
    four.add("d");
    base.add(four);
    List<Object> fourtwo = new ArrayList<>();
    fourtwo.add("e");
    fourtwo.add("f");
    fourtwo.add("g");
    fourtwo.add("h");
    base.add(fourtwo);

    List<List<Object>> result = OIndexCandidateOne.cartesianProduct(base);

    assertEquals(16, result.size());
    for (List<Object> ele : result) {
      assertEquals(2, ele.size());
    }
    assertEquals("a", result.get(0).get(0));
    assertEquals("e", result.get(0).get(1));

    assertEquals("a", result.get(1).get(0));
    assertEquals("f", result.get(1).get(1));

    assertEquals("a", result.get(3).get(0));
    assertEquals("h", result.get(3).get(1));

    assertEquals("b", result.get(7).get(0));
    assertEquals("h", result.get(7).get(1));

    assertEquals("d", result.get(15).get(0));
    assertEquals("h", result.get(15).get(1));
  }
}
