package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.sql.executor.ORidSet;
import org.junit.Assert;
import org.junit.Test;

import java.util.Iterator;
import java.util.Set;

public class ORidSetTest extends OParserTestAbstract {

  @Test public void testPut() {
    ORidSet set = new ORidSet();
    ORID rid = new ORecordId(12, 100);
    Assert.assertFalse(set.contains(rid));
    set.add(rid);
    Assert.assertTrue(set.contains(rid));
  }

  @Test public void testPut0() {
    ORidSet set = new ORidSet();
    ORID rid = new ORecordId(12, 0);
    Assert.assertFalse(set.contains(rid));
    set.add(rid);
    Assert.assertTrue(set.contains(rid));
  }

  @Test public void testPut31() {
    ORidSet set = new ORidSet();
    ORID rid = new ORecordId(12, 31);
    Assert.assertFalse(set.contains(rid));
    set.add(rid);
    Assert.assertTrue(set.contains(rid));
  }

  @Test public void testPut32() {
    ORidSet set = new ORidSet();
    ORID rid = new ORecordId(12, 32);
    Assert.assertFalse(set.contains(rid));
    set.add(rid);
    Assert.assertTrue(set.contains(rid));
  }

  @Test public void testPut63() {
    ORidSet set = new ORidSet();
    ORID rid = new ORecordId(12, 63);
    Assert.assertFalse(set.contains(rid));
    set.add(rid);
    Assert.assertTrue(set.contains(rid));
  }

  @Test public void testPut64() {
    ORidSet set = new ORidSet();
    ORID rid = new ORecordId(12, 64);
    Assert.assertFalse(set.contains(rid));
    set.add(rid);
    Assert.assertTrue(set.contains(rid));
  }

  @Test public void testPut65() {
    ORidSet set = new ORidSet();
    ORID rid = new ORecordId(12, 65);
    Assert.assertFalse(set.contains(rid));
    set.add(rid);
    Assert.assertTrue(set.contains(rid));
  }

  @Test public void testRemove() {
    ORidSet set = new ORidSet();
    ORID rid = new ORecordId(12, 31);
    Assert.assertFalse(set.contains(rid));
    set.add(rid);
    Assert.assertTrue(set.contains(rid));
    set.remove(rid);
    Assert.assertFalse(set.contains(rid));
  }

  @Test public void testBigClusterId() {
    ORidSet set = new ORidSet();
    ORID rid = new ORecordId(1200, 100);
    Assert.assertFalse(set.contains(rid));
    set.add(rid);
    Assert.assertTrue(set.contains(rid));
    set.remove(rid);
    Assert.assertFalse(set.contains(rid));
  }

  @Test public void testBigClusterPosition() {
    ORidSet set = new ORidSet();
    ORID rid = new ORecordId(12, 20L * 1000 * 1000 * 1000);
    Assert.assertFalse(set.contains(rid));
    set.add(rid);
    Assert.assertTrue(set.contains(rid));
    set.remove(rid);
    Assert.assertFalse(set.contains(rid));
  }

  @Test public void testHugeClusterPosition() {
    ORidSet set = new ORidSet();
    ORID rid = new ORecordId(12, 2000L * 1000 * 1000 * 1000);
    Assert.assertFalse(set.contains(rid));
    set.add(rid);
    Assert.assertTrue(set.contains(rid));
    set.remove(rid);
    Assert.assertFalse(set.contains(rid));
  }

  @Test public void testIterator() {

    Set<ORID> set = new ORidSet();
    int clusters = 100;
    int idsPerCluster = 10;

    for (int cluster = 0; cluster < clusters; cluster++) {
      for (long id = 0; id < idsPerCluster; id++) {
        set.add(new ORecordId(cluster, id));
      }
    }
    Iterator<ORID> iterator = set.iterator();

    System.out.println("stating");
    long begin = System.currentTimeMillis();
    for (int cluster = 0; cluster < clusters; cluster++) {
      for (long id = 0; id < idsPerCluster; id++) {
        Assert.assertTrue(iterator.hasNext());
        ORID next = iterator.next();
        Assert.assertNotNull(next);
        //        Assert.assertEquals(new ORecordId(cluster, id), next);
      }
    }
    System.out.println("elapsed: " + (System.currentTimeMillis() - begin));
    Assert.assertFalse(iterator.hasNext());

  }

  @Test public void testIteratorOffset() {

    Set<ORID> set = new ORidSet();

    long offset = 20L * 1000 * 1000 * 1000;
    long idsPerCluster = 10;

    int cluster = 1;
    for (long id = 0; id < idsPerCluster; id++) {
      ORecordId rid = new ORecordId(cluster, offset + id);
      set.add(rid);
      System.out.println("added " + rid);
    }
    Iterator<ORID> iterator = set.iterator();

    System.out.println("stating");
    long begin = System.currentTimeMillis();

    for (long id = 0; id < idsPerCluster; id++) {
      Assert.assertTrue(iterator.hasNext());
      ORID next = iterator.next();
      System.out.println("found " + next);
      Assert.assertNotNull(next);
      //        Assert.assertEquals(new ORecordId(cluster, id), next);
    }
    System.out.println("elapsed: " + (System.currentTimeMillis() - begin));
    Assert.assertFalse(iterator.hasNext());

  }

  @Test public void stressTest() {
    for (int iter = 0; iter < 10; iter++) {
      Set<ORID> set = new ORidSet();
      long begin = System.currentTimeMillis();
      for (int i = 0; i < 1000 * 1000; i++) {
        ORID rid = new ORecordId(12, i);
        Assert.assertFalse(set.contains(rid));
        set.add(rid);
        Assert.assertTrue(set.contains(rid));
      }
      System.out.println("elapsed: " + (System.currentTimeMillis() - begin));
    }
  }
}
