package com.orientechnologies.common.concur.collection;

import org.junit.Assert;
import org.junit.Test;

public class CASObjectArrayTest {
  @Test
  public void testAddSingleItem() {
    final CASObjectArray<Integer> array = new CASObjectArray<>();
    Assert.assertEquals(0, array.size());

    Assert.assertEquals(0, array.add(1));
    Assert.assertEquals(1, array.size());
    Assert.assertEquals(1, array.get(0).intValue());
  }

  @Test
  public void testAddTwoItems() {
    final CASObjectArray<Integer> array = new CASObjectArray<>();
    Assert.assertEquals(0, array.size());

    Assert.assertEquals(0, array.add(1));
    Assert.assertEquals(1, array.size());

    Assert.assertEquals(1, array.add(2));
    Assert.assertEquals(2, array.size());

    Assert.assertEquals(1, array.get(0).intValue());
    Assert.assertEquals(2, array.get(1).intValue());
  }

  @Test
  public void testAddThreeItems() {
    final CASObjectArray<Integer> array = new CASObjectArray<>();
    Assert.assertEquals(0, array.size());

    Assert.assertEquals(0, array.add(1));
    Assert.assertEquals(1, array.size());

    Assert.assertEquals(1, array.add(2));
    Assert.assertEquals(2, array.size());

    Assert.assertEquals(2, array.add(3));
    Assert.assertEquals(3, array.size());

    Assert.assertEquals(1, array.get(0).intValue());
    Assert.assertEquals(2, array.get(1).intValue());
    Assert.assertEquals(3, array.get(2).intValue());
  }

  @Test
  public void testAdd12Items() {
    final CASObjectArray<Integer> array = new CASObjectArray<>();

    for (int i = 0; i < 12; i++) {
      array.add(i + 1);
      Assert.assertEquals(i + 1, array.size());
    }

    for (int i = 0; i < 12; i++) {
      Assert.assertEquals(i + 1, array.get(i).intValue());
    }
  }

  @Test
  public void testSetSingleItem() {
    final CASObjectArray<Integer> array = new CASObjectArray<>();

    Assert.assertEquals(0, array.add(1));
    array.set(0, 21, -1);

    Assert.assertEquals(21, array.get(0).intValue());
  }

  @Test
  public void testSetTwoItems() {
    final CASObjectArray<Integer> array = new CASObjectArray<>();

    Assert.assertEquals(0, array.add(1));
    Assert.assertEquals(1, array.add(2));

    array.set(0, 21, -1);
    array.set(1, 22, -1);

    Assert.assertEquals(21, array.get(0).intValue());
    Assert.assertEquals(22, array.get(1).intValue());
  }

  @Test
  public void testSetThreeItems() {
    final CASObjectArray<Integer> array = new CASObjectArray<>();

    Assert.assertEquals(0, array.add(1));
    Assert.assertEquals(1, array.add(2));
    Assert.assertEquals(2, array.add(3));

    array.set(0, 21, -1);
    array.set(1, 22, -1);
    array.set(2, 23, -1);

    Assert.assertEquals(21, array.get(0).intValue());
    Assert.assertEquals(22, array.get(1).intValue());
    Assert.assertEquals(23, array.get(2).intValue());
  }

  @Test
  public void testSet12Items() {
    final CASObjectArray<Integer> array = new CASObjectArray<>();

    for (int i = 0; i < 12; i++) {
      Assert.assertEquals(i, array.add(i + 1));
      Assert.assertEquals(i + 1, array.size());
    }

    for (int i = 0; i < 12; i++) {
      array.set(i, 21 + i, -1);
    }

    for (int i = 0; i < 12; i++) {
      Assert.assertEquals(i + 21, array.get(i).intValue());
    }
  }

  @Test
  public void testCompareAndSetSingleItem() {
    final CASObjectArray<Integer> array = new CASObjectArray<>();

    Assert.assertEquals(0, array.add(1));
    Assert.assertFalse(array.compareAndSet(0, 12, 21));
    Assert.assertEquals(1, array.get(0).intValue());

    Assert.assertTrue(array.compareAndSet(0, 1, 22));
    Assert.assertEquals(22, array.get(0).intValue());
  }

  @Test
  public void testCompareAndSetTwoItems() {
    final CASObjectArray<Integer> array = new CASObjectArray<>();

    Assert.assertEquals(0, array.add(1));
    Assert.assertEquals(1, array.add(2));

    Assert.assertFalse(array.compareAndSet(0, 22, 21));
    Assert.assertEquals(1, array.get(0).intValue());
    Assert.assertTrue(array.compareAndSet(0, 1, 21));

    Assert.assertFalse(array.compareAndSet(1, 23, 22));
    Assert.assertEquals(2, array.get(1).intValue());
    Assert.assertTrue(array.compareAndSet(1, 2, 22));

    Assert.assertEquals(21, array.get(0).intValue());
    Assert.assertEquals(22, array.get(1).intValue());
  }

  @Test
  public void testCompareAndSetThreeItems() {
    final CASObjectArray<Integer> array = new CASObjectArray<>();

    Assert.assertEquals(0, array.add(1));
    Assert.assertEquals(1, array.add(2));
    Assert.assertEquals(2, array.add(3));

    Assert.assertFalse(array.compareAndSet(0, 22, 21));
    Assert.assertEquals(1, array.get(0).intValue());
    Assert.assertTrue(array.compareAndSet(0, 1, 21));

    Assert.assertFalse(array.compareAndSet(1, 23, 22));
    Assert.assertEquals(2, array.get(1).intValue());
    Assert.assertTrue(array.compareAndSet(1, 2, 22));

    Assert.assertFalse(array.compareAndSet(2, 24, 23));
    Assert.assertEquals(3, array.get(2).intValue());
    Assert.assertTrue(array.compareAndSet(2, 3, 23));

    Assert.assertEquals(21, array.get(0).intValue());
    Assert.assertEquals(22, array.get(1).intValue());
    Assert.assertEquals(23, array.get(2).intValue());
  }

  @Test
  public void testCompareAndSet12Items() {
    final CASObjectArray<Integer> array = new CASObjectArray<>();

    for (int i = 0; i < 12; i++) {
      Assert.assertEquals(i, array.add(i + 1));
      Assert.assertEquals(i + 1, array.size());
    }

    for (int i = 0; i < 12; i++) {
      Assert.assertFalse(array.compareAndSet(i, 22 + i, 21 + i));
    }

    for (int i = 0; i < 12; i++) {
      Assert.assertEquals(i + 1, array.get(i).intValue());
    }

    for (int i = 0; i < 12; i++) {
      Assert.assertTrue(array.compareAndSet(i, i + 1, 21 + i));
    }

    for (int i = 0; i < 12; i++) {
      Assert.assertEquals(i + 21, array.get(i).intValue());
    }
  }
}
