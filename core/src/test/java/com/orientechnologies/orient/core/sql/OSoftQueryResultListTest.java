package com.orientechnologies.orient.core.sql;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

public class OSoftQueryResultListTest {
  private static final int MAX_CLUSTER = 32767;

  @Test
  public void testListFilledByAdd() {
    final long seed = System.nanoTime();
    System.out.println("testListFilledByAdd seed :" + seed);

    final Random random = new Random(seed);

    for (int n = 0; n < 3; n++) {
      final OSoftQueryResultList<ORID> rids = new OSoftQueryResultList<ORID>("test");
      final List<ORID> baseRids = new ArrayList<ORID>();

      final int size = random.nextInt(998) + 6;
      verifyAdd(random, rids, baseRids, size);

      verifyList(random, rids, baseRids, size, 1);
    }
  }

  @Test
  public void testListFilledFromAnotherList() {
    final long seed = System.nanoTime();
    System.out.println("testListFilledFromAnotherList seed :" + seed);

    final Random random = new Random(seed);

    for (int n = 0; n < 3; n++) {
      final List<ORID> baseRids = new ArrayList<ORID>();

      final int size = random.nextInt(998) + 6;
      for (int i = 0; i < size; i++) {
        final ORecordId rid = new ORecordId(random.nextInt(MAX_CLUSTER), random.nextInt(Integer.MAX_VALUE));
        baseRids.add(rid);
      }

      final OSoftQueryResultList<ORID> rids = new OSoftQueryResultList<ORID>(baseRids, "test");
      Assert.assertEquals(baseRids, rids);
      Assert.assertEquals(baseRids.size(), rids.size());
      Assert.assertFalse(rids.isEmpty());

      verifyList(random, rids, baseRids, size, 1);
    }
  }

  @Test(expected = NullPointerException.class)
  public void testAddNullValue() {
    @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
    final OSoftQueryResultList<ORID> rids = new OSoftQueryResultList<ORID>("test");
    rids.add(null);
  }

  @Test(expected = NullPointerException.class)
  public void testSetNullValue() {
    @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
    final OSoftQueryResultList<ORID> rids = new OSoftQueryResultList<ORID>("test");
    rids.add(new ORecordId(1, 1));
    rids.set(0, null);
  }

  @Test(expected = NullPointerException.class)
  public void testAddIndexNull() {
    @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
    final OSoftQueryResultList<ORID> rids = new OSoftQueryResultList<ORID>("test");
    rids.add(new ORecordId(1, 1));
    rids.add(0, null);
  }

  @Test(expected = NullPointerException.class)
  public void testIndexOfNull() {
    final OSoftQueryResultList<ORID> rids = new OSoftQueryResultList<ORID>("test");
    rids.indexOf(null);
  }

  @Test(expected = NullPointerException.class)
  public void testLastIndexOf() {
    final OSoftQueryResultList<ORID> rids = new OSoftQueryResultList<ORID>("test");
    rids.lastIndexOf(null);
  }

  @Test(expected = NullPointerException.class)
  public void testListIteratorAddNull() {
    final OSoftQueryResultList<ORID> rids = new OSoftQueryResultList<ORID>("test");
    rids.add(new ORecordId(1, 1));

    final ListIterator<ORID> listIterator = rids.listIterator();
    listIterator.add(null);
  }

  @Test(expected = NullPointerException.class)
  public void testListIteratorSetNull() {
    final OSoftQueryResultList<ORID> rids = new OSoftQueryResultList<ORID>("test");
    rids.add(new ORecordId(1, 1));

    final ListIterator<ORID> listIterator = rids.listIterator();
    listIterator.next();
    listIterator.set(null);
  }

  @Test(expected = NullPointerException.class)
  public void testListIteratorAddIndexNull() {
    final OSoftQueryResultList<ORID> rids = new OSoftQueryResultList<ORID>("test");
    rids.add(new ORecordId(1, 1));

    final ListIterator<ORID> listIterator = rids.listIterator(1);
    listIterator.add(null);
  }

  @Test(expected = NullPointerException.class)
  public void testListIteratorSetIndexNull() {
    final OSoftQueryResultList<ORID> rids = new OSoftQueryResultList<ORID>("test");
    rids.add(new ORecordId(1, 1));

    final ListIterator<ORID> listIterator = rids.listIterator(1);
    listIterator.previous();
    listIterator.set(null);
  }

  private void verifyList(Random random, OSoftQueryResultList<ORID> rids, List<ORID> baseRids, int size, int depth) {
    if (depth > 3)
      return;

    verifyContains(rids, baseRids);

    verifyContainsAll(random, rids, baseRids, size);

    verifyGet(rids, baseRids);

    verifySet(random, rids, baseRids);

    verifyIndexOfAndAddFromIndex(random, rids, baseRids);

    verifyIterator(random, rids, baseRids);

    verifyAddAll(random, rids, baseRids, size);

    verifyAddAllFromIndex(random, rids, baseRids);

    ListIterator<ORID> baseListIterator = baseRids.listIterator();
    ListIterator<ORID> listIterator = rids.listIterator();

    verifyListIteratorForth(random, rids, baseRids, baseListIterator, listIterator);
    verifyListIteratorBack(random, rids, baseRids, baseListIterator, listIterator);
    checkListSize(rids, baseRids, random);

    baseListIterator = baseRids.listIterator(baseRids.size());
    listIterator = rids.listIterator(baseRids.size());

    Assert.assertFalse(listIterator.hasNext());
    verifyListIteratorBack(random, rids, baseRids, baseListIterator, listIterator);
    checkListSize(rids, baseRids, random);

    final int indexToIterateForth = random.nextInt(baseRids.size());
    baseListIterator = baseRids.listIterator(indexToIterateForth);
    listIterator = rids.listIterator(indexToIterateForth);

    verifyListIteratorForth(random, rids, baseRids, baseListIterator, listIterator);
    checkListSize(rids, baseRids, random);

    final int indexToIterateBack = random.nextInt(baseRids.size());
    baseListIterator = baseRids.listIterator(indexToIterateBack);
    listIterator = rids.listIterator(indexToIterateBack);

    verifyListIteratorBack(random, rids, baseRids, baseListIterator, listIterator);
    checkListSize(rids, baseRids, random);

    verifySort(rids, baseRids);

    verifyRemove(rids, baseRids, random);

    verifyRemoveAll(rids, baseRids, random);

    verifyRetainAll(rids, baseRids, random);

    verifySubList(random, rids, baseRids, depth);

    verifyClear(rids, baseRids, random);
  }

  private void verifyClear(OSoftQueryResultList<ORID> rids, List<ORID> baseRids, Random random) {
    final int initialSize = baseRids.size();
    baseRids.clear();
    rids.clear();

    Assert.assertEquals(baseRids, rids);
    Assert.assertEquals(0, rids.size());
    Assert.assertTrue(rids.isEmpty());

    for (int i = 0; i < initialSize; i++) {
      final ORID rid = new ORecordId(random.nextInt(MAX_CLUSTER), random.nextInt(Integer.MAX_VALUE));

      baseRids.add(rid);
      rids.add(rid);
    }

    Assert.assertEquals(baseRids, rids);
    Assert.assertEquals(initialSize, baseRids.size());
    Assert.assertFalse(rids.isEmpty());
  }

  private void verifyRetainAll(OSoftQueryResultList<ORID> rids, List<ORID> baseRids, Random random) {
    final int initialSize = checkListSize(rids, baseRids, random);
    final int amountOfExistingObjectsToRetain = random.nextInt(baseRids.size());
    final int amountOfNonExistingObjectsToRetain = random.nextInt(baseRids.size());

    final List<ORID> objectsToRetain = new ArrayList<ORID>();

    for (int i = 0; i < amountOfExistingObjectsToRetain; i++) {
      final int index = random.nextInt(baseRids.size());
      final ORID rid = baseRids.get(index);
      objectsToRetain.add(rid);
    }

    for (int i = 0; i < amountOfNonExistingObjectsToRetain; i++) {
      final int index = random.nextInt(baseRids.size());
      final ORID rid = baseRids.get(index);
      objectsToRetain.add(new ORecordId(rid.getClusterId(), -(rid.getClusterPosition() + 1)));
    }

    baseRids.retainAll(objectsToRetain);
    rids.retainAll(objectsToRetain);

    Assert.assertEquals(baseRids, rids);
    Assert.assertEquals(baseRids.size(), rids.size());

    while (baseRids.size() < initialSize) {
      final ORID rid = new ORecordId(random.nextInt(MAX_CLUSTER), random.nextInt(Integer.MAX_VALUE));
      baseRids.add(rid);
      rids.add(rid);
    }

    Assert.assertEquals(baseRids, rids);
    Assert.assertEquals(baseRids.size(), rids.size());
  }

  private void verifyRemove(OSoftQueryResultList<ORID> rids, List<ORID> baseRids, Random random) {
    int initialSize = checkListSize(rids, baseRids, random);

    final int amountOfObjectToRemove = random.nextInt(baseRids.size());

    for (int i = 0; i < amountOfObjectToRemove; i++) {
      final int indexToRemove = random.nextInt(baseRids.size());
      baseRids.remove(indexToRemove);
      rids.remove(indexToRemove);
    }

    Assert.assertEquals(baseRids, rids);
    Assert.assertEquals(baseRids.size(), rids.size());

    while (baseRids.size() < initialSize) {
      final ORecordId rid = new ORecordId(random.nextInt(MAX_CLUSTER), random.nextInt(Integer.MAX_VALUE));
      baseRids.add(rid);
      rids.add(rid);
    }

    Assert.assertEquals(baseRids, rids);
    Assert.assertEquals(baseRids.size(), rids.size());

    for (int i = 0; i < amountOfObjectToRemove; i++) {
      final int indexToRemove = random.nextInt(baseRids.size());
      final ORID rid = baseRids.get(indexToRemove);

      baseRids.remove(rid);
      rids.remove(rid);
    }

    for (int i = 0; i < amountOfObjectToRemove; i++) {
      final int indexToRemove = random.nextInt(baseRids.size());
      final ORID rid = baseRids.get(indexToRemove);

      final ORID ridToRemove = new ORecordId(rid.getClusterId(), -(rid.getClusterPosition() + 1));
      baseRids.remove(ridToRemove);
      rids.remove(ridToRemove);
    }

    Assert.assertEquals(baseRids, rids);
    Assert.assertEquals(baseRids.size(), rids.size());

    while (baseRids.size() < initialSize) {
      final ORecordId rid = new ORecordId(random.nextInt(MAX_CLUSTER), random.nextInt(Integer.MAX_VALUE));
      baseRids.add(rid);
      rids.add(rid);
    }

    Assert.assertEquals(baseRids, rids);
    Assert.assertEquals(baseRids.size(), rids.size());
  }

  private int checkListSize(OSoftQueryResultList<ORID> rids, List<ORID> baseRids, Random random) {
    if (baseRids.size() < 2) {
      while (baseRids.size() < 2) {
        final ORID rid = new ORecordId(random.nextInt(MAX_CLUSTER), random.nextInt(Integer.MAX_VALUE));
        baseRids.add(rid);
        rids.add(rid);
      }

      Assert.assertEquals(baseRids, rids);
      Assert.assertEquals(2, rids.size());
      return 2;
    }

    return baseRids.size();
  }

  private void verifyRemoveAll(OSoftQueryResultList<ORID> rids, List<ORID> baseRids, Random random) {
    final int initialSize = checkListSize(rids, baseRids, random);
    final int amountOfExistingObjectToRemove = random.nextInt(baseRids.size());

    final List<ORID> toRemove = new ArrayList<ORID>();
    for (int i = 0; i < amountOfExistingObjectToRemove; i++) {
      final int indexToRemove = random.nextInt(baseRids.size());
      final ORID objectToRemove = baseRids.get(indexToRemove);

      toRemove.add(objectToRemove);
    }

    final int numberOfNonExistingObjectsToRemove = random.nextInt(baseRids.size());
    for (int i = 0; i < numberOfNonExistingObjectsToRemove; i++) {
      final int indexRid = random.nextInt(baseRids.size());
      final ORID rid = baseRids.get(indexRid);

      final ORID objectToRemove = new ORecordId(rid.getClusterId(), -(rid.getClusterPosition() + 1));
      toRemove.add(objectToRemove);
    }

    baseRids.removeAll(toRemove);
    rids.removeAll(toRemove);

    Assert.assertEquals(baseRids, rids);
    Assert.assertEquals(baseRids.size(), rids.size());

    while (baseRids.size() < initialSize) {
      final ORecordId rid = new ORecordId(random.nextInt(MAX_CLUSTER), random.nextInt(Integer.MAX_VALUE));
      baseRids.add(rid);
      rids.add(rid);
    }

    Assert.assertEquals(baseRids, rids);
    Assert.assertEquals(baseRids.size(), rids.size());
  }

  private void verifySubList(Random random, OSoftQueryResultList<ORID> rids, List<ORID> baseRids, int depth) {
    final int subListStartIndex = random.nextInt(baseRids.size() - 1);
    final int subListEndIndex = random.nextInt(baseRids.size() - subListStartIndex) + subListStartIndex + 1;

    final List<ORID> baseSubList = baseRids.subList(subListStartIndex, subListEndIndex);
    final OSoftQueryResultList<ORID> ridsSubList = rids.subList(subListStartIndex, subListEndIndex);

    Assert.assertEquals(baseSubList, ridsSubList);
    Assert.assertEquals(baseSubList.size(), ridsSubList.size());

    if (baseSubList.size() >= 2) {
      verifyList(random, ridsSubList, baseSubList, baseSubList.size(), depth + 1);
    }

    Assert.assertEquals(baseSubList, ridsSubList);
    Assert.assertEquals(baseSubList.size(), ridsSubList.size());

    Assert.assertEquals(baseRids, rids);
    Assert.assertEquals(baseRids.size(), rids.size());
  }

  private void verifySort(OSoftQueryResultList<ORID> rids, List<ORID> baseRids) {
    Collections.sort(baseRids);
    rids.sort(new Comparator<ORID>() {
      @Override
      public int compare(ORID o1, ORID o2) {
        return o1.compareTo(o2);
      }
    });
    Assert.assertEquals(baseRids, rids);
  }

  private void verifyListIteratorBack(Random random, OSoftQueryResultList<ORID> rids, List<ORID> baseRids,
      ListIterator<ORID> baseListIterator, ListIterator<ORID> listIterator) {
    while (baseListIterator.hasPrevious()) {
      Assert.assertTrue(listIterator.hasPrevious());

      Assert.assertEquals(baseListIterator.nextIndex(), listIterator.nextIndex());
      Assert.assertEquals(baseListIterator.previousIndex(), listIterator.previousIndex());

      Assert.assertEquals(baseListIterator.previous(), listIterator.previous());

      boolean listModified = false;
      if (random.nextDouble() > 0.5) {
        baseListIterator.remove();
        listIterator.remove();
        listModified = true;
      }

      if (random.nextDouble() > 0.5) {
        final ORecordId recordId = new ORecordId(random.nextInt(MAX_CLUSTER), random.nextInt(Integer.MAX_VALUE));
        baseListIterator.add(recordId);
        listIterator.add(recordId);
        listModified = true;
      }

      if (!listModified && random.nextDouble() > 0.5) {
        final ORecordId recordId = new ORecordId(random.nextInt(MAX_CLUSTER), random.nextInt(Integer.MAX_VALUE));
        baseListIterator.set(recordId);
        listIterator.set(recordId);
      }
    }

    Assert.assertFalse(listIterator.hasPrevious());
    Assert.assertEquals(baseRids, rids);
    Assert.assertEquals(baseRids.size(), rids.size());
  }

  private void verifyListIteratorForth(Random random, OSoftQueryResultList<ORID> rids, List<ORID> baseRids,
      ListIterator<ORID> baseListIterator, ListIterator<ORID> listIterator) {
    while (baseListIterator.hasNext()) {
      Assert.assertTrue(listIterator.hasNext());

      Assert.assertEquals(baseListIterator.nextIndex(), listIterator.nextIndex());
      Assert.assertEquals(baseListIterator.previousIndex(), listIterator.previousIndex());

      Assert.assertEquals(baseListIterator.next(), listIterator.next());

      boolean listModified = false;

      if (random.nextDouble() > 0.5) {
        baseListIterator.remove();
        listIterator.remove();
        listModified = true;
      }

      if (random.nextDouble() > 0.5) {
        final ORecordId recordId = new ORecordId(random.nextInt(MAX_CLUSTER), random.nextInt(Integer.MAX_VALUE));
        baseListIterator.add(recordId);
        listIterator.add(recordId);
        listModified = true;
      }

      if (!listModified && random.nextDouble() > 0.5) {
        final ORecordId recordId = new ORecordId(random.nextInt(MAX_CLUSTER), random.nextInt(Integer.MAX_VALUE));
        baseListIterator.set(recordId);
        listIterator.set(recordId);
      }
    }

    Assert.assertFalse(listIterator.hasNext());
    Assert.assertEquals(baseRids, rids);
    Assert.assertEquals(baseRids.size(), rids.size());
  }

  private void verifyAddAllFromIndex(Random random, OSoftQueryResultList<ORID> rids, List<ORID> baseRids) {
    final List<ORID> ridsToAdd = new ArrayList<ORID>();
    final int itemsToAdd = random.nextInt(20) + 10;
    for (int i = 0; i < itemsToAdd; i++) {
      final ORecordId rid = new ORecordId(random.nextInt(MAX_CLUSTER), random.nextInt(Integer.MAX_VALUE));
      ridsToAdd.add(rid);
    }

    final int indexToInsert = random.nextInt(baseRids.size() - 1) + 1;
    baseRids.addAll(indexToInsert, ridsToAdd);
    rids.addAll(indexToInsert, ridsToAdd);

    Assert.assertEquals(baseRids, rids);
    Assert.assertEquals(baseRids.size(), rids.size());
  }

  private void verifyAddAll(Random random, OSoftQueryResultList<ORID> rids, List<ORID> baseRids, int size) {
    final int sizeDiff = size - baseRids.size();
    if (sizeDiff > 0) {
      final List<ORID> ridsToAdd = new ArrayList<ORID>();
      for (int i = 0; i < sizeDiff; i++) {
        final ORecordId rid = new ORecordId(random.nextInt(MAX_CLUSTER), random.nextInt(Integer.MAX_VALUE));
        ridsToAdd.add(rid);
      }

      baseRids.addAll(ridsToAdd);
      rids.addAll(ridsToAdd);

      Assert.assertEquals(baseRids, rids);
      Assert.assertEquals(baseRids.size(), rids.size());
    }
  }

  private void verifyIterator(Random random, OSoftQueryResultList<ORID> rids, List<ORID> baseRids) {
    final Iterator<ORID> baseIterator = baseRids.iterator();
    final Iterator<ORID> iterator = rids.iterator();

    while (baseIterator.hasNext()) {
      Assert.assertTrue(iterator.hasNext());
      Assert.assertEquals(iterator.next(), baseIterator.next());

      if (random.nextDouble() > 0.5) {
        iterator.remove();
        baseIterator.remove();
      }
    }

    Assert.assertFalse(iterator.hasNext());
    Assert.assertEquals(baseRids, rids);
    Assert.assertEquals(baseRids.size(), rids.size());
  }

  private void verifyIndexOfAndAddFromIndex(Random random, OSoftQueryResultList<ORID> rids, List<ORID> baseRids) {
    final int numberOfDuplicates = random.nextInt(baseRids.size() - 1) + 1;
    for (int i = 0; i < numberOfDuplicates; i++) {
      final int indexFrom = random.nextInt(baseRids.size());
      final int indexTo = random.nextInt(baseRids.size() + 1);

      final ORID rid = baseRids.get(indexFrom);

      baseRids.add(indexTo, rid);
      rids.add(indexTo, rid);
    }

    Assert.assertEquals(baseRids, rids);
    Assert.assertEquals(baseRids.size(), rids.size());

    for (int i = 0; i < baseRids.size(); i++) {
      final ORID rid = baseRids.get(i);
      Assert.assertEquals(baseRids.indexOf(rid), rids.indexOf(rid));
      Assert.assertEquals(baseRids.lastIndexOf(rid), rids.lastIndexOf(rid));
    }

    for (ORID rid : baseRids) {
      Assert.assertEquals(-1, rids.indexOf(new ORecordId(rid.getClusterId(), -(rid.getClusterPosition() + 1))));
      Assert.assertEquals(-1, rids.lastIndexOf(new ORecordId(rid.getClusterId(), -(rid.getClusterPosition() + 1))));
    }
  }

  private void verifySet(Random random, OSoftQueryResultList<ORID> rids, List<ORID> baseRids) {
    for (int i = 0; i < baseRids.size() / 2; i++) {
      final ORID rid = new ORecordId(random.nextInt(MAX_CLUSTER), random.nextInt(Integer.MAX_VALUE));
      baseRids.set(i, rid);
      rids.set(i, rid);
    }

    Assert.assertEquals(baseRids, rids);

    Assert.assertEquals(baseRids.size(), rids.size());
    Assert.assertEquals(baseRids.isEmpty(), rids.isEmpty());
  }

  private void verifyGet(OSoftQueryResultList<ORID> rids, List<ORID> baseRids) {
    for (int i = 0; i < baseRids.size(); i++) {
      Assert.assertEquals(baseRids.get(i), rids.get(i));
    }
  }

  private void verifyContainsAll(Random random, OSoftQueryResultList<ORID> rids, List<ORID> baseRids, int size) {
    final int subListStartIndex = random.nextInt(size - 1);
    final int subListEndIndex = random.nextInt(size - subListStartIndex) + subListStartIndex + 1;
    Assert.assertTrue(rids.containsAll(baseRids.subList(subListStartIndex, subListEndIndex)));

    final List<ORID> negRids = new ArrayList<ORID>(baseRids.subList(subListStartIndex, subListEndIndex));
    negRids.add(0, new ORecordId(1, -1));
    negRids.add(new ORecordId(10, -10));

    Assert.assertFalse(rids.containsAll(negRids));
  }

  private void verifyContains(OSoftQueryResultList<ORID> rids, List<ORID> baseRids) {
    for (ORID rid : baseRids) {
      Assert.assertTrue(rids.contains(rid));
    }

    for (ORID rid : baseRids) {
      Assert.assertFalse(rids.contains(new ORecordId(rid.getClusterId(), -(rid.getClusterPosition() + 1))));
    }
  }

  private void verifyAdd(Random random, OSoftQueryResultList<ORID> rids, List<ORID> baseRids, int size) {
    for (int i = 0; i < size; i++) {
      final ORecordId rid = new ORecordId(random.nextInt(MAX_CLUSTER), random.nextInt(Integer.MAX_VALUE));

      rids.add(rid);
      baseRids.add(rid);
    }

    Assert.assertEquals(baseRids, rids);
    Assert.assertEquals(baseRids.size(), rids.size());
    Assert.assertEquals(baseRids.isEmpty(), rids.isEmpty());
  }
}
