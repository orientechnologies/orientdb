package com.orientechnologies.orient.core.sql;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

public class OSoftQueryResultListTest {

  public static final int MAX_CLUSTER = 32767;

  @Test
  public void testListFilledByAdd() {
    final long seed = 147948378170170L;
    System.nanoTime();
    System.out.println("testListFilledByAdd seed :" + seed);

    final Random random = new Random(seed);

    for (int n = 0; n < 300000; n++) {
      final OSoftQueryResultList<ORID> rids = new OSoftQueryResultList<ORID>();
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

    for (int n = 0; n < 300000; n++) {
      final List<ORID> baseRids = new ArrayList<ORID>();

      final int size = random.nextInt(998) + 6;
      for (int i = 0; i < size; i++) {
        final ORecordId rid = new ORecordId(random.nextInt(MAX_CLUSTER), random.nextInt(Integer.MAX_VALUE));
        baseRids.add(rid);
      }

      final OSoftQueryResultList<ORID> rids = new OSoftQueryResultList<ORID>(baseRids);
      Assert.assertEquals(baseRids, rids);
      Assert.assertEquals(baseRids.size(), rids.size());
      Assert.assertFalse(rids.isEmpty());

      verifyList(random, rids, baseRids, size, 1);
    }
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

    baseListIterator = baseRids.listIterator(baseRids.size());
    listIterator = rids.listIterator(baseRids.size());

    Assert.assertFalse(listIterator.hasNext());
    verifyListIteratorBack(random, rids, baseRids, baseListIterator, listIterator);

    final int indexToIterateForth = random.nextInt(baseRids.size());
    baseListIterator = baseRids.listIterator(indexToIterateForth);
    listIterator = rids.listIterator(indexToIterateForth);

    verifyListIteratorForth(random, rids, baseRids, baseListIterator, listIterator);

    final int indexToIterateBack = random.nextInt(baseRids.size());
    baseListIterator = baseRids.listIterator(indexToIterateBack);
    listIterator = rids.listIterator(indexToIterateBack);

    verifyListIteratorBack(random, rids, baseRids, baseListIterator, listIterator);

    verifySort(rids, baseRids);

    verifySubList(random, rids, baseRids, depth);
  }

  private void verifySubList(Random random, OSoftQueryResultList<ORID> rids, List<ORID> baseRids, int depth) {
    final int subListStartIndex = random.nextInt(baseRids.size() - 1);
    final int subListEndIndex = random.nextInt(baseRids.size() - subListStartIndex) + subListStartIndex + 1;

    final List<ORID> baseSubList = baseRids.subList(subListStartIndex, subListEndIndex);
    final OSoftQueryResultList<ORID> ridsSubList = rids.subList(subListStartIndex, subListEndIndex);

    Assert.assertEquals(baseSubList, ridsSubList);
    Assert.assertEquals(baseSubList.size(), ridsSubList.size());

    if (baseSubList.size() >= 6) {
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
      if (random.nextDouble() > 0.5) {
        baseListIterator.remove();
        listIterator.remove();
      }

      if (random.nextDouble() > 0.5) {
        final ORecordId recordId = new ORecordId(random.nextInt(MAX_CLUSTER), random.nextInt(Integer.MAX_VALUE));
        baseListIterator.add(recordId);
        listIterator.add(recordId);
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
      if (random.nextDouble() > 0.5) {
        baseListIterator.remove();
        listIterator.remove();
      }

      if (random.nextDouble() > 0.5) {
        final ORecordId recordId = new ORecordId(random.nextInt(MAX_CLUSTER), random.nextInt(Integer.MAX_VALUE));
        baseListIterator.add(recordId);
        listIterator.add(recordId);
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

    final int indexToInsert = random.nextInt(baseRids.size() - 2) + 1;
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
