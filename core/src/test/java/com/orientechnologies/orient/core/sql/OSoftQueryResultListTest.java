package com.orientechnologies.orient.core.sql;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

public class OSoftQueryResultListTest {
  @Test
  public void testListFilledByAdd() {
    final long seed = System.nanoTime();
    System.out.println("testListFilledByAdd seed :" + seed);

    final Random random = new Random(seed);

    for (int n = 0; n < 1; n++) {
      final OSoftQueryResultList<ORID> rids = new OSoftQueryResultList<ORID>();
      final List<ORID> baseRids = new ArrayList<ORID>();

      final int size = random.nextInt(998) + 6;
      for (int i = 0; i < size; i++) {
        final ORecordId rid = new ORecordId(random.nextInt(Integer.MAX_VALUE), random.nextInt(Integer.MAX_VALUE));

        rids.add(rid);
        baseRids.add(rid);
      }

      Assert.assertEquals(baseRids, rids);
      Assert.assertEquals(baseRids.size(), rids.size());
      Assert.assertEquals(baseRids.isEmpty(), rids.isEmpty());

      for (ORID rid : baseRids) {
        Assert.assertTrue(rids.contains(rid));
      }

      for (ORID rid : baseRids) {
        Assert.assertFalse(rids.contains(new ORecordId(-rid.getClusterId(), rid.getClusterPosition())));
      }

      final int subListStartIndex = random.nextInt(size - 1);
      final int subListEndIndex = random.nextInt(size - subListStartIndex) + subListStartIndex + 1;
      Assert.assertTrue(rids.containsAll(baseRids.subList(subListStartIndex, subListEndIndex)));

      final List<ORID> negRids = new ArrayList<ORID>(baseRids.subList(subListStartIndex, subListEndIndex));
      negRids.add(0, new ORecordId(-1, -1));
      negRids.add(new ORecordId(-10, -10));

      Assert.assertFalse(rids.containsAll(negRids));

      for (int i = 0; i < baseRids.size(); i++) {
        Assert.assertEquals(baseRids.get(i), rids.get(i));
      }

      for (int i = 0; i < baseRids.size() / 2; i++) {
        final ORID rid = new ORecordId(random.nextInt(Integer.MAX_VALUE), random.nextInt(Integer.MAX_VALUE));
        baseRids.set(i, rid);
        rids.set(i, rid);
      }

      Assert.assertEquals(baseRids, rids);

      Assert.assertEquals(baseRids.size(), rids.size());
      Assert.assertEquals(baseRids.isEmpty(), rids.isEmpty());

      for (int i = 0; i < baseRids.size(); i++) {
        final ORID rid = baseRids.get(i);
        Assert.assertEquals(baseRids.indexOf(rid), rids.indexOf(rid));
        Assert.assertEquals(baseRids.lastIndexOf(rid), rids.lastIndexOf(rid));
      }

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

      Assert.assertFalse(baseIterator.hasNext());
      Assert.assertFalse(iterator.hasNext());

      Assert.assertEquals(baseRids, rids);
    }
  }
}
